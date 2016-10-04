/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.history.yarn.commands

import java.io.{FileNotFoundException, IOException, InputStream}
import java.net.URI
import java.util.Locale

import scala.io.Source

import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.{ExitUtil, ToolRunner}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils
import org.json4s.jackson.JsonMethods._

import org.apache.spark.deploy.history.yarn.{AppAttemptDetails, YarnHistoryService, YarnTimelineUtils}
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.publish.{EntityPublisher, SparkEventPublisher}
import org.apache.spark.scheduler.{EventLoggingListener, SparkListenerEvent}
import org.apache.spark.util.JsonProtocol

/**
 * Upload: `applicationId application-attempt file`.
 *
 * Uploads a file to the history.
 */
private[spark] class Upload extends TimelineCommand {

  import org.apache.spark.deploy.history.yarn.commands.TimelineCommand._
  import org.apache.spark.deploy.history.yarn.commands.Upload._

  /**
   * Execute the operation.
   * @param args list of arguments
   * @return the exit code.
   */
  override def exec(args: Seq[String]): Int = {
    if (args.length != 3) {
      throw usage()
    }
    val appId = args.head
    val attemptId = if (args(1).toLowerCase(Locale.ENGLISH) == Upload.NO_ATTEMPT)  {
      None
    } else {
      Some(args(1))
    }
    val filename = args(2)

    uploadHistory(appId, attemptId, filename)
  }


  def usage(): CommandException = {
    new CommandException(E_USAGE, Upload.USAGE)
  }

  /**
   * Upload a history.
   * @param appId application ID
   * @param attempt attempt ID
   * @param pathname path to the file
   * @return the exit code.
   */
  def uploadHistory(appId: String, attempt: Option[String], pathname: String)
      : Int = {
    val config = getConf
    val logPathname = new URI(pathname)
    val logPath = new Path(logPathname)
    val fs = logPath.getFileSystem(config)
    // this will raise a FileNotFoundException or other IOException if the file isn't present
    val status = fs.getFileStatus(logPath)
    if (!status.isFile()) {
      throw new FileNotFoundException(s"Not a file $pathname")
    }

    val timelineVersion1_5 = timelineServiceV1_5Enabled(config)
    val applicationId = ConverterUtils.toApplicationId(appId)
    val attemptId = attempt.map(ConverterUtils.toApplicationAttemptId)
    val timelineWebappAddress = getTimelineEndpoint(config)
    val timelineClient = YarnTimelineUtils.createYarnTimelineClient(config)
    val groupId = if (timelineVersion1_5) Some(appId) else None

    // create the publisher
    val atsPublisher = new EntityPublisher(
      new AppAttemptDetails(applicationId, attemptId, groupId),
      timelineClient,
      timelineWebappAddress,
      timelineVersion1_5,
      millis(YarnHistoryService.POST_RETRY_INTERVAL, DEFAULT_POST_RETRY_INTERVAL),
      millis(POST_RETRY_MAX_INTERVAL, DEFAULT_POST_RETRY_MAX_INTERVAL),
      millis(SHUTDOWN_WAIT_TIME, DEFAULT_SHUTDOWN_WAIT_TIME))

    val batchSize = intOption(BATCH_SIZE, DEFAULT_BATCH_SIZE)
    val postQueueLimit = batchSize + intOption(POST_EVENT_LIMIT, DEFAULT_POST_EVENT_LIMIT)
    val sparkPublisher = new SparkEventPublisher(atsPublisher, batchSize, postQueueLimit)
    val eventsIn = EventLoggingListener.openEventLog(logPath, fs)
    try {
      sparkPublisher.start()

      def process(ev: SparkListenerEvent): Unit = {
        logDebug(s"$ev")
        sparkPublisher.process(ev)
      }
      loadAndApply(pathname, eventsIn, process)
      sparkPublisher.flush()
      sparkPublisher.stop();
    } finally {
      eventsIn.close()
      // idempotent stop operation
      sparkPublisher.stop();
    }

    E_SUCCESS
  }

  /**
   * Load and process each event in the order maintained in the given stream.
   *
   * See: ReplayListenerBus
   *
   * @param logData Stream containing event log data.
   * @param sourceName Filename (or other source identifier) from whence @logData is being read
   */
  def loadAndApply(
      sourceName: String,
      logData: InputStream,
      action: (SparkListenerEvent) => Unit): Unit = {
    var currentLine: String = null
    var lineNumber: Int = 1
    try {
      val lines = Source.fromInputStream(logData).getLines()
      while (lines.hasNext) {
        currentLine = lines.next()
        val event = try {
          JsonProtocol.sparkEventFromJson(parse(currentLine))
        }
        catch {
          case e: Exception =>
            logError(s"Exception parsing Spark event log: $sourceName", e)
            logError(s"Line #$lineNumber: $currentLine\n")
            throw e
        }
        action(event)
        lineNumber += 1
      }
    } catch {
      case ioe: IOException =>
        throw ioe
      case e: Exception =>
        logError(s"Exception parsing Spark event log: $sourceName", e)
        logError(s"Malformed line #$lineNumber: $currentLine\n")
    }
  }
}

private[spark] object Upload {
  val NO_ATTEMPT = "none"
  val USAGE = s"Usage: upload <applicationId> [<attemptId> | $NO_ATTEMPT ] filename"

  def main(args: Array[String]): Unit = {
    ExitUtil.halt(ToolRunner.run(new YarnConfiguration(), new Upload(), args))
  }

  /**
   * Limit on number of posts in the outbound queue -when exceeded
   * new events will be dropped.
   */
  val POST_EVENT_LIMIT = "yarn.timeline.post.limit"

  /**
   * The default limit of events in the post queue.
   */
  val DEFAULT_POST_EVENT_LIMIT = 10000

  /**
   * Interval in milliseconds between POST retries. Every
   * failure causes the interval to increase by this value.
   */
  val POST_RETRY_INTERVAL = "yarn.timeline.post.retry.interval"

  /**
   * The default retry interval in millis.
   */
  val DEFAULT_POST_RETRY_INTERVAL = 1000L

  /**
   * The maximum interval between retries.
   */

  val POST_RETRY_MAX_INTERVAL = "yarn.timeline.post.retry.max.interval"

  /**
   * The default maximum retry interval.
   */
  val DEFAULT_POST_RETRY_MAX_INTERVAL = 60000L

  /**
   * The maximum time in to wait for event posting to complete when the service stops.
   */
  val SHUTDOWN_WAIT_TIME = "yarn.timeline.shutdown.waittime"

  /**
   * Time in millis to wait for shutdown on service stop.
   */
  val DEFAULT_SHUTDOWN_WAIT_TIME = 30000L

  /**
   * Option for the size of the batch for timeline uploads. Bigger: less chatty.
   * Smaller: history more responsive.
   */
  val BATCH_SIZE = "yarn.timeline.batch.size"

  /**
   * The default size of a batch.
   */
  val DEFAULT_BATCH_SIZE = 100

  /**
   * Name of a domain for the timeline.
   */
  val TIMELINE_DOMAIN = "yarn.timeline.domain"

}
