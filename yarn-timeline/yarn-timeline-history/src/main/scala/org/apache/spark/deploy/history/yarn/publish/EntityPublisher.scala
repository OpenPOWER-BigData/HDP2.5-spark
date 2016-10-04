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

package org.apache.spark.deploy.history.yarn.publish

import java.io.{Flushable, InterruptedIOException}
import java.net.{ConnectException, URI}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{LinkedBlockingDeque, TimeUnit}

import scala.collection.JavaConverters._

import com.codahale.metrics.{Counter, Metric, Timer}
import org.apache.hadoop.yarn.api.records.{ApplicationAttemptId, ApplicationId}
import org.apache.hadoop.yarn.api.records.timeline.{TimelineDomain, TimelineEntity, TimelineEntityGroupId}
import org.apache.hadoop.yarn.client.api.TimelineClient

import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.{AppAttemptDetails, BoolGauge, LongGauge, TimeInMillisecondsGauge, TimeSource, YarnTimelineUtils}
import org.apache.spark.deploy.history.yarn.publish.PublishMetricNames._

/**
 * This is the class which publishes entities containing events to the timeline server.
 *
 * It contains a queue of events to post, and a thread in the background which
 * publishes them in order
 *
 * @param applicationInfo info about the attempt
 * @param timelineClient timeline client
 * @param timelineWebappAddress URI of the timeline (used in logs)
 * @param timelineVersion1_5 Does the the timeline server support v 1.5 APIs?
 * @param retryInterval the initial and incrementing interval for POST retries
 * @param retryIntervalMax the max interval for POST retries
 * @param shutdownWaitTime How long to wait in millseconds for shutdown before giving up
 */
private[yarn] class EntityPublisher(
    val applicationInfo: AppAttemptDetails,
    timelineClient: TimelineClient,
    val timelineWebappAddress: URI,
    val timelineVersion1_5: Boolean,
    val retryInterval: Long,
    val retryIntervalMax: Long,
    val shutdownWaitTime: Long)
    extends AbstractPublisher with TimeSource {

  /** ATS v 1.5 group ID. */
  val groupId = if (timelineVersion1_5) {
    Some(TimelineEntityGroupId.newInstance(applicationInfo.appId, applicationInfo.groupId.get))
  } else {
    None
  }

  /** Domain ID for entities: may be null. */
  private var domainId: Option[String] = None

  /** Queue of entities to asynchronously post, plus the number of events in each entry. */
  private val postingQueue = new LinkedBlockingDeque[PostQueueAction]()

  def postingQueueSize: Int = { postingQueue.size() }

  /** Number of events in the post queue. */
  private val _postQueueEventCount = new AtomicLong

  /**
   * Number of events in the queue
   * @return
   */
  def postQueueEventCount: Long = _postQueueEventCount.get()

  /** Event handler thread. */
  private var entityPostThread: Option[Thread] = None

  /** Flag to indicate the queue is stopped; events aren't being processed. */
  private val postingQueueStopped = new AtomicBoolean(true)

  def isPostingQueueStopped: Boolean = postingQueueStopped.get()

  /** Boolean to track when the post thread is active; Set and reset in the thread itself. */
  private val postThreadActive = new AtomicBoolean(false)

  /** Name for metrics: yarn_history */
  override val sourceName = "event_publisher_metrics"

  /** Counter of events successfully posted. */
  val eventsSuccessfullyPosted = new Counter()

  /** Counter of number of attempts to post entities. */
  val entityPostAttempts = new Counter()

  /** Counter of number of successful entity post operations. */
  val entityPostSuccesses = new Counter()

  /** How many entity postings failed? */
  val entityPostFailures = new Counter()

  /** How many entity postings were rejected? */
  val entityPostRejections = new Counter()

  /** Timer to build up statistics on post operation times */
  val postOperationTimer = new Timer()

  val postTimestamp = new TimeInMillisecondsGauge()

  /**
   * The metrics of this class
   */
  val metricsMap: Map[String, Metric] = Map(
    ENTITY_EVENTS_SUCCESSFULLY_POSTED -> eventsSuccessfullyPosted,
    ENTITY_POST_ATTEMPTS -> entityPostAttempts,
    ENTITY_POST_FAILURES -> entityPostFailures,
    ENTITY_POST_REJECTIONS -> entityPostRejections,
    ENTITY_POST_SUCCESSES -> entityPostSuccesses,
    ENTITY_POST_TIMER -> postOperationTimer,
    ENTITY_POST_TIMESTAMP -> postTimestamp,
    ENTITY_POST_QUEUE_IS_STOPPED -> new BoolGauge(() => postingQueueStopped.get),
    ENTITY_POST_QUEUE_SIZE -> new LongGauge(() => postingQueueSize),
    ENTITY_POST_QUEUE_EVENT_COUNT -> new LongGauge(() => postQueueEventCount)
    )

  /**
   * Initialization: register the metrics locally
   */
  register()

  /**
   * Is the asynchronous posting thread active?
   *
   * @return true if the post thread has started; false if it has not yet/ever started, or
   *         if it has finished.
   */
  def isPostThreadActive: Boolean = postThreadActive.get

  /**
   * All Startup Operations
   */
  override def start(): Unit = {
    super.start()
    // declare that the processing is started
    postingQueueStopped.set(false)
    val thread = new Thread(new EntityPoster(), "EventPoster")
    entityPostThread = Some(thread)
    thread.setDaemon(true)
    thread.start()
  }

  /**
   * Reset the timeline client. Idempotent.
   *
   * 1. Stop the timeline client service if running.
   * 2. set the `timelineClient` field to `None`
   */
  def stopTimelineClient(): Unit = {
    timelineClient.stop()
  }

  /** YARN Application ID. */
  def applicationId: ApplicationId = {
    applicationInfo.appId
  }

  /**
   * YARN Attempt ID.
   * @return the attempt ID, if defined
   */
  def attemptId: Option[ApplicationAttemptId] = {
    applicationInfo.attemptId
  }

  /**
   * Create a timeline domain and PUT it to ATS
   * @param domain domain ID
   * @param readers reader permissions
   * @param writers writer permissions
   * @return the created instance
   */
  def putNewDomain(
      domain: String,
      readers: String,
      writers: String): TimelineDomain = {
    logInfo(s"Creating domain $domain with readers: $readers and writers: $writers")
    val timelineDomain = new TimelineDomain()
    timelineDomain.setId(domain)
    timelineDomain.setReaders(readers)
    timelineDomain.setWriters(writers)
    timelineClient.putDomain(timelineDomain)
    timelineDomain
  }

  /**
   * A `StopQueueAction` action has a size of 0
   *
   */
  def pushQueueStop(): Unit = {
    postingQueueStopped.set(true)
    postingQueue.add(StopQueueAction(now(), shutdownWaitTime))
  }


  /**
   * Queue an entity for posting; also increases
   * `_postQueueEventCount` by the size of the entity.
   *
   * @param timelineEntity entity to push
   */
  def queueForPosting(timelineEntity: TimelineEntity): Unit = {
    // queue the entity for posting
    preflightCheck(timelineEntity)
    val e = new PostEntity(timelineEntity)
    _postQueueEventCount.addAndGet(e.size)
    postingQueue.add(e)
  }

  /**
   * Push a `PostQueueAction` to the start of the queue; also increments
   * `_postQueueEventCount` by the size of the action.
   *
   * @param action action to push
   */
  private def pushToFrontOfQueue(action: PostQueueAction): Unit = {
    postingQueue.push(action)
    _postQueueEventCount.addAndGet(action.size)
  }

  /**
   * Take from the posting queue; decrements `_postQueueEventCount` by the size
   * of the action.
   *
   * @return the action
   */
  private def takeFromPostingQueue(): PostQueueAction = {
    val taken = postingQueue.take()
    _postQueueEventCount.addAndGet(-taken.size)
    taken
  }

  /**
   * Poll from the posting queue; decrements  [[_postQueueEventCount]] by the size
   * of the action.
   *
   * @return
   */
  private def pollFromPostingQueue(mills: Long): Option[PostQueueAction] = {
    val taken = postingQueue.poll(mills, TimeUnit.MILLISECONDS)
    _postQueueEventCount.addAndGet(-taken.size)
    Option(taken)
  }

  /**
   * Perform any preflight checks.
   *
   * This is just a safety check to catch regressions in the code which
   * publish data that cannot be parsed at the far end.
   *
   * @param entity timeline entity to review.
   */
  private def preflightCheck(entity: TimelineEntity): Unit = {
    require(entity.getStartTime != null,
      s"No start time in ${describeEntity(entity)}")
  }

  /**
   * Generate the timeline entity
   * @param entityType the entity type to declare the entity as
   * @param startTime time in milliseconds when this entity was started (must be non zero)
   * @param endTime time in milliseconds when this entity was last updated (0 means not ended)
   * @param lastUpdated time in milliseconds when this entity was last updated (0 leaves unset)
   * @return the timeline entity
   */
  def createTimelineEntity(
      entityType: String,
      startTime: Long,
      endTime: Long,
      lastUpdated: Long,
      entityCount: Long): TimelineEntity = {
    YarnTimelineUtils.createTimelineEntity(entityType, applicationInfo, startTime, endTime,
      lastUpdated, entityCount);
  }

    /**
   * Post events until told to stop.
   */
  private class EntityPoster extends Runnable {

    override def run(): Unit = {
      postThreadActive.set(true)
      try {
        val shutdown = postEntities(retryInterval, retryIntervalMax)
        // getting here means the `stop` flag is true
        postEntitiesShutdownPhase(shutdown, retryInterval)
        logInfo(s"Stopping dequeue service, final queue size is ${postingQueue.size};" +
            s" outstanding events to post count: ${_postQueueEventCount.get()}")
      } catch {
        // handle exceptions triggering thread exit. Interrupts are good; others less welcome.
        case ex: InterruptedException =>
          logInfo("Entity Posting thread interrupted")
          logDebug("Entity Posting thread interrupted", ex)

        case ex: InterruptedIOException =>
          logInfo("Entity Posting thread interrupted")
          logDebug("Entity Posting thread interrupted", ex)

        case ex: Exception =>
          logError("Entity Posting thread exiting after exception raised", ex)
      } finally {
        stopTimelineClient()
        postThreadActive synchronized {
          // declare that this thread is no longer active
          postThreadActive.set(false)
          // and notify all listeners of this fact
          postThreadActive.notifyAll()
        }
      }
    }
  }

  /**
   * Wait for and then post entities until stopped.
   *
   * Algorithm.
   *
   * 1. The thread waits for events in the [[postingQueue]] until stopped or interrupted.
   * 1. Failures result in the entity being queued for resending, after a delay which grows
   * linearly on every retry.
   * 1. Successful posts reset the retry delay.
   * 1. If the process is interrupted, the loop continues with the `stopFlag` flag being checked.
   *
   * To stop this process then, first set the `stopFlag` flag, then interrupt the thread.
   *
   * @param retryInterval delay in milliseconds for the first retry delay; the delay increases
   *        by this value on every future failure. If zero, there is no delay, ever.
   * @param retryMax maximum interval time in milliseconds
   * @return the [[StopQueueAction]] received to stop the process.
   */
  private def postEntities(retryInterval: Long, retryMax: Long): StopQueueAction = {
    var lastAttemptFailed = false
    var currentRetryDelay = retryInterval
    var result: StopQueueAction = null
    while (result == null) {
      takeFromPostingQueue() match {
        case PostEntity(entity) =>
          postOneEntity(entity) match {
            case Some(ex) =>
              // something went wrong
              if (!postingQueueStopped.get()) {
                if (!lastAttemptFailed) {
                  // avoid filling up logs with repeated failures
                  logWarning(s"Exception submitting entity to $timelineWebappAddress", ex)
                }
                // log failure and queue for posting again
                lastAttemptFailed = true
                // push back to the head of the queue
                postingQueue.addFirst(PostEntity(entity))
                currentRetryDelay = Math.min(currentRetryDelay + retryInterval, retryMax)
                if (currentRetryDelay > 0) {
                  Thread.sleep(currentRetryDelay)
                }
              }
            case None =>
              // success; reset flags and retry delay
              lastAttemptFailed = false
              currentRetryDelay = retryInterval
              postTimestamp.touch()
          }

        case stop: StopQueueAction =>
          logDebug("Queue stopped")
          result = stop
      }
    }
    result
  }

  /**
   * Post a single entity.
   *
   * Any network/connectivity errors will be caught and logged, and returned as the
   * exception field in the returned tuple.
   *
   * Any posting which generates a response will result in the timeline response being
   * returned. This response *may* contain errors; these are almost invariably going
   * to re-occur when resubmitted.
   *
   * @param entity entity to post
   * @return Any exception other than an interruption raised during the operation.
   * @throws InterruptedException if an [[InterruptedException]] or [[InterruptedIOException]] is
   * received. These exceptions may also get caught and wrapped in the ATS client library.
   */
  private def postOneEntity(entity: TimelineEntity): Option[Exception] = {
    domainId.foreach(entity.setDomainId)
    val entityDescription = describeEntity(entity)
    logInfo(s"About to publish entity ${entity.getEntityType}/${entity.getEntityId}" +
        s" with ${entity.getEvents.size()} events" +
        s" to timeline service $timelineWebappAddress")
    logDebug(s"About to publish $entityDescription")
    val timeContext = postOperationTimer.time()
    entityPostAttempts.inc()
    try {
      val response = if (timelineVersion1_5) {
        timelineClient.putEntities(attemptId.orNull, groupId.get, entity)
      } else {
        timelineClient.putEntities(entity)
      }
      val errors = response.getErrors
      if (errors.isEmpty) {
        logDebug(s"entity successfully published")
        entityPostSuccesses.inc()
        eventsSuccessfullyPosted.inc(entity.getEvents.size())
        // and flush the timeline if it implements the API
        timelineClient match {
          case flushable: Flushable =>
            flushable.flush()
          case _ =>
        }
      } else {
        // The ATS service rejected the request at the API level.
        // this is something we assume cannot be re-tried
        entityPostRejections.inc()
        logError(s"Failed to publish $entityDescription")
        errors.asScala.foreach { err =>
          logError(describeError(err))
        }
      }
      // whether accepted or rejected, this request is not re-issued
      None
    } catch {

      case e: InterruptedException =>
        // interrupted; this will break out of IO/Sleep operations and
        // trigger a rescan of the stopped() event.
        throw e

      case e: ConnectException =>
        // connection failure: network, ATS down, config problems, ...
        entityPostFailures.inc()
        logDebug(s"Connection exception submitting $entityDescription", e)
        Some(e)

      case e: Exception =>
        val cause = e.getCause
        if (cause.isInstanceOf[InterruptedException]) {
          // hadoop 2.7 retry logic wraps the interrupt
          throw cause
        }
        // something else has gone wrong.
        entityPostFailures.inc()
        logDebug(s"Could not handle history entity: $entityDescription", e)
        Some(e)

    } finally {
      val duration = timeContext.stop()
      logDebug(s"Duration of posting: $duration nS")
    }
  }

  /**
   * Shutdown phase: continually post oustanding entities until the timeout has been exceeded.
   * The interval between failures is the retryInterval: there is no escalation, and if
   * is longer than the remaining time in the shutdown, the remaining time sets the limit.
   *
   * @param shutdown shutdown parameters.
   * @param retryInterval delay in milliseconds for every delay.
   */
  private def postEntitiesShutdownPhase(shutdown: StopQueueAction, retryInterval: Long): Unit = {
    val timeLimit = shutdown.timeLimit
    val timestamp = YarnTimelineUtils.timeShort(timeLimit, "")
    logDebug(s"Queue shutdown, time limit= $timestamp")
    while (now() < timeLimit && !postingQueue.isEmpty) {
      pollFromPostingQueue(timeLimit - now()) match {
        case Some(PostEntity(entity)) =>
          postOneEntity(entity).foreach {
            case ex: InterruptedException => throw ex
            case ex: InterruptedIOException => throw ex
            case ex: Exception =>
              // failure, push back to try again
              pushToFrontOfQueue(PostEntity(entity))
              if (retryInterval > 0) {
                Thread.sleep(retryInterval)
              } else {
                // there's no retry interval, so fail immediately
                throw ex
              }
          }
        case Some(StopQueueAction(_, _)) =>
          // ignore these
          logDebug("Ignoring StopQueue action")

        case None =>
        // get here then the queue is empty; all is well
      }
    }
  }

  def awaitQueueCompletion(): Boolean = {
    var shutdown = false
    if (postThreadActive.get) {
      postThreadActive.synchronized {
        // check it hasn't switched state
        if (postThreadActive.get) {
          logDebug(s"Stopping posting thread and waiting $shutdownWaitTime mS")
          shutdown = true
          postThreadActive.wait(shutdownWaitTime)
          // then interrupt the thread if it is still running
          if (postThreadActive.get) {
            logInfo("Interrupting posting thread after $shutdownWaitTime mS")
            entityPostThread.foreach(_.interrupt())
          }
        }
      }
    }
    shutdown
  }

  /**
   * Return a summary of the service state to help diagnose problems
   * during test runs, possibly even production.
   *
   * @return a summary of the current service state
   */

  override def toString(): String = s"""EventPublisher(
     | domainId=$domainId,
     | postThreadActive=$postThreadActive,
     | ${metricsToString}
     | """.stripMargin
}



/** Actions in the post queue */
private[yarn] sealed trait PostQueueAction {
  /**
   * Number of events in this entry
   *
   * @return a natural number
   */
  def size: Int
}

/**
 * A `StopQueueAction` action has a size of 0
 *
 * @param currentTime time when action was queued.
 * @param waitTime time for shutdown to wait
 */
private[yarn] case class StopQueueAction(currentTime: Long, waitTime: Long)
    extends PostQueueAction {
  override def size: Int = 0

  def timeLimit: Long = currentTime + waitTime
}

/**
 *  A `PostEntity` action has a size of the number of listed events
 */
private[yarn] case class PostEntity(entity: TimelineEntity) extends PostQueueAction {
  override def size: Int = entity.getEvents.size()
}
