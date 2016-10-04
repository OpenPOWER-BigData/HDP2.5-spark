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

import java.io.FileNotFoundException

import org.apache.commons.io.IOUtils
import org.apache.hadoop.util.{ExitUtil, ToolRunner}
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.publish.EntityConstants._
import org.apache.spark.deploy.history.yarn.rest.JerseyBinding
import org.apache.spark.deploy.history.yarn.server.TimelineQueryClient
import org.apache.spark.deploy.history.yarn.server.TimelineQueryClient._

/**
 * List all Spark applications in the YARN timeline server.
 *
 * If arguments are passed in, then they are accepted as attempt IDs and explicitly retrieved.
 *
 * Everything goes to stdout
 */
private[spark] class Ls extends TimelineCommand {

  import org.apache.spark.deploy.history.yarn.commands.TimelineCommand._

  /**
   * Execute the operation.
   * @param args list of arguments
   * @return the exit code.
   */
  override def exec(args: Seq[String]): Int = {
    val yarnConf = getConf
    val timelineEndpoint = getTimelineEndpoint(yarnConf)
    logInfo(s"Timeline server is at $timelineEndpoint")
    var result = E_SUCCESS
    var client: TimelineQueryClient = null
    try {
      client = new TimelineQueryClient(timelineEndpoint, yarnConf,
        JerseyBinding.createClientConfig())
      if (args.isEmpty) {
        client.listEntities(SPARK_SUMMARY_ENTITY_TYPE,
          fields = Seq(PRIMARY_FILTERS, OTHER_INFO))
            .foreach(e => logInfo(describeEntity(e)))
      } else {
        args.foreach { entity =>
          logInfo(entity)
          try {
            val tle = client.getEntity(SPARK_SUMMARY_ENTITY_TYPE, entity)
            logInfo(describeEntity(tle))
          } catch {
            // these inner failures can be caught and swallowed without stopping
            // the rest of the iteration
            case notFound: FileNotFoundException =>
              // one of the entities was missing: report and continue with the rest
              logInfo(s"Not found: $entity")
              result = E_NOT_FOUND
          }
        }
      }
    } catch {

      case notFound: FileNotFoundException =>
        logInfo(s"Not found: $timelineEndpoint")
        result = E_NOT_FOUND
    } finally {
      IOUtils.closeQuietly(client)
    }

    result
  }
}

private[spark] object Ls {
  def main(args: Array[String]): Unit = {
    ExitUtil.halt(ToolRunner.run(new YarnConfiguration(), new Ls(), args))
  }
}

