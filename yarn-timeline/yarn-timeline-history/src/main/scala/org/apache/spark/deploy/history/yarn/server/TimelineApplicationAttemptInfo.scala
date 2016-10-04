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

package org.apache.spark.deploy.history.yarn.server

import org.apache.spark.deploy.history.{ApplicationAttemptInfo, ApplicationHistoryInfo}
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._

/**
 * Extend [[ApplicationAttemptInfo]] with information about the entityID; this allows
 * the attemptId to be set to something in the web UI for people, rather than
 * display the YARN attempt ID used  to retrieve it from the timeline server.
 *
 * @param attemptId attemptID for GUI
 * @param startTime start time in millis
 * @param endTime end time in millis (or 0)
 * @param lastUpdated updated time in millis
 * @param sparkUser user
 * @param completed flag true if completed
 * @param entityId ID of the YARN timeline server entity containing the data
 * @param sparkAttemptId spark attempt id as saved on the metadata; null means client-side driver
 * @param version spark version as saved on the metadata
 */
private[spark] class TimelineApplicationAttemptInfo(
    attemptId: Option[String],
    startTime: Long,
    endTime: Long,
    lastUpdated: Long,
    sparkUser: String,
    completed: Boolean,
    val entityId: String,
    val sparkAttemptId: Option[String],
    val version: Long = 0,
    val groupId: Option[String] = None)
    extends ApplicationAttemptInfo(attemptId,
      startTime,
      endTime,
      lastUpdated,
      sparkUser,
      completed) {

  /**
   * Describe the application history, including timestamps and completed flag.
   *
   * @return a string description
   */
  override def toString: String = {
    val never = "-"
    s"""TimelineApplicationAttemptInfo: attemptId $attemptId,
       | completed = $completed,
       | sparkAttemptId $sparkAttemptId,
       | started ${timeShort(startTime, never)},
       | ended ${timeShort(endTime, never)},
       | updated ${timeShort(lastUpdated, never)},
       | sparkUser = $sparkUser,
       | version = $version,
       | groupId = $groupId
     """.stripMargin
  }

  /**
   * The equality rules which consider two attempts to be "the same"
   * @param that the other entity
   * @return true if for the purpose of update operations, these two attempts
   *         are considered current and equivalent.
   */
  def sameAs(that: TimelineApplicationAttemptInfo): Boolean = {
    entityId.equals(that.entityId) &&
        ((version > 0 && version == that.version)) ||
        (version == 0 && that.version == 0 && lastUpdated == that.lastUpdated)
  }
}

private[spark] class TimelineApplicationHistoryInfo(
  override val id: String,
  override val name: String,
  override val attempts: List[TimelineApplicationAttemptInfo])
  extends ApplicationHistoryInfo(id, name, attempts)
