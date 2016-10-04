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

import java.util.zip.ZipOutputStream

import scala.collection.mutable

import org.apache.spark._
import org.apache.spark.deploy.history.{ApplicationHistoryProvider, _}

/**
 * A History provider which wraps up both the ATS and FsHistory Providers.
 * - Enumeration is done by listing both providers and merging the results.
 * - Retrieving instances is slightly trickier as the origin needs to be tracked
 *   and the appropriate provider called.
 */
private[spark] class JointHistoryProvider(sparkConf: SparkConf)
    extends ApplicationHistoryProvider with Logging {

  private var atsHistory: Option[ApplicationHistoryProvider] = None

  private var fsHistory: Option[ApplicationHistoryProvider] = None

  /**
   * The map used for application lookup
   */
  @volatile
  private var applicationMap: Map[String, JointApplicationHistoryInfo] = Map()

  init()

  def init(): Unit = {
    var launchFailures = 0
    var expected = 0
    var lastFailure: Option[Exception] = None

    def launch(name: String, option: String,
        f: () => ApplicationHistoryProvider): Option[ApplicationHistoryProvider] = {
      if (sparkConf.getBoolean(option, true)) {
        try {
          expected += 1
          logInfo("Creating History provider $name")
          Some(f())
        } catch {
          case e: Exception =>
            launchFailures += 1
            lastFailure = Some(e)
            logError(s"History provider $name failed to launch", e)
            None
        }
      } else {
        logDebug(s"$name not enabled")
        None
      }
    }
    fsHistory = launch("Filesystem", JointHistoryProvider.ENABLE_FS,
      () => new FsHistoryProvider(sparkConf) )
    atsHistory = launch("YARN ATS", JointHistoryProvider.ENABLE_ATS,
      () => new YarnHistoryProvider(sparkConf) )

    if (atsHistory.isEmpty && fsHistory.isEmpty) {
      val text = "Both ATS and filesystem histories are disabled: no histories will be retrieved"
      logError(text)
      throw lastFailure.getOrElse(new SparkException("text"))
    }
  }

  /**
   * Look up the provider for an application from the map.
   * If the application is not found in the history of either provider,
   * `None` is returned
   * @param appId application ID
   */
  def lookupProvider(appId: String): Option[ApplicationHistoryProvider] = {
    applicationMap.get(appId).flatMap { info: JointApplicationHistoryInfo =>
      if (info.fromAts) {
        atsHistory
      } else {
        fsHistory
      }
    }
  }

  /**
   * Returns a list of applications available for the history server to show.
   *
   * @return List of all know applications.
   */
  override def getListing(): Iterable[ApplicationHistoryInfo] = {
    var joint: scala.collection.mutable.Map[String, JointApplicationHistoryInfo] = mutable.Map()
    joint ++ fsHistory.map(_.getListing().map(h => new JointApplicationHistoryInfo(h, false)))
    joint ++ atsHistory.map(_.getListing().map(h => new JointApplicationHistoryInfo(h, true)))
    val asImmutable = joint.toMap
    updateApplicationMap(asImmutable)
    asImmutable.values
  }

  private def updateApplicationMap(update: Map[String, JointApplicationHistoryInfo]): Unit = {
    applicationMap = update
  }

  /**
   * Returns the Spark UI for a specific application.
   *
   * @param appId The application ID.
   * @param attemptId The application attempt ID (or None if there is no attempt ID).
   * @return a [[LoadedAppUI]] instance containing the application's UI and any state information
   *         for update probes, or `None` if the application/attempt is not found.
   */
  override def getAppUI(appId: String, attemptId: Option[String]): Option[LoadedAppUI] = {

    lookupProvider(appId).flatMap(_.getAppUI(appId, attemptId))
  }

  /**
   * Writes out the event logs to the output stream provided. The logs will be compressed into a
   * single zip file and written out.
   * @throws SparkException if the logs for the app id cannot be found.
   */
  override def writeEventLogs(appId: String,
      attemptId: Option[String],
      zipStream: ZipOutputStream): Unit = {

    lookupProvider(appId) match {
      case Some(provider) =>
        provider.writeEventLogs(appId, attemptId, zipStream)
      case None =>
        throw new SparkException(s"Failed to write log for $appId/$attemptId")
    }
  }

  override def toString: String = {
    val sb = new StringBuilder()
    sb.append("Joint History Provider;")
    fsHistory.foreach(h => sb.append(s"FS History provider $h;"))
    atsHistory.foreach(h => sb.append(s"ATS History provider $h;"))
    sb.toString()
  }

  /**
   * Returns configuration data to be shown in the History Server home page.
   * Merges in both the FS and ATS elements
   *
   * @return A map with the configuration data. Data is show in the order returned by the map.
   */
  override def getConfig(): Map[String, String] = {
    val m: mutable.Map[String, String] = mutable.Map()
    m ++ super.getConfig()
    forEachProvider(h => m ++ h.getConfig())
    m.toMap
  }

  /**
   * Stop the underlying providers
   */
  override def stop(): Unit = {
    forEachProvider(_.stop())
  }

  def forEachProvider(f: (ApplicationHistoryProvider) => Unit): Unit = {
    fsHistory.foreach(f)
    atsHistory.foreach(f)
  }

}

/**
 * The application history information has to track the origin
 * @param source source history
 * @param fromAts flag to indicate this is an ATS event
 */
private[spark] class JointApplicationHistoryInfo(val source: ApplicationHistoryInfo,
    val fromAts: Boolean) extends ApplicationHistoryInfo(source.id, source.name, source.attempts) {
  override def toString: String = s"History $id $name attempts=${attempts.size} fromAts=$fromAts"
}

private[spark] object JointHistoryProvider {

  val ENABLE_FS = "spark.history.joint.fs.enable";
  val ENABLE_ATS = "spark.history.joint.ats.enable";
}
