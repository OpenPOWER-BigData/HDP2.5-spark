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

package org.apache.spark.deploy.history.yarn.integration

import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._
import org.apache.spark.deploy.yarn.history.{YarnHistoryProvider, YarnHistoryService}

/**
 * Use the old names in the HDP 1.3/1.4 release, verify they all hookup
 */
class DeprecatedClassesSuite extends WebsiteIntegrationSuite {

  /**
   * Add the history service to the spark conf
   *
   * @param sparkConf configuratin to patch
   * @return the patched configuration
   */
  override def addHistoryService(sparkConf: SparkConf): SparkConf = {
    sparkConf.set("spark.yarn.services",
      "org.apache.spark.deploy.yarn.history.YarnHistoryService")
  }

  override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    super.setupConfiguration(sparkConf)
        .set(SPARK_HISTORY_PROVIDER, "org.apache.spark.deploy.yarn.history.YarnHistoryProvider")
  }
}
