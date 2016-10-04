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

package org.apache.spark.deploy.history.yarn.unit

import java.net.{NoRouteToHostException, URI}

import org.apache.hadoop.http.HttpConfig
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.conf.YarnConfiguration._
import org.scalatest.BeforeAndAfter

import org.apache.spark.{Logging, SparkFunSuite}
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.testtools.ExtraAssertions

/**
 * Tests of methods in [[org.apache.spark.deploy.history.yarn.YarnTimelineUtils]].
 */
class YarnTimelineUtilsSuite extends SparkFunSuite
  with BeforeAndAfter with Logging with ExtraAssertions {

  test("verifyNoHost") {
    intercept[NoRouteToHostException] {
      validateEndpoint(new URI("http://0.0.0.0:8080/ws"))
    }
  }

  test("verifyNoPort") {
    intercept[NoRouteToHostException] {
      validateEndpoint(new URI("http://127.0.1.1:0/ws"))
    }
  }

  test("verifyValid") {
    validateEndpoint(new URI("http://127.0.1.1:8080/ws"))
  }

  test("HTTPS timeline") {
    val conf = new YarnConfiguration()
    assert("0.0.0.0:8190" === conf.get(TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS))
    conf.set(YARN_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.toString)
    val address = "localhost:8190"
    conf.set(TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS, address)
    assert(address === conf.get(TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS))
    assert(address === conf.getTrimmed(TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS,
          DEFAULT_TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS))
    assert(address === getTimelineWebappAddress(conf, true))

    assert(s"https://$address/ws/v1/timeline/" === getTimelineEndpoint(conf).toString)
  }

  test("HTTP timeline") {
    val conf = new YarnConfiguration()
    conf.set(YARN_HTTP_POLICY_KEY, HttpConfig.Policy.HTTP_ONLY.toString)
    val address = "localhost:8199"
    conf.set(TIMELINE_SERVICE_WEBAPP_ADDRESS, address)
    assert(address === getTimelineWebappAddress(conf, false))
    assert("http://" + address + "/ws/v1/timeline/" === getTimelineEndpoint(conf).toString)
  }

}
