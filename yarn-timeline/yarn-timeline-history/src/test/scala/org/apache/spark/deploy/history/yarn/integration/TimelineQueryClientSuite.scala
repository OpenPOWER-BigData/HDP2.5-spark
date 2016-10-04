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

import java.io.IOException
import java.net.{URI, URL}

import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity

import org.apache.spark.deploy.history.yarn.commands.Ls
import org.apache.spark.deploy.history.yarn.publish.EntityConstants._
import org.apache.spark.deploy.history.yarn.rest.HttpRequestException
import org.apache.spark.deploy.history.yarn.rest.JerseyBinding._
import org.apache.spark.deploy.history.yarn.server.TimelineQueryClient
import org.apache.spark.deploy.history.yarn.server.TimelineQueryClient._
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._

class TimelineQueryClientSuite extends AbstractHistoryIntegrationTests {

  private var timeline: URI = _
  var queryClient: TimelineQueryClient = _

  override def setup(): Unit = {
    super.setup()
    historyService = startHistoryService(sc)
    timeline = historyService.timelineWebappAddress
    queryClient = createTimelineQueryClient()
  }

  test("About") {
    val response = queryClient.about()
    logInfo(s"$timeline/about => \n$response")
    assertNotNull(response, s"$queryClient about()")
    assertContains(response, "Timeline")
  }

  def assertNilQuery(filter: String, fields: Seq[String] = Nil): Unit = {
    assertNil(queryClient.listEntities(filter), s"list with filter $filter")
  }

  test("ListNoEntityTypes") {
    assertNilQuery(SPARK_SUMMARY_ENTITY_TYPE)
  }

  test("List LAST_EVENT_ONLY") {
    assertNilQuery(SPARK_SUMMARY_ENTITY_TYPE, Seq(LAST_EVENT_ONLY))
  }

  test("List RELATED_ENTITIES") {
    assertNilQuery(SPARK_SUMMARY_ENTITY_TYPE, Seq(RELATED_ENTITIES))
  }

  test("List LAST_EVENT_ONLY | PRIMARY_FILTERS") {
    assertNil(queryClient.listEntities(SPARK_SUMMARY_ENTITY_TYPE,
        fields = Seq(LAST_EVENT_ONLY, TimelineQueryClient.PRIMARY_FILTERS)),
      "List LAST_EVENT_ONLY | PRIMARY_FILTERS")
  }

  test("List OTHER_INFO") {
    assertNilQuery(SPARK_SUMMARY_ENTITY_TYPE, Seq(OTHER_INFO))
  }

  test("List PRIMARY_FILTERS") {
    assertNilQuery(SPARK_SUMMARY_ENTITY_TYPE, Seq(PRIMARY_FILTERS))
  }

  test("List EVENTS") {
    assertNilQuery(SPARK_SUMMARY_ENTITY_TYPE, Seq(EVENTS))
  }

  test("Ls command") {
    val ls = new Ls()
    ls.setConf(historyService.yarnConfiguration)
    ls.exec(Seq()) should be(0)
    ls.exec(Seq(applicationId.toString)) should be(44)
//    ls.exec(Seq("unparseable")) should be(-1)
  }

  test("PostEntity") {
    describe("post an entity and then retrieve it")
    val te = new TimelineEntity
    te.setStartTime(now())
    te.setEntityId(applicationId.toString)
    te.setEntityType(SPARK_SUMMARY_ENTITY_TYPE)
    te.addPrimaryFilter(FILTER_APP_START, FILTER_APP_START_VALUE)
    completed(applicationId)

    val timelineClient = historyService.timelineClient
    timelineClient.putEntities(te)
    val timelineEntities = awaitSequenceSize(1, "applications on ATS", TIMELINE_SCAN_DELAY,
      () => queryClient.listEntities(SPARK_SUMMARY_ENTITY_TYPE))
    assertEntitiesEqual(te, timelineEntities.head)

    val entity2 = queryClient.getEntity(SPARK_SUMMARY_ENTITY_TYPE, te.getEntityId() )
    assertEntitiesEqual(te, entity2)

    val listing2 = queryClient.listEntities(SPARK_SUMMARY_ENTITY_TYPE,
        primaryFilter = Some((FILTER_APP_START, FILTER_APP_START_VALUE)))
    assert(1 === listing2.size, s"filtering on $FILTER_APP_START:$FILTER_APP_START_VALUE")
    // filtered query
    assertEntitiesEqual(te, listing2.head)

    val ls = new Ls()
    ls.setConf(historyService.yarnConfiguration)
    ls.exec(Seq(applicationId.toString)) should be(0)
  }

  def createTimelineClientRootPath(): TimelineQueryClient = {
    val realTimelineEndpoint = historyService.timelineWebappAddress.toURL
    val rootTimelineServer = new URL(realTimelineEndpoint, "/").toURI
    new TimelineQueryClient(rootTimelineServer,
        sc.hadoopConfiguration,
        createClientConfig())
  }

  test("Client about() Against Wrong URL") {
    intercept[IOException] {
      createTimelineClientRootPath().about()
    }
  }

  test("Client endpointcheck() Against Wrong URL") {
    val client = createTimelineClientRootPath()
    val ex = intercept[HttpRequestException] {
      client.endpointCheck()
    }
    assertExceptionMessageContains(ex, "text/html")
  }

}
