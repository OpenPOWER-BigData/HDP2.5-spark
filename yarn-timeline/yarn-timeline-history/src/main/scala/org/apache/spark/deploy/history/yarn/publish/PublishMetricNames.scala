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

import com.codahale.metrics.{Gauge, Metric}

object PublishMetricNames {

  val SPARK_EVENTS_DROPPED = "spark_events_dropped"
  val SPARK_EVENTS_PROCESSED = "spark_events_processed"
  val SPARK_EVENTS_QUEUED = "spark_events_queued"
  val SPARK_EVENTS_FLUSH_COUNT = "spark_events_flush_count"
  val SPARK_EVENTS_BATCH_SIZE = "spark_events_batch_size"

  val ENTITY_EVENTS_SUCCESSFULLY_POSTED = "eventsSuccessfullyPosted"
  val ENTITY_POST_ATTEMPTS = "entity_post_attempts"
  val ENTITY_POST_FAILURES = "entity_post_failures"
  val ENTITY_POST_REJECTIONS = "entity_post_rejections"
  val ENTITY_POST_SUCCESSES = "entity_post_successes"
  val ENTITY_POST_TIMER = "entity_post_timer"
  val ENTITY_POST_TIMESTAMP = "entity_post_timestamp"
  val ENTITY_POST_QUEUE_IS_STOPPED = "entity_post_queue_stopped"
  val ENTITY_POST_QUEUE_SIZE = "entity_post_queue_size"
  val ENTITY_POST_QUEUE_EVENT_COUNT = "entity_post_queue_event_count"
}
