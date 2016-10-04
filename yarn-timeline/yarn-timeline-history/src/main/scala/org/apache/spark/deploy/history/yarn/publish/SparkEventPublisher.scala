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

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.collection.mutable

import com.codahale.metrics.{Counter, Metric}
import org.apache.hadoop.yarn.api.records.timeline.{TimelineEntity, TimelineEvent}

import org.apache.spark.Logging
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.{ExtendedMetricsSource, LongGauge, SparkAppAttemptDetails, TimeSource, YarnTimelineUtils}
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerBlockUpdated, SparkListenerEvent, SparkListenerExecutorMetricsUpdate}
import org.apache.spark.deploy.history.yarn.publish.PublishMetricNames._

/**
 * This is the higher level event publisher: it converts spark events to timeline entity events,
 * builds up a list of entity events to publish, and, when the the size of a batch
 * (or when a lifecycle event forces it), publishes the entity
 *
 * @param entityPublisher publisher instance
 * @param batchSize Number of events to batch up before posting.
 * @param postQueueLimit Limit on the total number of events permitted.
 */
class SparkEventPublisher(
    entityPublisher: EntityPublisher,
    val batchSize: Int,
    val postQueueLimit: Int
    ) extends AbstractPublisher with TimeSource {

  /**
   * All Startup Operations
   */
  override def start(): Unit = {
    super.start()
    logDebug("Spark Event publisher started")
    entityPublisher.start();
  }

  val timelineVersion1_5 = entityPublisher.timelineVersion1_5

  /** Counter of events queued. */
  val sparkEventsQueued = new Counter()

  /** The number of events which were dropped as the backlog of pending posts was too big. */
  val eventsDropped = new Counter()

  /** How many flushes have taken place? */
  val flushCount = new Counter()

  /** spark attempt details; only set after a start event is received]. */
  private var sparkAttemptDetails: Option[SparkAppAttemptDetails] = _

  /** Application ID received from a [[SparkListenerApplicationStart]]. */
  private var sparkApplicationId: Option[String] = None

  /** Optional Attempt ID string from [[SparkListenerApplicationStart]]. */
  private var sparkApplicationAttemptId: Option[String] = None

  /** Start time of the application, as received in the start event. */
  private var startTime: Long = 0

  /** Start time of the application, as received in the end event. */
  private var endTime: Long = 0

  /** The received application started event; `None` if no event has been received. */
  private var applicationStartEvent: Option[SparkListenerApplicationStart] = None

  /** The received application end event; `None` if no event has been received. */
  private var applicationEndEvent: Option[SparkListenerApplicationEnd] = None

  /** Has a start event been processed? */
  private val appStartEventProcessed = new AtomicBoolean(false)

  /** Has the application event event been processed? */
  val appEndEventProcessed = new AtomicBoolean(false)

  /** Counter of events processed -that is have been through `handleEvent()`. */
  val eventsProcessed = new Counter()

  /**
   * A map to build up of all metrics to register and include in the string value
   *
   * @return the map of metric counters and callbacks
   */
  val metricsMap: Map[String, Metric] = Map(
    SPARK_EVENTS_BATCH_SIZE -> new LongGauge(() => batchSize),
    SPARK_EVENTS_DROPPED -> eventsDropped,
    SPARK_EVENTS_FLUSH_COUNT -> flushCount,
    SPARK_EVENTS_PROCESSED -> eventsProcessed,
    SPARK_EVENTS_QUEUED -> sparkEventsQueued
  )

  override val sourceName: String = "history_spark_event_publisher"

  /** List of events which will be pulled into a timeline entity when created. */
  private var pendingEvents = new mutable.MutableList[TimelineEvent]()

  /**
   * A counter incremented every time a new entity is created. This is included as an "other"
   * field in the entity information -so can be used as a probe to determine if the entity
   * has been updated since a previous check.
   */
  private val entityVersionCounter = new AtomicLong(1)

  /**
   * Can an event be added?
   *
   * The policy is: only if the number of queued entities is below the limit, or the
   * event marks the end of the application.
   *
   * @param isLifecycleEvent is this operation triggered by an application start/end?
   * @return true if the event can be added to the queue
   */
  private def canAddEvent(isLifecycleEvent: Boolean): Boolean = {
    isLifecycleEvent || postQueueHasCapacity
  }

  /**
   * Does the post queue have capacity for this event?
   *
   * @return true if the count of queued events is below the limit
   */
  private def postQueueHasCapacity: Boolean = {
    sparkEventsQueued.getCount < postQueueLimit
  }

  /**
   * Add another event to the pending event list.
   *
   * Returns the size of the event list after the event was added
   * (thread safe).
   *
   * @param event event to add
   * @return the event list size
   */
  private def addPendingEvent(event: TimelineEvent): Int = {
    pendingEvents.synchronized {
      pendingEvents :+= event
      pendingEvents.size
    }
  }

  private def createEntityType(isSummaryEntity: Boolean): String = {
    if (!timelineVersion1_5 || isSummaryEntity) {
      EntityConstants.SPARK_SUMMARY_ENTITY_TYPE
    } else {
      EntityConstants.SPARK_DETAIL_ENTITY_TYPE
    }
  }

  /**
   * Create a timeline entity populated with the state of this history service.
   * @param isSummaryEntity entity type: summary or full
   * @param timestamp timestamp
   * @return the entity.
   */
  private def createTimelineEntity(
      isSummaryEntity: Boolean,
      timestamp: Long,
      entityCount: Long): TimelineEntity = {
    require(sparkAttemptDetails.isDefined,
      "A spark application start event has not yet been received")
    val entity = entityPublisher.createTimelineEntity(
      createEntityType(isSummaryEntity),
      startTime,
      endTime,
      timestamp,
      entityCount)
    YarnTimelineUtils.addSparkAttemptDetails(entity, sparkAttemptDetails.get)
    entity
  }

  /**
   * Set the "spark" application and attempt information -the information
   * provided in the start event. The attempt ID here may be `None`; even
   * if set it may only be unique amongst the attempts of this application.
   * That is: not unique enough to be used as the entity ID
   *
   * @param appId application ID
   * @param attemptId attempt ID
   */
  private def setSparkAppAndAttemptInfo(
      appId: Option[String],
      attemptId: Option[String],
      appName: String,
      userName: String): Unit = {
    logDebug(s"Setting Spark application ID to $appId; attempt ID to $attemptId")
    sparkApplicationId = appId
    sparkApplicationAttemptId = attemptId
    sparkAttemptDetails = Some(SparkAppAttemptDetails(appId, attemptId, appName, userName))
  }

  /**
   * Publish next set of pending events if there are events to publish,
   * and the application has been recorded as started.
   *
   * @return true if another entity was queued
   */
  def flush(): Boolean = {
    // verify that there are events to publish
    val size = pendingEvents.synchronized {
      pendingEvents.size
    }
    if (size > 0 && applicationStartEvent.isDefined) {
      // push if there are events *and* the app is recorded as having started.
      // -as the app name is needed for the the publishing.
      flushCount.inc()
      val t = now()
      val count = entityVersionCounter.getAndIncrement()
      // create the detail entry. On ATS 1.0, this is essentially the
      // summary entry
      val detail = createTimelineEntity(false, t, count)
      // copy in pending events and then reset the list
      var oldPendingEvents: mutable.MutableList[TimelineEvent] = null
      pendingEvents.synchronized {
        oldPendingEvents = pendingEvents
        pendingEvents = new mutable.MutableList[TimelineEvent]()
      }
      oldPendingEvents.foreach(detail.addEvent)

      entityPublisher.queueForPosting(detail)

      if (timelineVersion1_5) {
        // ATS 1.5: push out an updated summary entry
        entityPublisher.queueForPosting(createTimelineEntity(true, t, count))
      }
      true
    } else {
      logDebug(s"Ignoring flush() request: pending event queue+ size=$size;" +
          s" applicationStartEvent=$applicationStartEvent")
      false
    }
  }

  /**
   * Process an action, or, if the service's `stopped` flag is set, discard it.
   *
   * This is the method called by the event listener when forwarding events to the service,
   * and at shutdown.
   *
   * @param event event to process
   * @return true if the event was queued
   */
  def process(event: SparkListenerEvent): Boolean = {
    if (!entityPublisher.isPostingQueueStopped) {
      sparkEventsQueued.inc()
      logDebug(s"Enqueue $event")
      handleEvent(event)
      true
    } else {
      false
    }
  }

  /**
   * If the event reaches the batch size or flush is true, push events to ATS.
   *
   * @param event event. If null, no event is queued, but the post-queue flush logic still applies
   */
  private def handleEvent(event: SparkListenerEvent): Unit = {
    // publish events unless stated otherwise
    var publish = true
    // don't trigger a push to the ATS
    var push = false
    // lifecycle events get special treatment: they are never discarded from the queues,
    // even if the queues are full.
    var isLifecycleEvent = false
    val timestamp = now()
    eventsProcessed.inc()
    if (eventsProcessed.getCount() % 1000 == 0) {
      logDebug(s"${eventsProcessed} events are processed")
    }
    event match {
      case start: SparkListenerApplicationStart =>
        // we already have all information,
        // flush it for old one to switch to new one
        logDebug(s"Handling application start event: $event")
        if (!appStartEventProcessed.getAndSet(true)) {
          applicationStartEvent = Some(start)
          var applicationName = start.appName
          if (applicationName == null || applicationName.isEmpty) {
            logWarning("Application does not have a name")
            applicationName = entityPublisher.applicationInfo.toString
          }
          startTime = if (start.time > 0) start.time else timestamp
          setSparkAppAndAttemptInfo(start.appId, start.appAttemptId, applicationName,
            start.sparkUser)
          logDebug(s"Application started: $event")
          isLifecycleEvent = true
          push = true
        } else {
          logWarning(s"More than one application start event received -ignoring: $start")
          publish = false
        }

      case end: SparkListenerApplicationEnd =>
        if (!appStartEventProcessed.get()) {
          // app-end events being received before app-start events can be triggered in
          // tests, even if not seen in real applications.
          // react by ignoring the event altogether, as an un-started application
          // cannot be reported.
          logError(s"Received application end event without application start $event -ignoring.")
        } else if (!appEndEventProcessed.getAndSet(true)) {
          // the application has ended
          logDebug(s"Application end event: $event")
          applicationEndEvent = Some(end)
          // flush old entity
          endTime = if (end.time > 0) end.time else timestamp
          push = true
          isLifecycleEvent = true
        } else {
          // another test-time only situation: more than one application end event
          // received. Discard the later one.
          logInfo(s"Discarding duplicate application end event $end")
          publish = false
        }

      case update: SparkListenerBlockUpdated =>
        publish = false

      case update: SparkListenerExecutorMetricsUpdate =>
        publish = false

      case _ =>
        publish = true
    }

    if (publish) {
      val tlEvent = toTimelineEvent(event, timestamp)
      val eventCount = if (tlEvent.isDefined && canAddEvent(isLifecycleEvent)) {
        addPendingEvent(tlEvent.get)
      } else {
        // discarding the event
        // if this is due to a full queue, log it
        if (!postQueueHasCapacity) {
          logInfo(s"Queue full at ${sparkEventsQueued.getCount}, limit =$postQueueLimit" +
              s" batch size = $batchSize: discarding event $tlEvent")
          eventsDropped.inc()
        }
        0
      }

      // trigger a push if the batch limit is reached
      // There's no need to check for the application having started, as that is done later.
      if (eventCount >= batchSize) {
        logDebug(s"Event count at $eventCount > $batchSize, pushing queue ")
        // note that push may already have been true; it's unimportant here
        push = true
      }

      if (push) {
        logDebug("Push triggered")
        flush()
      }
    }
  }

  /**
   * Get the number of flush events that have taken place.
   *
   * This includes flushes triggered by the event list being bigger the batch size,
   * but excludes flush operations triggered when the action processor thread
   * is stopped, or if the timeline service binding is disabled.
   *
   * @return count of processed flush events.
   */
  def getFlushCount: Long = {
    flushCount.getCount
  }

  /**
   * Stop the publisher
   */
  def stop(): Unit = {
    if (!entityPublisher.isPostingQueueStopped) {

        if (appStartEventProcessed.get && !appEndEventProcessed.get) {
          // push out an application stop event if none has been received
          logDebug("Generating a SparkListenerApplicationEnd during service stop()")
          process(SparkListenerApplicationEnd(now()))
        }

        // flush out the events
        flush()

      // push out that queue stop event; this immediately sets the `queueStopped` flag
        entityPublisher.pushQueueStop()

        if (!entityPublisher.awaitQueueCompletion()) {
          // there was no running post thread, just stop the timeline client ourselves.
          // (if there is a thread running, it must be the one to stop it)
          entityPublisher.stopTimelineClient()
          logInfo(s"Stopped: $this")
        }
      }
  }

  override def toString(): String = {
    s"""SparkEventPublisher($sparkApplicationId,
       | $sparkApplicationAttemptId,
       | $sourceName
       | ${metricsToString}
       |  )""".stripMargin
  }
}
