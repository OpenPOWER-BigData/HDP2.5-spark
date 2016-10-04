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

package org.apache.spark.deploy.history.yarn

import java.util.Date

import com.codahale.metrics.{Counting, Gauge, Metric, MetricRegistry, Timer}

import org.apache.spark.metrics.source.Source

/**
 * An extended metrics source with some operations to build up the registry, and
 * to time a closure.
 */
private[history] trait ExtendedMetricsSource extends Source {

  /** Metrics registry */
  override val metricRegistry = new MetricRegistry()

  /**
   * A map to build up of all metrics to register and include in the string value
   *
   * @return
   */
  def metricsMap: Map[String, Metric]

  protected def register(): Unit = {
    metricsMap.foreach(elt => {
      require(elt._2 != null, s"Null metric for ${elt._1}")
      metricRegistry.register(elt._1, elt._2)
    })
  }

  /**
   * Stringify all the metrics
   * @return a string which can be used in diagnostics and logging
   */
  override def toString: String = {
    metricsToString
  }

  def metricsToString: String = {
    def sb = new StringBuilder()
    metricsMap.foreach(elt => sb.append(s" ${elt._1} = ${elt._2}\n"))
    sb.toString()
  }

  /**
   * Time a closure, returning its output.
   *
   * @param t timer
   * @param f function
   * @tparam T type of return value of the function
   * @return the result of the function.
   */
  def time[T](t: Timer)(f: => T): T = {
    val timeCtx = t.time()
    try {
      f
    } finally {
      timeCtx.close()
    }
  }

  def lookup(name: String): Option[Metric] = {
    metricsMap.get(name)
  }

  /**
   * Get a count or gauge by name; return -1 if it is none
   * @param name metric name
   * @return value or -1
   */
  def metricValue(name: String): Long = {
    valueOfMetric(lookup(name))
  }

  /**
   * Get a count or gauge by name; return its value, or,
   * if the metric is undefined, the default value
   * @param name metric name
   * @param defVal default value
   * @return value or -1
   */
  def metricValue(name: String, defVal: Long): Long = {
    valueOfMetric(lookup(name), defVal)
  }

  /**
   * Get the value of a metric, returning -1 if the metric is undedefined.
   * @param metric the metric
   * @return
   */
  def valueOfMetric(metric: Option[Metric]): Long = {
    valueOfMetric(metric, -1)
  }

  /**
   * The value of a metric, returning the default value if the metric is undefined
   * @param metric metric
   * @param defVal default value
   * @return the value or the default
   */
  def valueOfMetric(metric: Option[Metric], defVal: Long): Long = {
    metric match {
      case Some(c: Counting) =>
        c.getCount
      case Some(c: LongGauge) =>
        c.getValue
      case _ => defVal
    }
  }
}

/**
 * A base class for the long gauge. Defining it explicitly makes
 * it possible for the `ExtendedMetricsSource.count()` method to match
 * on the type properly.
 */
private[spark] abstract class AbstractLongGauge extends Gauge[Long] {
}

/**
 * A Gauge which evaluates the closure
 * @param fn closure
 */
private[spark] class LongGauge(fn: () => Long) extends AbstractLongGauge {
  override def getValue: Long = { fn() }
}


/**
 * A gauge to  count time in milliseconds.
 */
private[spark] class TimeInMillisecondsGauge extends AbstractLongGauge {
  @volatile
  var time: Long = 0L

  /**
   * Set the time to "now"; return the value as set
   *
   * @return the time
   */
  def touch(): Long = {
    val t = System.currentTimeMillis()
    time = t
    t
  }

  override def getValue: Long = time

  /**
   * Return the value as `Date.toString()` unless it is `1/1/70`, in which case "unset" is returned.
   * @return
   */
  override def toString: String = {
    val t = getValue
    if (t <= 0) {
      "unset"
    } else {
      new Date(t).toString
    }
  }


}

/**
 * Convert a boolean value/function to a 0/1 gauge value
 * @param b predicate
 */
private[spark] class BoolGauge(b: () => Boolean) extends AbstractLongGauge {
  override def getValue: Long = {if (b()) 1 else 0}
}
