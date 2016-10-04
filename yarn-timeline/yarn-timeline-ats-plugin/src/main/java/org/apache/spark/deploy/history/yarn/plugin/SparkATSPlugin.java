/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.history.yarn.plugin;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;
import org.apache.hadoop.yarn.server.timeline.NameValuePair;
import org.apache.hadoop.yarn.server.timeline.TimelineEntityGroupPlugin;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;

/**
 * This class is designed to be loaded in the YARN application timeline server.
 * <i>Important:</i> this must not include any dependencies which aren't already on the ATS
 * classpath. No references to Spark classes, use of Scala etc.
 * This is why it is in Java, and in its own package.
 */
public class SparkATSPlugin extends TimelineEntityGroupPlugin {

  private static final Logger LOG =
      LoggerFactory.getLogger(SparkATSPlugin.class);

  /**
   * Name of the entity type used to declare summary
   * data of an application.
   */
  private static final String SPARK_SUMMARY_ENTITY_TYPE = "spark_event_v01";

  /**
   * Name of the entity type used to publish full
   * application details.
   */
  private static final String SPARK_DETAIL_ENTITY_TYPE = "spark_event_v01_detail";

  /**
   * Entity `OTHER_INFO` field: YARN application ID.
   */
  private static final String FIELD_APPLICATION_ID = "applicationId";

  /**
   * Entity `OTHER_INFO` field: attempt ID from spark start event.
   */
  private static final String FIELD_ATTEMPT_ID = "attemptId";

  public SparkATSPlugin() {
    LOG.info("SparkATSPlugin");
  }

  @Override
  public Set<TimelineEntityGroupId> getTimelineEntityGroupId(String entityType,
      NameValuePair filter, Collection<NameValuePair> secondaryFilters) {
    LOG.debug("getTimelineEntityGroupId({}, [{}]", entityType, filter);

    Set<TimelineEntityGroupId> result = null;
    try {
      if (entityType.equals(SPARK_DETAIL_ENTITY_TYPE) && filter != null) {
        String value = filter.getValue().toString();
        switch (filter.getName()) {
          case FIELD_APPLICATION_ID:
            result = toGroupId(value);
            break;
          case FIELD_ATTEMPT_ID:
            result = toGroupId(value);
            break;
          default:
            // no-op
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to process entity type {} and primary filter {}",
          entityType, filter, e);
      throw new IllegalArgumentException(e);
    }
    return result;
  }

  @Override
  public Set<TimelineEntityGroupId> getTimelineEntityGroupId(String entityId,
      String entityType) {
    LOG.debug("getTimelineEntityGroupId({}}, {}})", entityId, entityType);

    switch (entityType) {
      case SPARK_SUMMARY_ENTITY_TYPE:
        // return null for summary data
        return null;
      case SPARK_DETAIL_ENTITY_TYPE:
        // extract the ID
        return toGroupId(entityId);
      default:
      return null;
    }
  }

  @Override
  public Set<TimelineEntityGroupId> getTimelineEntityGroupId(String entityType,
      SortedSet<String> entityIds, Set<String> eventTypes) {
    LOG.debug("getTimelineEntityGroupId({}, [{}], [{}]", entityType,
        entityIds.size(), eventTypes.size());

    switch (entityType) {
      case SPARK_SUMMARY_ENTITY_TYPE:
        // return null for summary data
        LOG.debug("{} -> summary", entityType);
        return null;

      case SPARK_DETAIL_ENTITY_TYPE:
        // return the groups, ignore event types
        LOG.debug("{} -> group", entityType);

        Set<TimelineEntityGroupId> result = new HashSet<>();
        for (String entityId : entityIds) {
          result.addAll(toGroupId(entityId));
        }
        return result;

      default:
        return null;
    }
  }

  /**
   * Converts an entity ID to an application ID. This works with an appID or attempt ID
   * string: an application Attempt ID is tried first, and if that fails, its tried as an
   * application
   * @param entityId string of the entity
   * @return an application ID extracted from the entity
   * @throws IllegalArgumentException if it could not be converted
   */
  private ApplicationId entityToApplicationId(String entityId) {
    try {
      return ConverterUtils.toApplicationAttemptId(entityId).getApplicationId();
    } catch (IllegalArgumentException e) {
      return ConverterUtils.toApplicationId(entityId);
    }
  }

  private Set<TimelineEntityGroupId> toGroupId(ApplicationId applicationId) {
    TimelineEntityGroupId groupId = TimelineEntityGroupId.newInstance(
        applicationId,
        applicationId.toString());
    LOG.debug("mapped {} to {}, ", applicationId, groupId);
    Set<TimelineEntityGroupId> result = new HashSet<>();
    result.add(groupId);
    return result;
  }

  private Set<TimelineEntityGroupId> toGroupId(String entityId) {
    ApplicationId applicationId = entityToApplicationId(entityId);
    LOG.debug("Entity {} -> application {}", entityId, applicationId);
    return toGroupId(applicationId);
  }

}
