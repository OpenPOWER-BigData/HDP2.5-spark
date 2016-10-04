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

package org.apache.spark.cloud.s3

/**
 * S3A constants. Different Hadoop versions have an incomplete set of these;
 */
object S3AConstants {
  val ACCESS_KEY = "fs.s3a.access.key"
  val SECRET_KEY = "fs.s3a.secret.key"
  val AWS_CREDENTIALS_PROVIDER = "fs.s3a.aws.credentials.provider"
  val ANONYMOUS_CREDENTIALS = "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
  val SESSION_TOKEN = "fs.s3a.session.token"
  val MAXIMUM_CONNECTIONS = "fs.s3a.connection.maximum"
  val SECURE_CONNECTIONS = "fs.s3a.connection.ssl.enabled"
  val ENDPOINT = "fs.s3a.endpoint"
  val PATH_STYLE_ACCESS = "fs.s3a.path.style.access"
  val PROXY_HOST = "fs.s3a.proxy.host"
  val PROXY_PORT = "fs.s3a.proxy.port"
  val PROXY_USERNAME = "fs.s3a.proxy.username"
  val PROXY_PASSWORD = "fs.s3a.proxy.password"
  val PROXY_DOMAIN = "fs.s3a.proxy.domain"
  val PROXY_WORKSTATION = "fs.s3a.proxy.workstation"
  val MAX_ERROR_RETRIES = "fs.s3a.attempts.maximum"
  val ESTABLISH_TIMEOUT = "fs.s3a.connection.establish.timeout"
  val SOCKET_TIMEOUT = "fs.s3a.connection.timeout"
  val MAX_PAGING_KEYS = "fs.s3a.paging.maximum"
  val MAX_THREADS = "fs.s3a.threads.max"
  val KEEPALIVE_TIME = "fs.s3a.threads.keepalivetime"
  val MAX_TOTAL_TASKS = "fs.s3a.max.total.tasks"
  val MULTIPART_SIZE = "fs.s3a.multipart.size"
  val MIN_PERMITTED_MULTIPART_SIZE = 5 * (1024 * 1024)
  val MIN_MULTIPART_THRESHOLD = "fs.s3a.multipart.threshold"
  val ENABLE_MULTI_DELETE = "fs.s3a.multiobjectdelete.enable"
  val BUFFER_DIR = "fs.s3a.buffer.dir"
  val FAST_UPLOAD = "fs.s3a.fast.upload"
  val FAST_BUFFER_SIZE = "fs.s3a.fast.buffer.size"
  val PURGE_EXISTING_MULTIPART = "fs.s3a.multipart.purge"
  val PURGE_EXISTING_MULTIPART_AGE = "fs.s3a.multipart.purge.age"
  val SERVER_SIDE_ENCRYPTION_ALGORITHM = "fs.s3a.server-side-encryption-algorithm"
  val SERVER_SIDE_ENCRYPTION_AES256 = "AES256"
  val SIGNING_ALGORITHM = "fs.s3a.signing-algorithm"
  val FS_S3A_BLOCK_SIZE = "fs.s3a.block.size"
  val FS_S3A = "s3a"
  val USER_AGENT_PREFIX = "fs.s3a.user.agent.prefix"
  val READAHEAD_RANGE = "fs.s3a.readahead.range"
}
