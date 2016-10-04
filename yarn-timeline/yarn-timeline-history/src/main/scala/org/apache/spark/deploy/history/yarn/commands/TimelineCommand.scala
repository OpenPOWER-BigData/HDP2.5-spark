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

package org.apache.spark.deploy.history.yarn.commands

import java.io.FileNotFoundException
import java.util.concurrent.TimeUnit

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.util.Tool

import org.apache.spark.Logging
import org.apache.spark.deploy.history.yarn.rest.UnauthorizedRequestException

private[spark] abstract class TimelineCommand extends Configured with Tool with Logging {

  import org.apache.spark.deploy.history.yarn.commands.TimelineCommand._

  /**
   * Run the command.
   *
   * xit codes:
   * <pre>
   *  0: success
   * 44: "not found": the endpoint or a named
   * 41: "unauthed": caller was not authenticated
   * -1: any other failure
   * </pre>
   * @param args command line
   * @return exit code
   */
  override def run(args: Array[String]): Int = {
    if (UserGroupInformation.isSecurityEnabled) {
      logInfo(s"Logging in to secure cluster as ${UserGroupInformation.getCurrentUser}")
    }
    try {
      exec(args)
    } catch {

      case notFound: FileNotFoundException =>
        E_NOT_FOUND

      case ure: UnauthorizedRequestException =>
        logError(s"Authentication Failure $ure", ure)
        E_UNAUTH

      case e: CommandException =>
        logError(s"$e")
        logDebug(s"exit code ${e.exitCode}", e)
        e.exitCode

      case e: Exception =>
        logError(s"Failed: $e", e)
        E_ERROR
    }
  }

  /**
   * Execute the operation.
   * @param args list of arguments
   * @return the exit code.
   */
  def exec(args: Seq[String]): Int = {
    0

  }

  /**
   * Get a time configuration in milliseconds
   * @param key configuration key
   * @param defVal default value
   * @return the time in millseconds
   */
  def millis(key: String, defVal: Long): Long = {
    getConf.getTimeDuration(key, defVal, TimeUnit.MILLISECONDS)
  }

  /**
   * Get a positive integer operation
   * @param key configuration key
   * @param defVal default value
   * @return the value.
   */
  def intOption(key: String, defVal: Int): Int = {
    val v = getConf.getInt(key, defVal)
    require(v > 0, s"Option $key out of range: $v")
    v
  }
}

private[spark] object TimelineCommand {

  val E_SUCCESS = 0
  val E_NOT_FOUND = 44
  val E_UNAUTH = 1
  val E_USAGE = 2
  val E_ERROR = -1

}

/**
 * An exception that can be raised to provide an exit code and a message.
 * The stack trace in here will only be logged at debug level
 * @param exitCode exit code
 * @param message message
 * @param thrown any exception to contain
 */
class CommandException(val exitCode: Int, message: String, thrown: Throwable = null)
    extends Exception(message, thrown) {
}
