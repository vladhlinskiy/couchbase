/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.couchbase.exception;

/**
 * Runtime Couchbase execution exception thrown when there were errors during execution of Couchbase operations or
 * commands.
 */
public class CouchbaseExecutionException extends RuntimeException {

  public CouchbaseExecutionException(String message) {
    super(message);
  }

  public CouchbaseExecutionException(String message, Throwable cause) {
    super(message, cause);
  }
}
