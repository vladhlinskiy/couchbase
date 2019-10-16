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

package io.cdap.plugin.couchbase;

/**
 * Type of write operation to perform. This can be set to Insert, Replace or Upsert.
 */
public enum OperationType {

  /**
   * Insert will only create the document if the given ID is not found within the database.
   */
  INSERT,

  /**
   * Replace will only replace the document if the given ID already exists within the database.
   */
  REPLACE,

  /**
   * Upsert will always replace the document, ignoring whether the ID has already existed or not.
   */
  UPSERT;
}
