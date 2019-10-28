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
 * Couchbase util methods.
 */
public class CouchbaseUtil {

  private CouchbaseUtil() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  /**
   * Converts specified Couchbase property name to valid CDAP field name by replacing any character that are not
   * one of [A-Z][a-z][0-9] or _ with an underscore (_).
   *
   * @param propertyName Couchbase property name, such as "prop name".
   * @return valid CDAP field name such as "prop_name", corresponding to the specified property name.
   */
  public static String fieldName(String propertyName) {
    return propertyName.toLowerCase().replaceAll("[^A-Za-z0-9]", "_");
  }
}
