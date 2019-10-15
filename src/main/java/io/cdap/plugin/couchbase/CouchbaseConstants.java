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
 * Couchbase constants.
 */
public class CouchbaseConstants {

  private CouchbaseConstants() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  /**
   * Couchbase plugin name.
   */
  public static final String PLUGIN_NAME = "Couchbase";

  /**
   * Configuration property name used to specify list of nodes to use when connecting to the Couchbase cluster.
   */
  public static final String NODES = "nodes";

  /**
   * Configuration property name used to specify Couchbase bucket name.
   */
  public static final String BUCKET = "bucket";

  /**
   * Configuration property name used to specify N1QL query to use to import data from the specified bucket.
   */
  public static final String QUERY = "query";

  /**
   * Configuration property name used to specify user identity for connecting to the Couchbase.
   */
  public static final String USERNAME = "username";

  /**
   * Configuration property name used to specify password to use to connect to the Couchbase.
   */
  public static final String PASSWORD = "password";

  /**
   * Configuration property name used to specify the schema of the entries.
   */
  public static final String SCHEMA = "schema";

  /**
   * Configuration property name used to specify how to handle error in record processing. An error will be thrown if
   * failed to parse value according to a provided schema.
   */
  public static final String ON_ERROR = "on-error";

  /**
   * Configuration property name used to specify maximum number of CPU cores can be used to process a query.
   * If the specified value is less than zero or greater than the total number of cores in a cluster, the system will
   * use all available cores in the cluster.
   */
  public static final String MAX_PARALLELISM = "maxParallelism";

  /**
   * Configuration property name used to specify the consistency guarantee or constraint for index scanning.
   */
  public static final String SCAN_CONSISTENCY = "consistency";

  /**
   * Configuration property name used to specify number of seconds to wait before a timeout has occurred on a query.
   */
  public static final String QUERY_TIMEOUT = "timeout";
}
