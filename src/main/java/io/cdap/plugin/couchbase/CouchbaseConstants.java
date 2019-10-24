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
   * Configuration property name used to specify comma-separated list of fields to be read.
   */
  public static final String SELECT_FIELDS = "selectFields";

  /**
   * Configuration property name used to specify optional criteria (filters or predicates) that the result documents
   * must satisfy. Corresponds to the WHERE clause in N1QL SELECT statement.
   */
  public static final String CONDITIONS = "conditions";

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
   * Configuration property name used to specify maximum number of CPU cores to be used to process a query.
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

  /**
   * Configuration property name used to specify which of the incoming fields should be used as a document identifier.
   * Identifier is expected to be of type string.
   */
  public static final String KEY_FIELD = "keyField";

  /**
   * Configuration property name used to specify type of write operation to perform.
   * This can be set to Insert, Replace or Upsert.
   */
  public static final String OPERATION = "operation";

  /**
   * Configuration property name used to specify size (in number of records) of the batched writes to the Couchbase
   * bucket. Each write to Couchbase contains some overhead. To maximize bulk write throughput, maximize the amount of
   * data stored per write. Commits of 1 MiB usually provide the best performance. Default value is 100 records.
   */
  public static final String BATCH_SIZE = "batchSize";

  /**
   * Configuration property name used to specify the number of documents to randomly sample in the bucket when
   * inferring the schema. The default sample size is 1000 documents. If a bucket contains fewer documents than the
   * specified number, then all the documents in the bucket will be used.
   */
  public static final String SAMPLE_SIZE = "sampleSize";

  /**
   * Configuration property name used to specify the desired number of splits to divide the query into when reading
   * from Couchbase. Fewer splits may be created if the query cannot be divided into the desired number of splits. If
   * the specified value is zero, the plugin will use the number of map tasks as the number of splits.
   */
  public static final String NUM_SPLITS = "numSplits";

  /**
   * Configuration property name used to specify N1QL query to run.
   */
  public static final String QUERY = "query";
}
