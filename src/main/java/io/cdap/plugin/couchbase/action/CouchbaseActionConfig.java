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

package io.cdap.plugin.couchbase.action;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.couchbase.Consistency;
import io.cdap.plugin.couchbase.CouchbaseConfig;
import io.cdap.plugin.couchbase.CouchbaseConstants;
import io.cdap.plugin.couchbase.sink.CouchbaseSink;

import java.util.Objects;

/**
 * Config class for {@link CouchbaseSink}.
 */
public class CouchbaseActionConfig extends CouchbaseConfig {

  @Name(CouchbaseConstants.QUERY)
  @Description("N1QL query to run.")
  @Macro
  private String query;

  @Name(CouchbaseConstants.MAX_PARALLELISM)
  @Description("Maximum number of CPU cores to be used to process a query. If the specified value is less " +
    "than zero or greater than the total number of cores in a cluster, the system will use all available cores in " +
    "the cluster.")
  @Macro
  private int maxParallelism;

  @Name(CouchbaseConstants.SCAN_CONSISTENCY)
  @Description("Specifies the consistency guarantee or constraint for index scanning.")
  @Macro
  private String consistency;

  @Name(CouchbaseConstants.QUERY_TIMEOUT)
  @Description("Number of seconds to wait before a timeout has occurred on a query.")
  @Macro
  private int timeout;

  public CouchbaseActionConfig(String referenceName, String nodes, String bucket, String user, String password,
                               String query, int maxParallelism, String consistency, int timeout) {
    super(referenceName, nodes, bucket, user, password);
    this.query = query;
    this.maxParallelism = maxParallelism;
    this.consistency = consistency;
    this.timeout = timeout;
  }

  public String getQuery() {
    return query;
  }

  public int getMaxParallelism() {
    return maxParallelism;
  }

  public String getConsistency() {
    return consistency;
  }

  public int getTimeout() {
    return timeout;
  }

  public Consistency getScanConsistency() {
    return Objects.requireNonNull(Consistency.fromDisplayName(consistency));
  }

  /**
   * Validates {@link CouchbaseActionConfig} instance.
   *
   * @param collector failure collector.
   */
  public void validate(FailureCollector collector) {
    super.validate(collector);
    if (!containsMacro(CouchbaseConstants.QUERY) && Strings.isNullOrEmpty(query)) {
      collector.addFailure("Query must be specified", null)
        .withConfigProperty(CouchbaseConstants.QUERY);
    }
    if (!containsMacro(CouchbaseConstants.SCAN_CONSISTENCY)) {
      if (Strings.isNullOrEmpty(consistency)) {
        collector.addFailure("Scan consistency must be specified", null)
          .withConfigProperty(CouchbaseConstants.SCAN_CONSISTENCY);
      } else if (Consistency.fromDisplayName(consistency) == null) {
        collector.addFailure("Invalid scan consistency name", null)
          .withConfigProperty(CouchbaseConstants.SCAN_CONSISTENCY);
      }
    }
    if (!containsMacro(CouchbaseConstants.QUERY_TIMEOUT)) {
      if (timeout < 1) {
        collector.addFailure("Query timeout must be greater than 0", null)
          .withConfigProperty(CouchbaseConstants.QUERY_TIMEOUT);
      }
    }
  }
}
