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

import io.cdap.plugin.couchbase.CouchbaseConfigBuilder;

/**
 * Builder class that provides handy methods to construct {@link CouchbaseActionConfig} for testing.
 */
public class CouchbaseActionConfigBuilder extends CouchbaseConfigBuilder<CouchbaseActionConfigBuilder> {

  private String query;
  private int maxParallelism;
  private String scanConsistency;
  private int timeout;

  public static CouchbaseActionConfigBuilder builder() {
    return new CouchbaseActionConfigBuilder();
  }

  public static CouchbaseActionConfigBuilder builder(CouchbaseActionConfig original) {
    return builder()
      .setReferenceName(original.getReferenceName())
      .setNodes(original.getNodes())
      .setBucket(original.getBucket())
      .setQuery(original.getQuery())
      .setUser(original.getUser())
      .setPassword(original.getPassword())
      .setMaxParallelism(original.getMaxParallelism())
      .setScanConsistency(original.getConsistency())
      .setQueryTimeout(original.getTimeout());
  }

  public CouchbaseActionConfigBuilder setQuery(String query) {
    this.query = query;
    return this;
  }

  public CouchbaseActionConfigBuilder setMaxParallelism(int maxParallelism) {
    this.maxParallelism = maxParallelism;
    return this;
  }

  public CouchbaseActionConfigBuilder setScanConsistency(String scanConsistency) {
    this.scanConsistency = scanConsistency;
    return this;
  }

  public CouchbaseActionConfigBuilder setQueryTimeout(int timeout) {
    this.timeout = timeout;
    return this;
  }

  public CouchbaseActionConfig build() {
    return new CouchbaseActionConfig(referenceName, nodes, bucket, user, password, query, maxParallelism,
                                     scanConsistency, timeout);
  }
}
