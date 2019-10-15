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
 * Builder class that provides handy methods to construct {@link CouchbaseConfig} for testing.
 */
public class CouchbaseConfigBuilder {

  protected String referenceName;
  protected String nodes;
  protected String bucket;
  protected String query;
  protected String user;
  protected String password;
  protected String onError;
  protected String schema;
  protected Integer maxParallelism;
  protected String scanConsistency;
  protected Integer timeout;

  public static CouchbaseConfigBuilder builder() {
    return new CouchbaseConfigBuilder();
  }

  public static CouchbaseConfigBuilder builder(CouchbaseConfig original) {
    return builder()
      .setReferenceName(original.getReferenceName())
      .setNodes(original.getNodes())
      .setBucket(original.getBucket())
      .setQuery(original.getQuery())
      .setUser(original.getUser())
      .setPassword(original.getPassword())
      .setOnError(original.getErrorHandling() == null ? null : original.getErrorHandling().getDisplayName())
      .setMaxParallelism(original.getMaxParallelism())
      .setScanConsistency(original.getScanConsistency() == null ? null : original.getScanConsistency().getDisplayName())
      .setQueryTimeout(original.getTimeout())
      .setSchema(original.getSchema());
  }

  public CouchbaseConfigBuilder setReferenceName(String referenceName) {
    this.referenceName = referenceName;
    return this;
  }

  public CouchbaseConfigBuilder setNodes(String nodes) {
    this.nodes = nodes;
    return this;
  }

  public CouchbaseConfigBuilder setBucket(String bucket) {
    this.bucket = bucket;
    return this;
  }

  public CouchbaseConfigBuilder setQuery(String query) {
    this.query = query;
    return this;
  }

  public CouchbaseConfigBuilder setUser(String user) {
    this.user = user;
    return this;
  }

  public CouchbaseConfigBuilder setPassword(String password) {
    this.password = password;
    return this;
  }

  public CouchbaseConfigBuilder setOnError(String onError) {
    this.onError = onError;
    return this;
  }

  public CouchbaseConfigBuilder setMaxParallelism(Integer maxParallelism) {
    this.maxParallelism = maxParallelism;
    return this;
  }

  public CouchbaseConfigBuilder setScanConsistency(String scanConsistency) {
    this.scanConsistency = scanConsistency;
    return this;
  }

  public CouchbaseConfigBuilder setQueryTimeout(Integer timeout) {
    this.timeout = timeout;
    return this;
  }

  public CouchbaseConfigBuilder setSchema(String schema) {
    this.schema = schema;
    return this;
  }

  public CouchbaseConfig build() {
    return new CouchbaseConfig(referenceName, nodes, bucket, query, user, password, onError, schema, maxParallelism,
                               scanConsistency, timeout);
  }
}
