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

import javax.annotation.Nullable;

/**
 * Builder class that provides handy methods to construct {@link CouchbaseSourceConfig} for testing.
 */
public class CouchbaseSourceConfigBuilder {

  protected String referenceName;
  protected String nodes;
  protected String bucket;
  protected String selectFields;
  protected String conditions;
  protected String user;
  protected String password;
  protected String onError;
  protected String schema;
  protected Integer maxParallelism;
  protected String scanConsistency;
  protected Integer timeout;

  public static CouchbaseSourceConfigBuilder builder() {
    return new CouchbaseSourceConfigBuilder();
  }

  public static CouchbaseSourceConfigBuilder builder(CouchbaseSourceConfig original) {
    return builder()
      .setReferenceName(original.getReferenceName())
      .setNodes(original.getNodes())
      .setBucket(original.getBucket())
      .setSelectFields(original.getSelectFields())
      .setConditions(original.getConditions())
      .setUser(original.getUser())
      .setPassword(original.getPassword())
      .setOnError(original.getOnError())
      .setMaxParallelism(original.getMaxParallelism())
      .setScanConsistency(original.getConsistency())
      .setQueryTimeout(original.getTimeout())
      .setSchema(original.getSchema());
  }

  public CouchbaseSourceConfigBuilder setReferenceName(String referenceName) {
    this.referenceName = referenceName;
    return this;
  }

  public CouchbaseSourceConfigBuilder setNodes(String nodes) {
    this.nodes = nodes;
    return this;
  }

  public CouchbaseSourceConfigBuilder setBucket(String bucket) {
    this.bucket = bucket;
    return this;
  }

  public CouchbaseSourceConfigBuilder setSelectFields(String selectFields) {
    this.selectFields = selectFields;
    return this;
  }

  public CouchbaseSourceConfigBuilder setConditions(@Nullable String conditions) {
    this.conditions = conditions;
    return this;
  }

  public CouchbaseSourceConfigBuilder setUser(@Nullable String user) {
    this.user = user;
    return this;
  }

  public CouchbaseSourceConfigBuilder setPassword(@Nullable String password) {
    this.password = password;
    return this;
  }

  public CouchbaseSourceConfigBuilder setOnError(String onError) {
    this.onError = onError;
    return this;
  }

  public CouchbaseSourceConfigBuilder setMaxParallelism(Integer maxParallelism) {
    this.maxParallelism = maxParallelism;
    return this;
  }

  public CouchbaseSourceConfigBuilder setScanConsistency(String scanConsistency) {
    this.scanConsistency = scanConsistency;
    return this;
  }

  public CouchbaseSourceConfigBuilder setQueryTimeout(Integer timeout) {
    this.timeout = timeout;
    return this;
  }

  public CouchbaseSourceConfigBuilder setSchema(String schema) {
    this.schema = schema;
    return this;
  }

  public CouchbaseSourceConfig build() {
    return new CouchbaseSourceConfig(referenceName, nodes, bucket, selectFields, conditions, user, password, onError,
                                     schema, maxParallelism, scanConsistency, timeout);
  }
}
