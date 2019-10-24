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
package io.cdap.plugin.couchbase.source;

import io.cdap.plugin.couchbase.CouchbaseConfigBuilder;

import javax.annotation.Nullable;

/**
 * Builder class that provides handy methods to construct {@link CouchbaseSourceConfig} for testing.
 */
public class CouchbaseSourceConfigBuilder extends CouchbaseConfigBuilder<CouchbaseSourceConfigBuilder> {

  private String selectFields;
  private String onError;
  private String schema;
  private int maxParallelism;
  private String scanConsistency;
  private int timeout;

  @Nullable
  private String conditions;

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

  public CouchbaseSourceConfigBuilder setSelectFields(String selectFields) {
    this.selectFields = selectFields;
    return this;
  }

  public CouchbaseSourceConfigBuilder setConditions(@Nullable String conditions) {
    this.conditions = conditions;
    return this;
  }

  public CouchbaseSourceConfigBuilder setOnError(String onError) {
    this.onError = onError;
    return this;
  }

  public CouchbaseSourceConfigBuilder setMaxParallelism(int maxParallelism) {
    this.maxParallelism = maxParallelism;
    return this;
  }

  public CouchbaseSourceConfigBuilder setScanConsistency(String scanConsistency) {
    this.scanConsistency = scanConsistency;
    return this;
  }

  public CouchbaseSourceConfigBuilder setQueryTimeout(int timeout) {
    this.timeout = timeout;
    return this;
  }

  public CouchbaseSourceConfigBuilder setSchema(String schema) {
    this.schema = schema;
    return this;
  }

  public CouchbaseSourceConfig build() {
    return new CouchbaseSourceConfig(referenceName, nodes, bucket, user, password, selectFields, conditions,
                                     onError, schema, maxParallelism, scanConsistency, timeout);
  }
}
