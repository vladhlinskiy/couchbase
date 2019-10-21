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
package io.cdap.plugin.couchbase.sink;

import io.cdap.plugin.couchbase.CouchbaseConfigBuilder;

/**
 * Builder class that provides handy methods to construct {@link CouchbaseSink.CouchbaseSinkConfig} for testing.
 */
public class CouchbaseSinkConfigBuilder extends CouchbaseConfigBuilder<CouchbaseSinkConfigBuilder> {

  private String keyField;
  private String operation;
  private int batchSize;

  public static CouchbaseSinkConfigBuilder builder() {
    return new CouchbaseSinkConfigBuilder();
  }

  public static CouchbaseSinkConfigBuilder builder(CouchbaseSink.CouchbaseSinkConfig original) {
    return builder()
      .setReferenceName(original.getReferenceName())
      .setNodes(original.getNodes())
      .setBucket(original.getBucket())
      .setKeyField(original.getKeyField())
      .setOperation(original.getOperation())
      .setUser(original.getUser())
      .setPassword(original.getPassword())
      .setBatchSize(original.getBatchSize());
  }

  public CouchbaseSinkConfigBuilder setKeyField(String keyField) {
    this.keyField = keyField;
    return this;
  }

  public CouchbaseSinkConfigBuilder setOperation(String operation) {
    this.operation = operation;
    return this;
  }

  public CouchbaseSinkConfigBuilder setBatchSize(int batchSize) {
    this.batchSize = batchSize;
    return this;
  }

  public CouchbaseSink.CouchbaseSinkConfig build() {
    return new CouchbaseSink.CouchbaseSinkConfig(referenceName, nodes, bucket, user, password, keyField, operation,
                                                 batchSize);
  }
}
