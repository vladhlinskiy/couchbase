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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.couchbase.CouchbaseConfig;
import io.cdap.plugin.couchbase.CouchbaseConstants;
import io.cdap.plugin.couchbase.OperationType;

import java.util.Set;

/**
 * Config class for {@link CouchbaseSink}.
 */
public class CouchbaseSinkConfig extends CouchbaseConfig {

  private static final Set<Schema.Type> SUPPORTED_TYPES = ImmutableSet.of(Schema.Type.ARRAY, Schema.Type.BOOLEAN,
                                                                          Schema.Type.BYTES, Schema.Type.STRING,
                                                                          Schema.Type.DOUBLE, Schema.Type.FLOAT,
                                                                          Schema.Type.INT, Schema.Type.LONG,
                                                                          Schema.Type.RECORD, Schema.Type.ENUM,
                                                                          Schema.Type.MAP, Schema.Type.UNION);

  private static final Set<Schema.LogicalType> SUPPORTED_LOGICAL_TYPES = ImmutableSet.of(
    Schema.LogicalType.DATE, Schema.LogicalType.DECIMAL, Schema.LogicalType.TIME_MILLIS,
    Schema.LogicalType.TIME_MICROS, Schema.LogicalType.TIMESTAMP_MILLIS, Schema.LogicalType.TIMESTAMP_MICROS);

  @Name(CouchbaseConstants.KEY_FIELD)
  @Description("Allows to specify which of the incoming fields should be used as an document identifier.")
  @Macro
  private String keyField;

  @Name(CouchbaseConstants.OPERATION)
  @Description("Type of write operation to perform. This can be set to Insert, Replace or Upsert.")
  @Macro
  private String operation;

  @Name(CouchbaseConstants.BATCH_SIZE)
  @Description("Size (in number of records) of the batched writes to the Couchbase bucket. Each write to Couchbase " +
    "contains some overhead. To maximize bulk write throughput, maximize the amount of data stored per write. " +
    "Commits of 1 MiB usually provide the best performance. Default value is 100 records.")
  @Macro
  private int batchSize;

  public CouchbaseSinkConfig(String referenceName, String nodes, String bucket, String user, String password,
                             String keyField, String operation, int batchSize) {
    super(referenceName, nodes, bucket, user, password);
    this.keyField = keyField;
    this.operation = operation;
    this.batchSize = batchSize;
  }

  public String getKeyField() {
    return keyField;
  }

  public String getOperation() {
    return operation;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public OperationType getOperationType() {
    return OperationType.valueOf(operation);
  }

  /**
   * Validates {@link CouchbaseSinkConfig} instance.
   *
   * @param collector failure collector.
   */
  public void validate(FailureCollector collector) {
    super.validate(collector);
    if (!containsMacro(CouchbaseConstants.KEY_FIELD) && Strings.isNullOrEmpty(keyField)) {
      collector.addFailure("Key field name must be specified", null)
        .withConfigProperty(CouchbaseConstants.KEY_FIELD);
    }
    if (!containsMacro(CouchbaseConstants.BATCH_SIZE)) {
      if (batchSize < 1) {
        collector.addFailure("Batch size must be greater than 0", null)
          .withConfigProperty(CouchbaseConstants.BATCH_SIZE);
      }
    }
    if (!containsMacro(CouchbaseConstants.OPERATION)) {
      if (Strings.isNullOrEmpty(operation)) {
        collector.addFailure("Operation type must be specified", null)
          .withConfigProperty(CouchbaseConstants.OPERATION);
      } else {
        try {
          OperationType.valueOf(operation);
        } catch (IllegalArgumentException e) {
          collector.addFailure("Invalid operation type name", null)
            .withConfigProperty(CouchbaseConstants.OPERATION);
        }
      }
    }
  }

  public void validateSchema(Schema schema, FailureCollector collector) {
    super.validateSchema(schema, SUPPORTED_LOGICAL_TYPES, SUPPORTED_TYPES, collector);
    if (!containsMacro(CouchbaseConstants.KEY_FIELD) && !Strings.isNullOrEmpty(keyField)) {
      if (schema.getField(keyField) == null) {
        collector.addFailure(String.format("Schema does not contain identifier field '%s'.", keyField), null)
          .withConfigProperty(CouchbaseConstants.KEY_FIELD);
      }
    }
  }
}
