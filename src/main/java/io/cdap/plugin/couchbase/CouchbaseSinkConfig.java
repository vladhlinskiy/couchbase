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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.couchbase.sink.CouchbaseSink;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * TODO avoid duplication
 * Defines a {@link PluginConfig} that {@link CouchbaseSink} can use.
 */
public class CouchbaseSinkConfig extends PluginConfig {

  private static final Set<Schema.Type> SUPPORTED_SIMPLE_TYPES = ImmutableSet.of(Schema.Type.ARRAY, Schema.Type.BOOLEAN,
                                                                                 Schema.Type.BYTES, Schema.Type.STRING,
                                                                                 Schema.Type.DOUBLE, Schema.Type.FLOAT,
                                                                                 Schema.Type.INT, Schema.Type.LONG,
                                                                                 Schema.Type.RECORD, Schema.Type.ENUM,
                                                                                 Schema.Type.MAP, Schema.Type.UNION);

  private static final Set<Schema.LogicalType> SUPPORTED_LOGICAL_TYPES = ImmutableSet.of(
    Schema.LogicalType.DATE, Schema.LogicalType.DECIMAL, Schema.LogicalType.TIME_MILLIS, Schema.LogicalType.TIME_MICROS,
    Schema.LogicalType.TIMESTAMP_MILLIS, Schema.LogicalType.TIMESTAMP_MICROS);

  @Name(Constants.Reference.REFERENCE_NAME)
  @Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
  private String referenceName;

  @Name(CouchbaseConstants.NODES)
  @Description("List of nodes to use when connecting to the Couchbase cluster..")
  @Macro
  private String nodes;

  @Name(CouchbaseConstants.BUCKET)
  @Description("Couchbase bucket name.")
  @Macro
  private String bucket;

  @Name(CouchbaseConstants.KEY_FIELD)
  @Description("Allows to specify which of the incoming fields should be used as an document identifier.")
  @Macro
  private String keyField;

  @Name(CouchbaseConstants.OPERATION)
  @Description("Type of write operation to perform. This can be set to Insert, Replace or Upsert.")
  @Macro
  private String operation;

  @Name(CouchbaseConstants.USERNAME)
  @Description("User identity for connecting to the Couchbase.")
  @Macro
  @Nullable
  private String user;

  @Name(CouchbaseConstants.PASSWORD)
  @Description("Password to use to connect to the Couchbase.")
  @Macro
  @Nullable
  private String password;

  @Name(CouchbaseConstants.BATCH_SIZE)
  @Description("Size (in number of records) of the batched writes to the Couchbase bucket. Each write to Couchbase " +
    "contains some overhead. To maximize bulk write throughput, maximize the amount of data stored per write. " +
    "Commits of 1 MiB usually provide the best performance. Default value is 100 records.")
  @Macro
  private Integer batchSize;

  public CouchbaseSinkConfig(String referenceName, String nodes, String bucket, String keyField, String operation,
                             String user, String password, Integer batchSize) {
    this.referenceName = referenceName;
    this.nodes = nodes;
    this.bucket = bucket;
    this.keyField = keyField;
    this.operation = operation;
    this.user = user;
    this.password = password;
    this.batchSize = batchSize;
  }

  public String getReferenceName() {
    return referenceName;
  }

  public String getNodes() {
    return nodes;
  }

  public String getBucket() {
    return bucket;
  }

  public String getKeyField() {
    return keyField;
  }

  public String getOperation() {
    return operation;
  }

  @Nullable
  public String getUser() {
    return user;
  }

  @Nullable
  public String getPassword() {
    return password;
  }

  public Integer getBatchSize() {
    return batchSize;
  }

  public List<String> getNodeList() {
    return Arrays.asList(getNodes().split(","));
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
    if (Strings.isNullOrEmpty(referenceName)) {
      collector.addFailure("Reference name must be specified", null)
        .withConfigProperty(Constants.Reference.REFERENCE_NAME);
    } else {
      try {
        IdUtils.validateId(referenceName);
      } catch (IllegalArgumentException e) {
        collector.addFailure("Invalid reference name", "Change the reference name to only " +
          "include letters, numbers, periods, underscores, or dashes.")
          .withConfigProperty(Constants.Reference.REFERENCE_NAME);
      }
    }
    if (!containsMacro(CouchbaseConstants.NODES) && Strings.isNullOrEmpty(nodes)) {
      collector.addFailure("Couchbase nodes must be specified", null)
        .withConfigProperty(CouchbaseConstants.NODES);
    }
    if (!containsMacro(CouchbaseConstants.BUCKET) && Strings.isNullOrEmpty(bucket)) {
      collector.addFailure("Bucket name must be specified", null)
        .withConfigProperty(CouchbaseConstants.BUCKET);
    }
    if (!containsMacro(CouchbaseConstants.KEY_FIELD) && Strings.isNullOrEmpty(keyField)) {
      collector.addFailure("Key field name must be specified", null)
        .withConfigProperty(CouchbaseConstants.KEY_FIELD);
    }
    if (!containsMacro(CouchbaseConstants.USERNAME) && !containsMacro(CouchbaseConstants.PASSWORD) &&
      Strings.isNullOrEmpty(user) && !Strings.isNullOrEmpty(password)) {
      collector.addFailure("Username must be specified", null)
        .withConfigProperty(CouchbaseConstants.USERNAME);
    }
    if (!containsMacro(CouchbaseConstants.USERNAME) && !containsMacro(CouchbaseConstants.PASSWORD) &&
      !Strings.isNullOrEmpty(user) && Strings.isNullOrEmpty(password)) {
      collector.addFailure("Password must be specified", null)
        .withConfigProperty(CouchbaseConstants.PASSWORD);
    }
  }

  public void validateSchema(Schema schema, FailureCollector collector) {
    if (schema == null) {
      collector.addFailure("Schema must be specified", null)
        .withConfigProperty(CouchbaseConstants.SCHEMA);
      return;
    }
    List<Schema.Field> fields = schema.getFields();
    if (fields == null || fields.isEmpty()) {
      collector.addFailure("Schema must contain at least one field", null)
        .withConfigProperty(CouchbaseConstants.SCHEMA);
      return;
    }
    for (Schema.Field field : fields) {
      validateFieldSchema(field.getName(), field.getSchema(), collector);
    }
    if (!containsMacro(CouchbaseConstants.KEY_FIELD) && !Strings.isNullOrEmpty(keyField)) {
      if (schema.getField(keyField) == null) {
        collector.addFailure(String.format("Schema does not contain key field '%s'", keyField), null)
          .withConfigProperty(CouchbaseConstants.SCHEMA)
          .withConfigProperty(CouchbaseConstants.KEY_FIELD);
      }
    }
  }

  private void validateFieldSchema(String fieldName, Schema schema, FailureCollector collector) {
    Schema nonNullableSchema = schema.isNullable() ? schema.getNonNullable() : schema;
    Schema.Type type = nonNullableSchema.getType();
    switch (type) {
      case RECORD:
        validateSchema(nonNullableSchema, collector);
        break;
      case ARRAY:
        validateArraySchema(fieldName, nonNullableSchema, collector);
        break;
      case MAP:
        validateMapSchema(fieldName, nonNullableSchema, collector);
        break;
      default:
        validateSchemaType(fieldName, nonNullableSchema, collector);
    }
  }

  private void validateMapSchema(String fieldName, Schema schema, FailureCollector collector) {
    Schema keySchema = schema.getMapSchema().getKey();
    if (keySchema.isNullable() || keySchema.getType() != Schema.Type.STRING) {
      collector.addFailure("Map keys must be a non-nullable string",
                           String.format("Change field '%s' to be a non-nullable string", fieldName))
        .withOutputSchemaField(fieldName, null);
    }
    validateFieldSchema(fieldName, schema.getMapSchema().getValue(), collector);
  }

  private void validateArraySchema(String fieldName, Schema schema, FailureCollector collector) {
    Schema componentSchema = schema.getComponentSchema().isNullable() ? schema.getComponentSchema().getNonNullable()
      : schema.getComponentSchema();
    validateFieldSchema(fieldName, componentSchema, collector);
  }

  private void validateSchemaType(String fieldName, Schema fieldSchema, FailureCollector collector) {
    Schema.Type type = fieldSchema.getType();
    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (SUPPORTED_SIMPLE_TYPES.contains(type) || SUPPORTED_LOGICAL_TYPES.contains(logicalType)) {
      return;
    }

    String supportedTypeNames = Stream.concat(
      SUPPORTED_SIMPLE_TYPES.stream().map(Enum::name).map(String::toLowerCase),
      SUPPORTED_LOGICAL_TYPES.stream().map(Schema.LogicalType::getToken)
    ).collect(Collectors.joining(", "));

    String errorMessage = String.format("Field '%s' is of unsupported type '%s'. Supported types are: %s",
                                        fieldName, fieldSchema.getDisplayName(), supportedTypeNames);
    collector.addFailure(errorMessage, String.format("Change field '%s' to be a supported type", fieldName))
      .withOutputSchemaField(fieldName, null);
  }
}
