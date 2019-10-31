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
import io.cdap.plugin.couchbase.source.CouchbaseSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Defines a {@link PluginConfig} that {@link CouchbaseSource} can use.
 */
public class CouchbaseSourceConfig extends PluginConfig {

  public static final Schema ERROR_SCHEMA = Schema.recordOf("error", Schema.Field.of("document",
                                                                                     Schema.of(Schema.Type.STRING)));

  private static final Set<Schema.Type> SUPPORTED_SIMPLE_TYPES = ImmutableSet.of(Schema.Type.BOOLEAN, Schema.Type.INT,
                                                                                 Schema.Type.DOUBLE, Schema.Type.LONG,
                                                                                 Schema.Type.STRING, Schema.Type.RECORD,
                                                                                 Schema.Type.ARRAY, Schema.Type.MAP);

  private static final Set<Schema.LogicalType> SUPPORTED_LOGICAL_TYPES = ImmutableSet.of(Schema.LogicalType.DECIMAL);

  @Name(Constants.Reference.REFERENCE_NAME)
  @Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
  private String referenceName;

  @Name(CouchbaseConstants.NODES)
  @Description("List of nodes to use when connecting to the Couchbase cluster.")
  @Macro
  private String nodes;

  @Name(CouchbaseConstants.BUCKET)
  @Description("Couchbase bucket name.")
  @Macro
  private String bucket;

  @Name(CouchbaseConstants.SELECT_FIELDS)
  @Description("Comma-separated list of fields to be read.")
  @Macro
  private String selectFields;

  @Name(CouchbaseConstants.CONDITIONS)
  @Description("Optional criteria (filters or predicates) that the result documents must satisfy. Corresponds to the " +
    "WHERE clause in N1QL SELECT statement.")
  @Macro
  @Nullable
  private String conditions;

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

  @Name(CouchbaseConstants.ON_ERROR)
  @Description("Specifies how to handle error in record processing. Error will be thrown if failed to parse value " +
    "according to provided schema.")
  @Macro
  private String onError;

  @Name(CouchbaseConstants.SCHEMA)
  @Description("Schema of records output by the source.")
  private String schema;

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

  public CouchbaseSourceConfig(String referenceName, String nodes, String bucket, String selectFields,
                               String conditions, String user, String password, String onError, String schema,
                               int maxParallelism, String consistency, int timeout) {
    this.referenceName = referenceName;
    this.nodes = nodes;
    this.bucket = bucket;
    this.selectFields = selectFields;
    this.conditions = conditions;
    this.user = user;
    this.password = password;
    this.onError = onError;
    this.schema = schema;
    this.maxParallelism = maxParallelism;
    this.consistency = consistency;
    this.timeout = timeout;
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

  public String getSelectFields() {
    return selectFields;
  }

  @Nullable
  public String getConditions() {
    return conditions;
  }

  @Nullable
  public String getUser() {
    return user;
  }

  @Nullable
  public String getPassword() {
    return password;
  }

  public String getOnError() {
    return onError;
  }

  public String getSchema() {
    return schema;
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

  /**
   * Parses the json representation into a schema object.
   *
   * @return parsed schema object of json representation.
   * @throws RuntimeException if there was an exception parsing the schema.
   */
  @Nullable
  public Schema getParsedSchema() {
    try {
      return schema == null ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      // this should not happen, since schema string comes from UI
      throw new IllegalStateException(String.format("Could not parse schema string: '%s'", schema), e);
    }
  }

  public List<String> getNodeList() {
    return Arrays.asList(getNodes().split(","));
  }

  public List<String> getSelectFieldsList() {
    return Arrays.asList(getSelectFields().split(","));
  }

  public Consistency getScanConsistency() {
    return Objects.requireNonNull(Consistency.fromDisplayName(consistency));
  }

  public ErrorHandling getErrorHandling() {
    return Objects.requireNonNull(ErrorHandling.fromDisplayName(onError));
  }

  /**
   * Validates {@link CouchbaseSourceConfig} instance.
   *
   * @param collector failure collector.
   */
  public void validate(FailureCollector collector) {
    if (Strings.isNullOrEmpty(referenceName)) {
      collector.addFailure("Reference name must be specified", null)
        .withConfigProperty(Constants.Reference.REFERENCE_NAME);
    } else {
      IdUtils.validateReferenceName(referenceName, collector);
    }
    if (!containsMacro(CouchbaseConstants.NODES) && Strings.isNullOrEmpty(nodes)) {
      collector.addFailure("Couchbase nodes must be specified", null)
        .withConfigProperty(CouchbaseConstants.NODES);
    }
    if (!containsMacro(CouchbaseConstants.BUCKET) && Strings.isNullOrEmpty(bucket)) {
      collector.addFailure("Bucket name must be specified", null)
        .withConfigProperty(CouchbaseConstants.BUCKET);
    }
    if (!containsMacro(CouchbaseConstants.SELECT_FIELDS) && Strings.isNullOrEmpty(selectFields)) {
      collector.addFailure("Select fields must be specified", null)
        .withConfigProperty(CouchbaseConstants.SELECT_FIELDS);
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
    if (!containsMacro(CouchbaseConstants.ON_ERROR)) {
      if (Strings.isNullOrEmpty(onError)) {
        collector.addFailure("Error handling must be specified", null)
          .withConfigProperty(CouchbaseConstants.ON_ERROR);
      } else if (ErrorHandling.fromDisplayName(onError) == null) {
        collector.addFailure("Invalid record error handling strategy name",
                             "Specify valid error handling strategy name")
          .withConfigProperty(CouchbaseConstants.ON_ERROR);
      }
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
    // TODO Couchbase Server 4.5 introduces INFER, a N1QL statement that infers the metadata of documents.
    // This can be used to infer the Output Schema.
    if (!containsMacro(CouchbaseConstants.SCHEMA) && Strings.isNullOrEmpty(schema)) {
      collector.addFailure("Output schema must be specified", null)
        .withConfigProperty(CouchbaseConstants.SCHEMA);
    } else if (!containsMacro(CouchbaseConstants.SCHEMA)) {
      Schema parsedSchema = getParsedSchema();
      validateSchema(parsedSchema, collector);
    }
  }

  private void validateSchema(Schema schema, FailureCollector collector) {
    List<Schema.Field> fields = schema.getFields();
    if (fields == null || fields.isEmpty()) {
      collector.addFailure("Schema must contain at least one field", null)
        .withConfigProperty(CouchbaseConstants.SCHEMA);
      return;
    }
    for (Schema.Field field : fields) {
      validateFieldSchema(field.getName(), field.getSchema(), collector);
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
                           String.format("Change field '%s' to use a non-nullable string as the map key", fieldName))
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
