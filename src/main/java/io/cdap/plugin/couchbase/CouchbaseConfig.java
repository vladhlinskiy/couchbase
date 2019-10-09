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
import com.google.common.base.Throwables;
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
public class CouchbaseConfig extends PluginConfig {

  // TODO review
  private static final Set<Schema.Type> SUPPORTED_SIMPLE_TYPES = ImmutableSet.of(Schema.Type.BOOLEAN, Schema.Type.INT,
                                                                                 Schema.Type.FLOAT, Schema.Type.DOUBLE,
                                                                                 Schema.Type.BYTES, Schema.Type.LONG,
                                                                                 Schema.Type.STRING);

  private static final Set<Schema.LogicalType> SUPPORTED_LOGICAL_TYPES = ImmutableSet.of(
    Schema.LogicalType.DECIMAL, Schema.LogicalType.TIMESTAMP_MILLIS, Schema.LogicalType.TIMESTAMP_MICROS);

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

  @Name(CouchbaseConstants.QUERY)
  @Description("N1QL query to use to import data from the specified bucket.")
  @Macro
  private String query;

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
  @Nullable
  private String schema;

  public CouchbaseConfig(String referenceName, String nodes, String bucket, String query, String user,
                         String password, String schema) {
    this.referenceName = referenceName;
    this.nodes = nodes;
    this.bucket = bucket;
    this.query = query;
    this.user = user;
    this.password = password;
    this.schema = schema;
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

  public String getQuery() {
    return query;
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

  @Nullable
  public String getSchema() {
    return schema;
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
      throw Throwables.propagate(e);
    }
  }

  public List<String> getNodeList() {
    return Arrays.asList(getNodes().split(","));
  }

  public ErrorHandling getErrorHandling() {
    return Objects.requireNonNull(ErrorHandling.fromDisplayName(onError));
  }

  /**
   * Validates {@link CouchbaseConfig} instance.
   *
   * @param collector failure collector.
   */
  public void validate(FailureCollector collector) {
    // TODO review corrective actions
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
      collector.addFailure("Couchbase nodes must be specified", "Specify valid Couchbase nodes")
        .withConfigProperty(CouchbaseConstants.NODES);
    }
    if (!containsMacro(CouchbaseConstants.BUCKET) && Strings.isNullOrEmpty(bucket)) {
      collector.addFailure("Bucket name must be specified", "Specify valid bucket name")
        .withConfigProperty(CouchbaseConstants.BUCKET);
    }
    if (!containsMacro(CouchbaseConstants.ON_ERROR)) {
      if (Strings.isNullOrEmpty(onError)) {
        collector.addFailure("Error handling must be specified", "Specify error handling")
          .withConfigProperty(CouchbaseConstants.BUCKET);
      }
      if (ErrorHandling.fromDisplayName(onError) == null) {
        collector.addFailure("Invalid record error handling strategy name",
                             "Specify valid error handling strategy name")
          .withConfigProperty(CouchbaseConstants.BUCKET);
      }
    }
    if (!Strings.isNullOrEmpty(schema) && !containsMacro(CouchbaseConstants.SCHEMA)) {
      Schema parsedSchema = getParsedSchema();
      validateSchema(parsedSchema, collector);
    }

    collector.getOrThrowException();
  }

  private void validateSchema(Schema parsedSchema, FailureCollector collector) {
    List<Schema.Field> fields = parsedSchema.getFields();
    if (null == fields || fields.isEmpty()) {
      collector.addFailure("Schema must contain at least one field", null)
        .withConfigProperty(CouchbaseConstants.SCHEMA);
      collector.getOrThrowException();
    }
    for (Schema.Field field : fields) {
      Schema nonNullableSchema = field.getSchema().isNullable() ?
        field.getSchema().getNonNullable() : field.getSchema();
      Schema.Type type = nonNullableSchema.getType();
      Schema.LogicalType logicalType = nonNullableSchema.getLogicalType();
      if (!SUPPORTED_SIMPLE_TYPES.contains(type) && !SUPPORTED_LOGICAL_TYPES.contains(logicalType)) {
        String supportedTypeNames = Stream.concat(
          SUPPORTED_SIMPLE_TYPES.stream().map(Enum::name).map(String::toLowerCase),
          SUPPORTED_LOGICAL_TYPES.stream().map(Schema.LogicalType::getToken)
        ).collect(Collectors.joining(", "));
        String errorMessage = String.format("Field '%s' is of unsupported type '%s'. Supported types are: %s",
                                            field.getName(), nonNullableSchema.getDisplayName(), supportedTypeNames);
        collector.addFailure(errorMessage, String.format("Change field '%s' to be a supported type", field.getName()))
          .withOutputSchemaField(field.getName(), null);
      }
    }
  }
}
