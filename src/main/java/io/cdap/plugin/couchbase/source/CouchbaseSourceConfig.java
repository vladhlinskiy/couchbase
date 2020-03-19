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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.couchbase.Consistency;
import io.cdap.plugin.couchbase.CouchbaseConfig;
import io.cdap.plugin.couchbase.CouchbaseConstants;
import io.cdap.plugin.couchbase.ErrorHandling;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;


/**
 * Config class for {@link CouchbaseSource}.
 */
public class CouchbaseSourceConfig extends CouchbaseConfig {

  public static final Schema ERROR_SCHEMA = Schema.recordOf("error", Schema.Field.of("document",
                                                                                     Schema.of(Schema.Type.STRING)));

  private static final Set<Schema.Type> SUPPORTED_TYPES = ImmutableSet.of(Schema.Type.BOOLEAN, Schema.Type.INT,
                                                                          Schema.Type.DOUBLE, Schema.Type.LONG,
                                                                          Schema.Type.STRING, Schema.Type.RECORD,
                                                                          Schema.Type.ARRAY, Schema.Type.MAP);

  private static final Set<Schema.LogicalType> SUPPORTED_LOGICAL_TYPES = ImmutableSet.of(Schema.LogicalType.DECIMAL);

  @Name(CouchbaseConstants.SELECT_FIELDS)
  @Description("Comma-separated list of fields to be read.")
  @Macro
  private String selectFields;

  @Name(CouchbaseConstants.CONDITIONS)
  @Description("Optional criteria (filters or predicates) that the result documents must satisfy. Corresponds to " +
    "the WHERE clause in N1QL SELECT statement.")
  @Macro
  @Nullable
  private String conditions;

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
  private int maxParallelism;

  @Name(CouchbaseConstants.SCAN_CONSISTENCY)
  @Description("Specifies the consistency guarantee or constraint for index scanning.")
  @Macro
  private String consistency;

  @Name(CouchbaseConstants.QUERY_TIMEOUT)
  @Description("Number of seconds to wait before a timeout has occurred on a query.")
  @Macro
  private int timeout;

  public CouchbaseSourceConfig(String referenceName, String nodes, String bucket, String user, String password,
                               String selectFields, String conditions, String onError, String schema,
                               int maxParallelism, String consistency, int timeout) {
    super(referenceName, nodes, bucket, user, password);
    this.selectFields = selectFields;
    this.conditions = conditions;
    this.onError = onError;
    this.schema = schema;
    this.maxParallelism = maxParallelism;
    this.consistency = consistency;
    this.timeout = timeout;
  }

  public String getSelectFields() {
    return selectFields;
  }

  @Nullable
  public String getConditions() {
    return conditions;
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
      return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
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
  @Override
  public void validate(FailureCollector collector) {
    super.validate(collector);
    if (!containsMacro(CouchbaseConstants.SELECT_FIELDS) && Strings.isNullOrEmpty(selectFields)) {
      collector.addFailure("Select fields must be specified", null)
        .withConfigProperty(CouchbaseConstants.SELECT_FIELDS);
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
    super.validateSchema(schema, SUPPORTED_LOGICAL_TYPES, SUPPORTED_TYPES, collector);
  }
}
