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

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.couchbase.Consistency;
import io.cdap.plugin.couchbase.CouchbaseConfig;
import io.cdap.plugin.couchbase.CouchbaseConstants;
import io.cdap.plugin.couchbase.ErrorHandling;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Plugin returns records from Couchbase Server.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(CouchbaseConstants.PLUGIN_NAME)
@Description("Read data from Couchbase Server.")
public class CouchbaseSource extends BatchSource<NullWritable, N1qlQueryRow, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(CouchbaseSource.class);
  private final CouchbaseSourceConfig config;
  private JsonObjectToRecordTransformer transformer;

  public CouchbaseSource(CouchbaseSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();
    Schema schema = getSchema();
    Schema configuredSchema = config.getParsedSchema();
    if (configuredSchema == null) {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
      return;
    }
    CouchbaseConfig.validateFieldsMatch(schema, configuredSchema, collector);
    collector.getOrThrowException();
    pipelineConfigurer.getStageConfigurer().setOutputSchema(configuredSchema);
    pipelineConfigurer.getStageConfigurer().setErrorSchema(CouchbaseSourceConfig.ERROR_SCHEMA);
  }

  @Override
  public void prepareRun(BatchSourceContext context) {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();
    Schema schema = context.getOutputSchema();
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.getReferenceName());
    lineageRecorder.createExternalDataset(schema);
    lineageRecorder.recordRead("Read", String.format("Read bucket '%s' from Couchbase cluster '%s'",
                                                     config.getBucket(), config.getNodes()),
                               Preconditions.checkNotNull(schema.getFields()).stream()
                                 .map(Schema.Field::getName)
                                 .collect(Collectors.toList()));

    context.setInput(Input.of(config.getReferenceName(), new N1qlQueryRowInputFormatProvider(config)));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    Schema schema = context.getOutputSchema();
    this.transformer = new JsonObjectToRecordTransformer(config, schema);
  }

  @Override
  public void transform(KeyValue<NullWritable, N1qlQueryRow> input, Emitter<StructuredRecord> emitter) {
    N1qlQueryRow row = input.getValue();
    JsonObject value = row.value();
    try {
      emitter.emit(transformer.transform(value));
    } catch (Exception e) {
      switch (config.getErrorHandling()) {
        case SEND_TO_ERROR:
          StructuredRecord errorRecord = StructuredRecord.builder(CouchbaseSourceConfig.ERROR_SCHEMA)
            .set("document", value.toString())
            .build();
          emitter.emitError(new InvalidEntry<>(400, e.getMessage(), errorRecord));
          break;
        case SKIP:
          LOG.warn("Failed to process record, skipping it", e);
          break;
        case FAIL_PIPELINE:
          throw new RuntimeException("Failed to process record", e);
        default:
          // this should never happen because it is validated at configure and prepare time
          throw new IllegalStateException(String.format("Unknown error handling strategy '%s'",
                                                        config.getErrorHandling()));
      }
    }
  }

  public Schema getSchema() {
    Cluster cluster = CouchbaseCluster.create(config.getNodeList());
    if (!Strings.isNullOrEmpty(config.getUser()) || !Strings.isNullOrEmpty(config.getPassword())) {
      cluster.authenticate(config.getUser(), config.getPassword());
    }
    String bucketName = config.getBucket();
    Bucket bucket = cluster.openBucket(bucketName);
    String inferStatement = String.format("INFER `%s` WITH {\"sample_size\":%d};", bucketName, config.getSampleSize());
    N1qlQuery query = N1qlQuery.simple(inferStatement);
    N1qlQueryResult result = bucket.query(query);
    if (!result.finalSuccess()) {
      String errorMessage = result.errors().stream()
        .map(JsonObject::toString)
        .collect(Collectors.joining("\n"));
      throw new InvalidStageException(String.format("Unable to infer output schema: '%s'", errorMessage));
    }

    // row.value() can not be used since it expects the result to be a JSON document. However, the result is an array
    // that can not be deserialized as JsonObject
    N1qlQueryRow row = result.rows().next();
    String json = row.toString();
    // Deserialize the output in the JSON Schema draft v4 format as JsonArray
    // The output is an array of schema documents, each of them groups properties for docs with similar structure
    // See: https://docs.couchbase.com/server/current/n1ql/n1ql-language-reference/infer.html#example
    JsonArray couchbaseSchema = JsonArray.fromJson(json);

    List<Schema.Field> fields = new ArrayList<>();
    // Select fields may contain metadata fields such as meta(`travel-sample`).id
    // Include them to the inferred schema as well using simple names after dot character
    // All metadata fields are strings
    if (config.getSelectFieldsList() != null) {
      List<Schema.Field> metadataFields = config.getSelectFieldsList().stream()
        .filter(f -> f.startsWith("meta"))
        .map(f -> f.substring(f.lastIndexOf(".") + 1))
        .map(simpleName -> Schema.Field.of(simpleName, Schema.of(Schema.Type.STRING)))
        .collect(Collectors.toList());
      fields.addAll(metadataFields);
    }

    for (int i = 0; i < couchbaseSchema.size(); i++) {
      JsonObject schema = couchbaseSchema.getObject(i);
      List<Schema.Field> schemaFields = recordSchema(schema, config.getSelectFieldsList());

      // Schemas ordered by number of sample documents.
      // Use inferred type that matches most of the documents
      schemaFields.stream()
        .filter(f -> !fields.contains(f))
        .forEach(fields::add);
    }

    return Schema.recordOf("inferred-schema", fields);
  }

  private List<Schema.Field> recordSchema(JsonObject recordMetadata, @Nullable List<String> selectFields) {
    JsonObject couchbasePropertiesMetadata = recordMetadata.getObject("properties");

    List<Schema.Field> fields = new ArrayList<>();
    for (String propertyName : couchbasePropertiesMetadata.getNames()) {
      if (selectFields != null && !selectFields.contains(propertyName) && !selectFields.contains("*")) {
        // include only selected fields
        continue;
      }
      JsonObject propertyMetadata = couchbasePropertiesMetadata.getObject(propertyName);
      Schema propertySchema = propertySchema(propertyName, propertyMetadata);
      // TODO move to utils
      // Replaces any character that are not one of [A-Z][a-z][0-9] or _ with an underscore (_).
      String fieldName = propertyName.toLowerCase().replaceAll("[^A-Za-z0-9]", "_");
      fields.add(Schema.Field.of(fieldName, propertySchema));
    }

    return fields;
  }

  private Schema propertySchema(String propertyName, JsonObject propertyMetadata) {
    String couchbaseType = propertyMetadata.getString("type");
    switch (couchbaseType) {
      case "null":
      case "string":
      case "number":
        return Schema.nullableOf(Schema.of(Schema.Type.STRING));
      case "boolean":
        return Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN));
      case "array":
        JsonObject componentMetadata = propertyMetadata.getObject("items");
        Schema componentSchema = propertySchema(propertyName, componentMetadata);
        return Schema.nullableOf(Schema.arrayOf(componentSchema));
      case "object":
        List<Schema.Field> objectFields = recordSchema(propertyMetadata, null);
        return Schema.nullableOf(Schema.recordOf(propertyName + "-inferred-nested", objectFields));
      default:
        // this should never happen
        throw new InvalidStageException(String.format("Field '%s' is of unsupported type '%s'.", propertyName,
                                                      propertyMetadata));
    }
  }

  /**
   * Config class for {@link CouchbaseSource}.
   */
  public static class CouchbaseSourceConfig extends CouchbaseConfig {

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

    @Name(CouchbaseConstants.SAMPLE_SIZE)
    @Description("Configuration property name used to specify the number of documents to randomly sample in the " +
      "bucket when inferring the schema. The default sample size is 1000 documents. If a bucket contains fewer " +
      "documents than the specified number, then all the documents in the bucket will be used.")
    private int sampleSize;

    @Name(CouchbaseConstants.MAX_PARALLELISM)
    @Description("Maximum number of CPU cores can be used to process a query. If the specified value is less " +
      "than zero or greater than the total number of cores in a cluster, the system will use all available cores in " +
      "the cluster.")
    private int maxParallelism;

    @Name(CouchbaseConstants.SCAN_CONSISTENCY)
    @Description("Specifies the consistency guarantee or constraint for index scanning.")
    private String consistency;

    @Name(CouchbaseConstants.QUERY_TIMEOUT)
    @Description("Number of seconds to wait before a timeout has occurred on a query.")
    private int timeout;

    public CouchbaseSourceConfig(String referenceName, String nodes, String bucket, String user, String password,
                                 String selectFields, String conditions, String onError, String schema, int sampleSize,
                                 int maxParallelism, String consistency, int timeout) {
      super(referenceName, nodes, bucket, user, password);
      this.selectFields = selectFields;
      this.conditions = conditions;
      this.onError = onError;
      this.schema = schema;
      this.sampleSize = sampleSize;
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

    public int getSampleSize() {
      return sampleSize;
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
      return Arrays.stream(getSelectFields().split(",")).map(String::trim).collect(Collectors.toList());
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
      if (!containsMacro(CouchbaseConstants.SAMPLE_SIZE)) {
        if (sampleSize < 1) {
          collector.addFailure("Sample size must be greater than 0", null)
            .withConfigProperty(CouchbaseConstants.SAMPLE_SIZE);
        }
      }
      if (!containsMacro(CouchbaseConstants.SCHEMA) && !Strings.isNullOrEmpty(schema)) {
        Schema parsedSchema = getParsedSchema();
        validateSchema(parsedSchema, collector);
      }
    }

    private void validateSchema(Schema schema, FailureCollector collector) {
      super.validateSchema(schema, SUPPORTED_LOGICAL_TYPES, SUPPORTED_TYPES, collector);
    }
  }
}
