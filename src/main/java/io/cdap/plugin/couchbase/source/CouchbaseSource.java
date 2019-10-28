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
import io.cdap.cdap.api.annotation.Description;
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
import io.cdap.plugin.couchbase.CouchbaseConfig;
import io.cdap.plugin.couchbase.CouchbaseConstants;
import io.cdap.plugin.couchbase.CouchbaseUtil;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Plugin returns records from Couchbase Server.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(CouchbaseConstants.PLUGIN_NAME)
@Description("Read data from Couchbase Server.")
public class CouchbaseSource extends BatchSource<NullWritable, JsonObject, StructuredRecord> {

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

    context.setInput(Input.of(config.getReferenceName(), new JsonObjectInputFormatProvider(config)));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    Schema schema = context.getOutputSchema();
    this.transformer = new JsonObjectToRecordTransformer(config, schema);
  }

  @Override
  public void transform(KeyValue<NullWritable, JsonObject> input, Emitter<StructuredRecord> emitter) {
    JsonObject value = input.getValue();
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
      String fieldName = CouchbaseUtil.fieldName(propertyName);
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
}
