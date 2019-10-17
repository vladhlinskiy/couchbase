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
import io.cdap.plugin.couchbase.CouchbaseConstants;
import io.cdap.plugin.couchbase.CouchbaseSourceConfig;
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
    CouchbaseSourceConfig.validateFieldsMatch(schema, configuredSchema, collector);
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
    Bucket bucket = cluster.openBucket(config.getBucket());
    N1qlQuery query = N1qlQuery.simple(String.format("INFER `%s`", config.getBucket()));
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
    JsonArray schemaDraft = JsonArray.fromJson(json);
    // The output is an array of single schema document
    // See: https://docs.couchbase.com/server/current/n1ql/n1ql-language-reference/infer.html#example
    JsonObject couchbaseSchema = schemaDraft.getObject(0);
    return recordSchema("parent", couchbaseSchema, config.getSelectFieldsList());
  }

  private Schema recordSchema(String recordName, JsonObject recordMetadata, @Nullable List<String> selectFields) {
    JsonObject couchbasePropertiesMetadata = recordMetadata.getObject("properties");
    List<Schema.Field> fields = new ArrayList<>();
    // Select fields may contain metadata fields such as meta(`travel-sample`).id
    // Include them to the inferred schema as well using simple names after dot character
    // All metadata fields are strings
    if (selectFields != null) {
      List<Schema.Field> metadataFields = selectFields.stream()
        .filter(f -> f.startsWith("meta"))
        .map(f -> f.substring(f.lastIndexOf(".") + 1))
        .map(simpleName -> Schema.Field.of(simpleName, Schema.of(Schema.Type.STRING)))
        .collect(Collectors.toList());
      fields.addAll(metadataFields);
    }

    for (String propertyName : couchbasePropertiesMetadata.getNames()) {
      if (selectFields != null && !selectFields.contains(propertyName) && !selectFields.contains("*")) {
        // include only selected fields
        continue;
      }
      JsonObject propertyMetadata = couchbasePropertiesMetadata.getObject(propertyName);
      Schema propertySchema = propertySchema(propertyName, propertyMetadata);
      fields.add(Schema.Field.of(propertyName, propertySchema));
    }

    return Schema.recordOf(recordName + "-schema", fields);
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
        Schema objectSchema = recordSchema(propertyName, propertyMetadata, null);
        return Schema.nullableOf(objectSchema);
      default:
        // this should never happen
        throw new InvalidStageException(String.format("Field '%s' is of unsupported type '%s'.", propertyName,
                                                      propertyMetadata));
    }
  }
}
