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

import com.couchbase.client.java.document.JsonDocument;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceBatchSink;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.couchbase.CouchbaseConfig;
import io.cdap.plugin.couchbase.CouchbaseConstants;
import io.cdap.plugin.couchbase.OperationType;
import org.apache.hadoop.io.NullWritable;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link BatchSink} that writes data to Couchbase bucket.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(CouchbaseConstants.PLUGIN_NAME)
@Description("Couchbase Batch Sink writes to a Couchbase bucket.")
public class CouchbaseSink extends ReferenceBatchSink<StructuredRecord, NullWritable, JsonDocument> {

  private final CouchbaseSinkConfig config;
  private RecordToJsonDocumentTransformer transformer;

  public CouchbaseSink(CouchbaseSinkConfig config) {
    super(new ReferencePluginConfig(config.getReferenceName()));
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();
    config.validate(collector);
    Schema inputSchema = stageConfigurer.getInputSchema();
    if (inputSchema != null) {
      config.validateSchema(inputSchema, collector);
    }
    collector.getOrThrowException();
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();
    emitLineage(context);
    context.addOutput(Output.of(config.getReferenceName(), new CouchbaseOutputFormatProvider(config)));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    transformer = new RecordToJsonDocumentTransformer(config.getKeyField());
  }

  @Override
  public void transform(StructuredRecord record, Emitter<KeyValue<NullWritable, JsonDocument>> emitter) {
    JsonDocument jsonDocument = transformer.transform(record);
    emitter.emit(new KeyValue<>(NullWritable.get(), jsonDocument));
  }

  private void emitLineage(BatchSinkContext context) {
    if (Objects.nonNull(context.getInputSchema())) {
      LineageRecorder lineageRecorder = new LineageRecorder(context, config.getReferenceName());
      lineageRecorder.createExternalDataset(context.getInputSchema());
      List<Schema.Field> fields = context.getInputSchema().getFields();
      if (fields != null && !fields.isEmpty()) {
        lineageRecorder.recordWrite("Write",
                                    String.format("Wrote to '%s' Couchbase bucket.", config.getBucket()),
                                    fields.stream().map(Schema.Field::getName).collect(Collectors.toList()));
      }
    }
  }

  /**
   * Config class for {@link CouchbaseSink}.
   */
  public static class CouchbaseSinkConfig extends CouchbaseConfig {

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

    public Integer getBatchSize() {
      return batchSize;
    }

    public OperationType getOperationType() {
      return OperationType.valueOf(operation);
    }

    /**
     * Validates {@link CouchbaseSink.CouchbaseSinkConfig} instance.
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
    }
  }
}
