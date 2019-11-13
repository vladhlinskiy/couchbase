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

import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.google.common.base.Preconditions;
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
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.couchbase.CouchbaseConstants;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;

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
    Schema schema = config.getParsedSchema();
    pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
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
}
