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
package io.cdap.plugin.couchbase.etl;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.couchbase.Consistency;
import io.cdap.plugin.couchbase.CouchbaseConstants;
import io.cdap.plugin.couchbase.ErrorHandling;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

public class CouchbaseSourceETLTest extends BaseCouchbaseETLTest {

  private static final Schema INNER_OBJECT_SCHEMA = Schema.recordOf(
    "inner-object-schema",
    Schema.Field.of("key", Schema.of(Schema.Type.STRING)));

  private static final Schema SCHEMA = Schema.recordOf(
    "schema",
    Schema.Field.of("uuid", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("boolean", Schema.of(Schema.Type.BOOLEAN)),
    Schema.Field.of("number_double", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("number_int", Schema.of(Schema.Type.INT)),
    Schema.Field.of("number_long", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("number_decimal", Schema.decimalOf(3, 2)),
    Schema.Field.of("number_big_int", Schema.decimalOf(21, 0)),
    Schema.Field.of("string", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("object", INNER_OBJECT_SCHEMA),
    Schema.Field.of("object_map", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))),
    Schema.Field.of("null", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("date_as_string", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("boolean_array", Schema.arrayOf(Schema.of(Schema.Type.BOOLEAN))),
    Schema.Field.of("number_array", Schema.arrayOf(Schema.of(Schema.Type.LONG))),
    Schema.Field.of("object_array", Schema.arrayOf(INNER_OBJECT_SCHEMA))
  );

  private static final List<JsonDocument> TEST_DOCUMENTS = Arrays.asList(
    JsonDocument.create(UUID.randomUUID().toString(),
                        JsonObject.create()
                          .put("uuid", UUID.randomUUID().toString())
                          .put("boolean", true)
                          .put("number_double", Double.MIN_VALUE)
                          .put("number_int", Integer.MIN_VALUE)
                          .put("number_long", Long.MIN_VALUE)
                          .put("number_decimal", new BigDecimal("3.14"))
                          .put("number_big_int",
                               BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.valueOf(Long.MAX_VALUE)))
                          .put("string", "string_value")
                          .put("object", JsonObject.create().put("key", "value"))
                          .put("object_map", JsonObject.create().put("key", "value"))
                          .putNull("null")
                          .put("date_as_string", "2006-01-02T15:04:05.567+08:00")
                          .put("boolean_array", JsonArray.create().add(true).add(false))
                          .put("number_array", JsonArray.create().add(Long.MIN_VALUE).add(0L).add(Long.MAX_VALUE))
                          .put("object_array", JsonArray.create()
                            .add(JsonObject.create().put("key", "value1"))
                            .add(JsonObject.create().put("key", "value2"))
                            .add(JsonObject.create().put("key", "value3"))
                          )
    ),
    JsonDocument.create(UUID.randomUUID().toString(),
                        JsonObject.create()
                          .put("uuid", UUID.randomUUID().toString())
                          .put("boolean", false)
                          .put("number_double", Double.MAX_VALUE)
                          .put("number_int", Integer.MAX_VALUE)
                          .put("number_long", Long.MAX_VALUE)
                          .put("number_decimal", new BigDecimal("3.14"))
                          .put("number_big_int",
                               BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.valueOf(Long.MAX_VALUE)))
                          .put("string", "string_value")
                          .put("object", JsonObject.create().put("key", "value"))
                          .put("object_map", JsonObject.create().put("key", "value"))
                          .putNull("null")
                          .put("date_as_string", "2006-03-02T15:04:05.567+08:00")
                          .put("boolean_array", JsonArray.create().add(true).add(false))
                          .put("number_array", JsonArray.create().add(Long.MIN_VALUE).add(0L).add(Long.MAX_VALUE))
                          .put("object_array", JsonArray.create())
    ),
    JsonDocument.create(UUID.randomUUID().toString(),
                        JsonObject.create()
                          .put("uuid", UUID.randomUUID().toString())
                          .put("boolean", false)
                          .put("number_double", 0d)
                          .put("number_int", 0)
                          .put("number_long", 0L)
                          .put("number_decimal", new BigDecimal("0.00"))
                          .put("number_big_int", BigInteger.valueOf(0L))
                          .put("string", "")
                          .put("object", JsonObject.create().put("key", "value"))
                          .put("object_map", JsonObject.create().put("key", "value"))
                          .putNull("null")
                          .put("date_as_string", "2006-03-02T15:04:05.567+08:00")
                          .put("boolean_array", JsonArray.create())
                          .put("number_array", JsonArray.create())
                          .put("object_array", JsonArray.create())
    )
  );

  private static final BiConsumer<JsonDocument, StructuredRecord> COMPARE_COMMON = (expected, actual) -> {
    JsonObject content = expected.content();
    Assert.assertEquals(content.getBoolean("boolean"), actual.<Boolean>get("boolean"));
    Assert.assertEquals(content.getString("string"), actual.<String>get("string"));

    Assert.assertNull(actual.get("null"));
    Assert.assertEquals(content.getString("date_as_string"), actual.<String>get("date_as_string"));

    JsonArray booleanArrayExpected = content.getArray("boolean_array");
    List<Boolean> booleanArrayActual = actual.get("boolean_array");
    Assert.assertEquals(booleanArrayExpected.toList(), booleanArrayActual);

    JsonArray objectArrayExpected = content.getArray("object_array");
    List<StructuredRecord> objectArrayActual = actual.get("object_array");
    for (int i = 0; i < objectArrayExpected.size(); i++) {
      JsonObject itemExpected = objectArrayExpected.getObject(i);
      StructuredRecord itemActual = objectArrayActual.get(i);
      Assert.assertEquals(itemExpected.getString("key"), itemActual.<String>get("key"));
    }
  };

  private static final BiConsumer<JsonDocument, StructuredRecord> COMPARE_EXPLICIT_TYPES = (expected, actual) -> {
    JsonObject content = expected.content();
    Assert.assertEquals(content.getDouble("number_double"), actual.<Double>get("number_double"));
    Assert.assertEquals(content.getInt("number_int"), actual.<Integer>get("number_int"));
    Assert.assertEquals(content.getLong("number_long"), actual.<Long>get("number_long"));
    Assert.assertEquals(content.getBigDecimal("number_decimal"), actual.getDecimal("number_decimal"));
    Assert.assertEquals(content.getBigInteger("number_big_int"), actual.getDecimal("number_big_int").unscaledValue());

    JsonArray numberArrayExpected = content.getArray("number_array");
    List<Number> numberArrayActual = actual.get("number_array");
    Assert.assertEquals(numberArrayExpected.toList(), numberArrayActual);

    JsonObject innerObjectMapExpected = expected.content().getObject("object_map");
    Map<String, String> innerObjectMapActual = actual.get("object_map");
    for (String key : innerObjectMapExpected.getNames()) {
      Assert.assertEquals(innerObjectMapExpected.get(key), innerObjectMapActual.get(key));
    }
    JsonObject innerObjectRecordExpected = expected.content().getObject("object");
    StructuredRecord innerObjectRecordActual = actual.get("object");
    for (String key : innerObjectRecordExpected.getNames()) {
      Assert.assertEquals(innerObjectRecordExpected.get(key), innerObjectRecordActual.get(key));
    }
  };

  private static final BiConsumer<JsonDocument, StructuredRecord> COMPARE_INFERRED_TYPES = (expected, actual) -> {
    JsonObject content = expected.content();
    Assert.assertEquals(content.getDouble("number_double").toString(), actual.<String>get("number_double"));
    Assert.assertEquals(content.getInt("number_int").toString(), actual.<String>get("number_int"));
    Assert.assertEquals(content.getLong("number_long").toString(), actual.<String>get("number_long"));

    // Couchabse stores '0.00' as '0' and scale/precision can not be honored when reading as string
    double expectedDoubleValue = content.getBigDecimal("number_decimal").doubleValue();
    String actualStringValue = actual.get("number_decimal");
    Assert.assertNotNull(actualStringValue);
    Assert.assertEquals(expectedDoubleValue, Double.valueOf(actualStringValue), 0.00001);

    Assert.assertEquals(content.getBigInteger("number_big_int").toString(), actual.<String>get("number_big_int"));
    List<String> numberArrayExpected = content.getArray("number_array").toList().stream()
      .map(Object::toString)
      .collect(Collectors.toList());
    List<String> numberArrayActual = actual.get("number_array");
    Assert.assertEquals(numberArrayExpected, numberArrayActual);
    JsonObject innerObjectMapExpected = expected.content().getObject("object_map");
    StructuredRecord innerObjectMapActual = actual.get("object_map");
    for (String key : innerObjectMapExpected.getNames()) {
      Assert.assertEquals(innerObjectMapExpected.get(key), innerObjectMapActual.get(key));
    }
    JsonObject innerObjectRecordExpected = expected.content().getObject("object");
    StructuredRecord innerObjectRecordActual = actual.get("object");
    for (String key : innerObjectRecordExpected.getNames()) {
      Assert.assertEquals(innerObjectRecordExpected.get(key), innerObjectRecordActual.get(key));
    }
  };

  private static final BiConsumer<JsonDocument, StructuredRecord> COMPARE_IDENTIFIERS = (expected, actual) -> {
    Assert.assertEquals(expected.id(), actual.get("id"));
  };

  @BeforeClass
  public static void prepareTestData() throws Exception {
    TEST_DOCUMENTS.forEach(bucket::insert);
  }

  @Test
  public void testSource() throws Exception {
    Map<String, String> properties = sourceProperties("*", SCHEMA);
    List<StructuredRecord> records = getPipelineResults(properties);
    verifyPipelineResults(TEST_DOCUMENTS, records, COMPARE_COMMON, COMPARE_EXPLICIT_TYPES);
  }

  @Test
  public void testSourceSingleSplit() throws Exception {
    Map<String, String> properties = sourceProperties("*", SCHEMA, 1);
    List<StructuredRecord> records = getPipelineResults(properties);
    verifyPipelineResults(TEST_DOCUMENTS, records, COMPARE_COMMON, COMPARE_EXPLICIT_TYPES);
  }

  @Test
  public void testSourceTwoSplits() throws Exception {
    Map<String, String> properties = sourceProperties("*", SCHEMA, 2);
    List<StructuredRecord> records = getPipelineResults(properties);
    verifyPipelineResults(TEST_DOCUMENTS, records, COMPARE_COMMON, COMPARE_EXPLICIT_TYPES);
  }

  @Test
  public void testSourceNumSplitsGreaterThanDocumentsNumber() throws Exception {
    Map<String, String> properties = sourceProperties("*", SCHEMA, 20);
    List<StructuredRecord> records = getPipelineResults(properties);
    verifyPipelineResults(TEST_DOCUMENTS, records, COMPARE_COMMON, COMPARE_EXPLICIT_TYPES);
  }

  @Test
  public void testSourceWithConditions() throws Exception {
    Map<String, String> properties = sourceProperties("*", "`boolean` = true", SCHEMA, null);
    List<StructuredRecord> records = getPipelineResults(properties);
    // single document satisfies the criteria
    List<JsonDocument> expected = Collections.singletonList(TEST_DOCUMENTS.get(0));
    verifyPipelineResults(expected, records, COMPARE_COMMON, COMPARE_EXPLICIT_TYPES);
  }

  @Test
  public void testSourceSingleField() throws Exception {
    Schema schema = Schema.recordOf("inner-object-schema", Schema.Field.of("uuid", Schema.of(Schema.Type.STRING)));
    Map<String, String> properties = sourceProperties("uuid", schema);
    List<StructuredRecord> records = getPipelineResults(properties);
    verifyPipelineResults(TEST_DOCUMENTS, records);
  }

  @Test
  public void testSourceIncludeId() throws Exception {
    String bucketName = BASE_PROPERTIES.get(CouchbaseConstants.BUCKET);
    String selectFields = String.format("meta(`%s`).id, *", bucketName);
    Schema schemaWithIdIncluded = Schema.recordOf(
      "schema",
      ImmutableList.<Schema.Field>builder()
        .add(Schema.Field.of("id", Schema.of(Schema.Type.STRING)))
        .addAll(SCHEMA.getFields())
        .build()
    );
    Map<String, String> properties = sourceProperties(selectFields, schemaWithIdIncluded);
    List<StructuredRecord> records = getPipelineResults(properties);
    verifyPipelineResults(TEST_DOCUMENTS, records, COMPARE_COMMON, COMPARE_EXPLICIT_TYPES, COMPARE_IDENTIFIERS);
  }

  @Test
  public void testSourceInferredSchema() throws Exception {
    Map<String, String> properties = sourceProperties("*", null);
    List<StructuredRecord> records = getPipelineResults(properties);
    verifyPipelineResults(TEST_DOCUMENTS, records, COMPARE_COMMON, COMPARE_INFERRED_TYPES);
  }

  @Test
  public void testSourceWithConditionsInferredSchema() throws Exception {
    Map<String, String> properties = sourceProperties("*", "`boolean` = true", null, null);
    List<StructuredRecord> records = getPipelineResults(properties);
    // single document satisfies the criteria
    List<JsonDocument> expected = Collections.singletonList(TEST_DOCUMENTS.get(0));
    verifyPipelineResults(expected, records, COMPARE_COMMON, COMPARE_INFERRED_TYPES);
  }

  @Test
  public void testSourceSingleFieldInferredSchema() throws Exception {
    Map<String, String> properties = sourceProperties("uuid", null);
    List<StructuredRecord> records = getPipelineResults(properties);
    verifyPipelineResults(TEST_DOCUMENTS, records);
  }

  @Test
  public void testSourceIncludeIdInferredSchema() throws Exception {
    String bucketName = BASE_PROPERTIES.get(CouchbaseConstants.BUCKET);
    String selectFields = String.format("meta(`%s`).id, *", bucketName);
    Map<String, String> properties = sourceProperties(selectFields, null);
    List<StructuredRecord> records = getPipelineResults(properties);
    verifyPipelineResults(TEST_DOCUMENTS, records, COMPARE_COMMON, COMPARE_INFERRED_TYPES, COMPARE_IDENTIFIERS);
  }

  private void verifyPipelineResults(List<JsonDocument> expected,
                                     List<StructuredRecord> actual,
                                     BiConsumer<JsonDocument, StructuredRecord>... tests) {
    Assert.assertEquals(expected.size(), actual.size());
    for (StructuredRecord actualRecord : actual) {
      String actualUUID = actualRecord.get("uuid");
      Assert.assertNotNull(actualUUID);
      JsonDocument expectedDocument = getTestDocumentByUUID(actualUUID);
      // Perform supplied test actions
      Stream.of(tests).forEach(test -> test.accept(expectedDocument, actualRecord));
    }
  }

  @Nullable
  private JsonDocument getTestDocumentByUUID(String uuid) {
    return TEST_DOCUMENTS.stream()
      .filter(d -> uuid.equals(d.content().getString("uuid")))
      .findAny()
      .orElse(null);
  }

  private Map<String, String> sourceProperties(String selectFields, @Nullable Schema schema,
                                               @Nullable Integer numSplits) {
    return sourceProperties(selectFields, null, schema, numSplits);
  }

  private Map<String, String> sourceProperties(String selectFields, @Nullable Schema schema) {
    return sourceProperties(selectFields, null, schema, null);
  }

  private Map<String, String> sourceProperties(String selectFields, @Nullable String conditions,
                                               @Nullable Schema schema, @Nullable Integer numSplits) {
    return new ImmutableMap.Builder<String, String>()
      .put(CouchbaseConstants.NODES, BASE_PROPERTIES.get(CouchbaseConstants.NODES))
      .put(CouchbaseConstants.USERNAME, BASE_PROPERTIES.get(CouchbaseConstants.USERNAME))
      .put(CouchbaseConstants.PASSWORD, BASE_PROPERTIES.get(CouchbaseConstants.PASSWORD))
      .put(CouchbaseConstants.BUCKET, BASE_PROPERTIES.get(CouchbaseConstants.BUCKET))
      .put(CouchbaseConstants.ON_ERROR, ErrorHandling.FAIL_PIPELINE.getDisplayName())
      .put(CouchbaseConstants.SCHEMA, schema != null ? schema.toString() : "")
      .put(CouchbaseConstants.SELECT_FIELDS, selectFields)
      .put(CouchbaseConstants.CONDITIONS, conditions != null ? conditions : "")
      .put(CouchbaseConstants.NUM_SPLITS, numSplits != null ? numSplits.toString() : "0")
      .put(CouchbaseConstants.SAMPLE_SIZE, "1000")
      .put(CouchbaseConstants.MAX_PARALLELISM, "0")
      .put(CouchbaseConstants.SCAN_CONSISTENCY, Consistency.NOT_BOUNDED.getDisplayName())
      .put(CouchbaseConstants.QUERY_TIMEOUT, "600")
      .build();
  }

  private List<StructuredRecord> getPipelineResults(Map<String, String> sourceProperties) throws Exception {
    Map<String, String> allProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, name.getMethodName())
      .putAll(sourceProperties)
      .build();

    ETLStage source = new ETLStage("CouchbaseSource", new ETLPlugin(CouchbaseConstants.PLUGIN_NAME,
                                                                    BatchSource.PLUGIN_TYPE, allProperties, null));

    String outputDatasetName = "output-batchsourcetest_" + name.getMethodName();
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    ApplicationId pipelineId = NamespaceId.DEFAULT.app("Couchbase_" + name.getMethodName());
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, etlConfig));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    return MockSink.readOutput(outputManager);
  }
}
