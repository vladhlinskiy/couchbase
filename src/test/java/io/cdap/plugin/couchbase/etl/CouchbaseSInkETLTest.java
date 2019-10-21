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
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
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
import io.cdap.plugin.couchbase.CouchbaseConstants;
import io.cdap.plugin.couchbase.OperationType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class CouchbaseSInkETLTest extends BaseCouchbaseETLTest {

  private static final ZonedDateTime DATE_TIME = ZonedDateTime.now(ZoneOffset.UTC);
  private static final Schema NESTED_SCHEMA = Schema.recordOf(
    "nested",
    Schema.Field.of("inner_field", Schema.of(Schema.Type.STRING)));

  private static final Schema SCHEMA = Schema.recordOf(
    "document",
    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("created", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("string", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("int32", Schema.of(Schema.Type.INT)),
    Schema.Field.of("double", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("array", Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.STRING)))),
    Schema.Field.of("object", NESTED_SCHEMA),
    Schema.Field.of("object-to-map", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))),
    Schema.Field.of("binary", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("boolean", Schema.of(Schema.Type.BOOLEAN)),
    Schema.Field.of("date", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
    Schema.Field.of("null", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
    Schema.Field.of("long", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("decimal", Schema.decimalOf(20, 10)),
    Schema.Field.of("date-field", Schema.of(Schema.LogicalType.DATE)),
    Schema.Field.of("float", Schema.of(Schema.Type.FLOAT)),
    Schema.Field.of("time", Schema.of(Schema.LogicalType.TIME_MILLIS)),
    Schema.Field.of("enum", Schema.enumWith("first", "second")),
    Schema.Field.of("union", Schema.unionOf(Schema.of(Schema.Type.STRING), NESTED_SCHEMA)));

  private static final List<StructuredRecord> TEST_RECORDS = Arrays.asList(
    StructuredRecord.builder(SCHEMA)
      .set("id", "5d079ee6d078c94008e4bb4a")
      .set("created", System.nanoTime())
      .set("string", "AAPL")
      .set("int32", Integer.MIN_VALUE)
      .set("double", Double.MIN_VALUE)
      .set("array", Arrays.asList("a1", "a2"))
      .set("object", StructuredRecord.builder(NESTED_SCHEMA).set("inner_field", "val").build())
      .set("object-to-map", ImmutableMap.builder().put("key", "value").build())
      .set("binary", "binary data".getBytes())
      .set("boolean", false)
      .setTimestamp("date", DATE_TIME)
      .set("long", Long.MIN_VALUE)
      .setDecimal("decimal", new BigDecimal("987654321.1234567890"))
      .setDate("date-field", DATE_TIME.toLocalDate())
      .set("float", Float.MIN_VALUE)
      .setTime("time", DATE_TIME.toLocalTime())
      .set("enum", "first")
      .set("union", StructuredRecord.builder(NESTED_SCHEMA).set("inner_field", "val").build())
      .build(),

    StructuredRecord.builder(SCHEMA)
      .set("id", "5d079ee6d078c94008e4bb47")
      .set("created", System.nanoTime())
      .set("string", "AAPL")
      .set("int32", 10)
      .set("double", 23.23)
      .set("array", Arrays.asList("a1", "a2"))
      .set("object", StructuredRecord.builder(NESTED_SCHEMA).set("inner_field", "val").build())
      .set("object-to-map", ImmutableMap.builder().put("key", "value").build())
      .set("binary", "binary data".getBytes())
      .set("boolean", false)
      .setTimestamp("date", DATE_TIME)
      .set("long", Long.MAX_VALUE)
      .setDecimal("decimal", new BigDecimal("0.1234567890"))
      .setDate("date-field", DATE_TIME.toLocalDate())
      .set("float", Float.MIN_VALUE)
      .setTime("time", DATE_TIME.toLocalTime())
      .set("enum", "second")
      .set("union", "string")
      .build()
  );

  @Before
  public void setupTest() throws Exception {
    // remove all documents
    bucket.bucketManager().flush(COUCHBASE_TIMEOUT, TimeUnit.SECONDS);
  }

  @Test
  public void testSink() throws Exception {
    Map<String, String> sinkProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, name.getMethodName())
      .put(CouchbaseConstants.NODES, BASE_PROPERTIES.get(CouchbaseConstants.NODES))
      .put(CouchbaseConstants.USERNAME, BASE_PROPERTIES.get(CouchbaseConstants.USERNAME))
      .put(CouchbaseConstants.PASSWORD, BASE_PROPERTIES.get(CouchbaseConstants.PASSWORD))
      .put(CouchbaseConstants.BUCKET, BASE_PROPERTIES.get(CouchbaseConstants.BUCKET))
      .put(CouchbaseConstants.KEY_FIELD, "id")
      .put(CouchbaseConstants.SCHEMA, SCHEMA.toString())
      .put(CouchbaseConstants.BATCH_SIZE, "100")
      .put(CouchbaseConstants.OPERATION, OperationType.INSERT.name())
      .build();

    List<N1qlQueryRow> rows = getPipelineResults(sinkProperties, TEST_RECORDS);
    Assert.assertEquals(TEST_RECORDS.size(), rows.size());
    for (int i = 0; i < rows.size(); i++) {
      JsonObject row = rows.get(i).value();
      assertEquals(TEST_RECORDS.get(i), row.getObject(BASE_PROPERTIES.get(CouchbaseConstants.BUCKET)));
    }
  }

  @Test
  public void testSinkReplace() throws Exception {
    // 'replace' will only replace the document if the given ID already exists within the database.
    // 'replace' operation replaces the document if it exists, but fails with a DocumentDoesNotExistException otherwise.
    // Store empty documents with TEST_RECORDS' identifiers.
    TEST_RECORDS.stream()
      .map(r -> JsonDocument.create(r.<String>get("id"), JsonObject.empty()))
      .forEach(bucket::insert);

    Map<String, String> sinkProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, name.getMethodName())
      .put(CouchbaseConstants.NODES, BASE_PROPERTIES.get(CouchbaseConstants.NODES))
      .put(CouchbaseConstants.USERNAME, BASE_PROPERTIES.get(CouchbaseConstants.USERNAME))
      .put(CouchbaseConstants.PASSWORD, BASE_PROPERTIES.get(CouchbaseConstants.PASSWORD))
      .put(CouchbaseConstants.BUCKET, BASE_PROPERTIES.get(CouchbaseConstants.BUCKET))
      .put(CouchbaseConstants.KEY_FIELD, "id")
      .put(CouchbaseConstants.SCHEMA, SCHEMA.toString())
      .put(CouchbaseConstants.BATCH_SIZE, "100")
      .put(CouchbaseConstants.OPERATION, OperationType.REPLACE.name())
      .build();
    List<N1qlQueryRow> rows = getPipelineResults(sinkProperties, TEST_RECORDS);
    Assert.assertEquals(TEST_RECORDS.size(), rows.size());
    for (int i = 0; i < rows.size(); i++) {
      JsonObject row = rows.get(i).value();
      assertEquals(TEST_RECORDS.get(i), row.getObject(BASE_PROPERTIES.get(CouchbaseConstants.BUCKET)));
    }
  }

  @Test
  public void testSinkUpsert() throws Exception {
    // 'upsert' will always replace the document, ignoring whether the ID has already existed or not.
    // Store single empty document and test if it's content is replaced.
    TEST_RECORDS.stream()
      .findFirst()
      .map(r -> JsonDocument.create(r.<String>get("id"), JsonObject.empty()))
      .ifPresent(bucket::insert);

    Map<String, String> sinkProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, name.getMethodName())
      .put(CouchbaseConstants.NODES, BASE_PROPERTIES.get(CouchbaseConstants.NODES))
      .put(CouchbaseConstants.USERNAME, BASE_PROPERTIES.get(CouchbaseConstants.USERNAME))
      .put(CouchbaseConstants.PASSWORD, BASE_PROPERTIES.get(CouchbaseConstants.PASSWORD))
      .put(CouchbaseConstants.BUCKET, BASE_PROPERTIES.get(CouchbaseConstants.BUCKET))
      .put(CouchbaseConstants.KEY_FIELD, "id")
      .put(CouchbaseConstants.SCHEMA, SCHEMA.toString())
      .put(CouchbaseConstants.BATCH_SIZE, "100")
      .put(CouchbaseConstants.OPERATION, OperationType.UPSERT.name())
      .build();
    List<N1qlQueryRow> rows = getPipelineResults(sinkProperties, TEST_RECORDS);
    Assert.assertEquals(TEST_RECORDS.size(), rows.size());
    for (int i = 0; i < rows.size(); i++) {
      JsonObject row = rows.get(i).value();
      assertEquals(TEST_RECORDS.get(i), row.getObject(BASE_PROPERTIES.get(CouchbaseConstants.BUCKET)));
    }
  }

  private void assertEquals(StructuredRecord expected, JsonObject actual) {
    Assert.assertEquals(expected.get("created"), actual.getLong("created"));
    Assert.assertEquals(expected.get("string"), actual.getString("string"));
    Assert.assertEquals(expected.get("int32"), actual.getInt("int32"));
    Assert.assertEquals(expected.get("long"), actual.getLong("long"));
    Assert.assertEquals(expected.<Float>get("float"), actual.getDouble("float").floatValue(), 0.00001);
    Assert.assertEquals(expected.<Double>get("double"), actual.getDouble("double"), 0.00001);
    // In Couchbase SDK a decimal value will be represented as primitive if it's possible, so client will read 'decimal'
    // as instance of java.lang.Double
    Assert.assertEquals(expected.getDecimal("decimal").doubleValue(), actual.getDouble("decimal"), 0.00001);
    Assert.assertEquals(expected.get("boolean"), actual.getBoolean("boolean"));
    Assert.assertNull(actual.get("null"));
    Assert.assertEquals(expected.get("enum"), actual.getString("enum"));

    JsonArray stringArrayActual = actual.getArray("array");
    List<String> stringArrayExpected = expected.get("array");
    Assert.assertEquals(stringArrayExpected, stringArrayActual.toList());

    StructuredRecord innerObjectExpected = expected.get("object");
    JsonObject innerObjectActual = actual.getObject("object");
    Assert.assertEquals(innerObjectExpected.get("inner_field"), innerObjectActual.getString("inner_field"));

    Map<String, String> mapObjectExpected = expected.get("object-to-map");
    JsonObject mapObjectActual = actual.getObject("object-to-map");
    Assert.assertEquals(mapObjectExpected.get("key"), mapObjectActual.getString("key"));

    // bytes  will be transformed to base64 encoded string
    byte[] bytes = expected.get("binary");
    byte[] encoded = Base64.getEncoder().encode(bytes);
    String base64Encoded = new String(encoded);
    Assert.assertEquals(base64Encoded, actual.getString("binary"));

    Assert.assertEquals(expected.getTimestamp("date").toString(), actual.getString("date"));
    Assert.assertEquals(expected.getDate("date-field").toString(), actual.getString("date-field"));
    Assert.assertEquals(expected.getTime("time").toString(), actual.getString("time"));
    // If actual value is a record
    if (expected.get("union") instanceof StructuredRecord) {
      Assert.assertEquals(expected.<StructuredRecord>get("union").get("inner_field"),
                          actual.getObject("union").getString("inner_field"));
    }
    // If actual value is a string
    if (expected.get("union") instanceof String) {
      Assert.assertEquals(expected.get("union"), actual.getString("union"));
    }
  }

  private List<N1qlQueryRow> getPipelineResults(Map<String, String> sinkProperties,
                                                List<StructuredRecord> sourceRecords) throws Exception {
    String inputDatasetName = "input-batchsinktest-" + name.getMethodName();
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputDatasetName));
    ETLPlugin sinkPlugin = new ETLPlugin(CouchbaseConstants.PLUGIN_NAME, BatchSink.PLUGIN_TYPE, sinkProperties, null);
    ETLStage sink = new ETLStage("CouchbaseSink", sinkPlugin);
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("Couchbase_" + name.getMethodName());
    ApplicationManager appManager = deployApplication(appId, appRequest);

    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    MockSource.writeInput(inputManager, sourceRecords);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, 1, 5, TimeUnit.MINUTES);

    String query = String.format("SELECT * FROM `%s` ORDER BY created", BASE_PROPERTIES.get(CouchbaseConstants.BUCKET));
    return bucket.query(N1qlQuery.simple(query), COUCHBASE_TIMEOUT, TimeUnit.SECONDS).allRows();
  }
}
