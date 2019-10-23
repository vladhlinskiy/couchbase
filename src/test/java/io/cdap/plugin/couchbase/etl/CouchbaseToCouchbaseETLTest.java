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

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.couchbase.Consistency;
import io.cdap.plugin.couchbase.CouchbaseConstants;
import io.cdap.plugin.couchbase.ErrorHandling;
import io.cdap.plugin.couchbase.OperationType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class CouchbaseToCouchbaseETLTest extends BaseCouchbaseETLTest {

  private static final String SINK_BUCKET_NAME = "sink-test-bucket";
  private static final Schema INNER_OBJECT_SCHEMA = Schema.recordOf(
    "inner-object-schema",
    Schema.Field.of("key", Schema.of(Schema.Type.STRING)));

  private static final Schema SCHEMA = Schema.recordOf(
    "schema",
    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("created", Schema.of(Schema.Type.LONG)),
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
                          .put("created", System.nanoTime())
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
                          .put("number_array", JsonArray.create().add(Integer.MIN_VALUE).add(0).add(Integer.MAX_VALUE))
                          .put("object_array", JsonArray.create()
                            .add(JsonObject.create().put("key", "value1"))
                            .add(JsonObject.create().put("key", "value2"))
                            .add(JsonObject.create().put("key", "value3"))
                          )
    ),
    JsonDocument.create(UUID.randomUUID().toString(),
                        JsonObject.create()
                          .put("created", System.nanoTime())
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
                          .put("number_array", JsonArray.create().add(-1).add(0).add(1))
                          .put("object_array", JsonArray.create())
    ),
    JsonDocument.create(UUID.randomUUID().toString(),
                        JsonObject.create()
                          .put("created", System.nanoTime())
                          .put("boolean", false)
                          .put("number_double", 0d)
                          .put("number_int", 0)
                          .put("number_long", 0L)
                          .put("number_decimal", new BigDecimal("0.00"))
                          .put("number_big_int",
                               BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.valueOf(Long.MAX_VALUE)))
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

  private static Bucket sinkBucket;

  @BeforeClass
  public static void prepareTestData() throws Exception {
    TEST_DOCUMENTS.forEach(bucket::insert);
    sinkBucket = createOrReplaceBucket(SINK_BUCKET_NAME);
  }

  @Test
  public void testCouchbaseSourceToCouchbaseSink() throws Exception {
    String sourceBucketName = BASE_PROPERTIES.get(CouchbaseConstants.BUCKET);
    ETLStage source = new ETLStage("CouchbaseSource", new ETLPlugin(
      CouchbaseConstants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      new ImmutableMap.Builder<String, String>()
        .put(Constants.Reference.REFERENCE_NAME, name.getMethodName())
        .put(CouchbaseConstants.NODES, BASE_PROPERTIES.get(CouchbaseConstants.NODES))
        .put(CouchbaseConstants.USERNAME, BASE_PROPERTIES.get(CouchbaseConstants.USERNAME))
        .put(CouchbaseConstants.PASSWORD, BASE_PROPERTIES.get(CouchbaseConstants.PASSWORD))
        .put(CouchbaseConstants.BUCKET, sourceBucketName)
        .put(CouchbaseConstants.ON_ERROR, ErrorHandling.FAIL_PIPELINE.getDisplayName())
        .put(CouchbaseConstants.SCHEMA, SCHEMA.toString())
        .put(CouchbaseConstants.SELECT_FIELDS, String.format("meta(`%s`).id, *", sourceBucketName))
        .put(CouchbaseConstants.CONDITIONS, "")
        .put(CouchbaseConstants.SAMPLE_SIZE, "1000")
        .put(CouchbaseConstants.NUM_SPLITS, "0")
        .put(CouchbaseConstants.MAX_PARALLELISM, "0")
        .put(CouchbaseConstants.SCAN_CONSISTENCY, Consistency.NOT_BOUNDED.getDisplayName())
        .put(CouchbaseConstants.QUERY_TIMEOUT, "600")
        .build(),
      null));

    ETLStage sink = new ETLStage("CouchbaseSink", new ETLPlugin(
      CouchbaseConstants.PLUGIN_NAME,
      BatchSink.PLUGIN_TYPE,
      new ImmutableMap.Builder<String, String>()
        .put(Constants.Reference.REFERENCE_NAME, name.getMethodName())
        .put(CouchbaseConstants.NODES, BASE_PROPERTIES.get(CouchbaseConstants.NODES))
        .put(CouchbaseConstants.USERNAME, BASE_PROPERTIES.get(CouchbaseConstants.USERNAME))
        .put(CouchbaseConstants.PASSWORD, BASE_PROPERTIES.get(CouchbaseConstants.PASSWORD))
        .put(CouchbaseConstants.BUCKET, SINK_BUCKET_NAME)
        .put(CouchbaseConstants.KEY_FIELD, "id")
        .put(CouchbaseConstants.SCHEMA, SCHEMA.toString())
        .put(CouchbaseConstants.BATCH_SIZE, "1")
        .put(CouchbaseConstants.OPERATION, OperationType.INSERT.name())
        .build(),
      null));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("Couchbase_" + name.getMethodName());
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, 1, 5, TimeUnit.MINUTES);

    String query = String.format("SELECT * FROM `%s` ORDER BY created", SINK_BUCKET_NAME);
    List<N1qlQueryRow> rows = sinkBucket.query(N1qlQuery.simple(query), COUCHBASE_TIMEOUT, TimeUnit.SECONDS).allRows();
    Assert.assertEquals(TEST_DOCUMENTS.size(), rows.size());
    for (int i = 0; i < rows.size(); i++) {
      JsonObject expected = TEST_DOCUMENTS.get(i).content();
      JsonObject row = rows.get(i).value();
      JsonObject actual = row.getObject(SINK_BUCKET_NAME);
      assertEquals(expected, actual);
    }
  }

  private void assertEquals(JsonObject expected, JsonObject actual) {
    Assert.assertEquals(expected.getBoolean("boolean"), actual.getBoolean("boolean"));
    Assert.assertEquals(expected.getDouble("number_double"), actual.getDouble("number_double"));
    Assert.assertEquals(expected.getInt("number_int"), actual.getInt("number_int"));
    Assert.assertEquals(expected.getLong("number_long"), actual.getLong("number_long"));
    // In Couchbase SDK a decimal value will be represented as primitive if it's possible, so client will read 'decimal'
    // as instance of java.lang.Double
    Assert.assertEquals(expected.getDouble("number_decimal"), actual.getDouble("number_decimal"), 0.00001);
    Assert.assertEquals(expected.getBigInteger("number_big_int"), actual.getBigInteger("number_big_int"));
    Assert.assertEquals(expected.getString("string"), actual.<String>get("string"));

    Assert.assertEquals(expected.getObject("object"), actual.getObject("object"));
    Assert.assertEquals(expected.getObject("object_map"), actual.getObject("object_map"));

    Assert.assertNull(actual.get("null"));
    Assert.assertEquals(expected.getString("date_as_string"), actual.getString("date_as_string"));

    Assert.assertEquals(expected.getArray("boolean_array"), actual.getArray("boolean_array"));
    Assert.assertEquals(expected.getArray("number_array"), actual.getArray("number_array"));
    Assert.assertEquals(expected.getArray("object_array"), actual.getArray("object_array"));
  }

  @AfterClass
  public static void closeSinkBucket() throws Exception {
    sinkBucket.close();
  }
}
