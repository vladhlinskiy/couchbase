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
import io.cdap.plugin.couchbase.CouchbaseConstants;
import io.cdap.plugin.couchbase.ErrorHandling;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class CouchbaseSourceETLTest extends BaseCouchbaseETLTest {

  private static final Schema INNER_OBJECT_SCHEMA = Schema.recordOf(
    "inner-object-schema",
    Schema.Field.of("key", Schema.of(Schema.Type.STRING)));

  private static final Schema SCHEMA = Schema.recordOf(
    "schema",
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
                          .put("number_array", JsonArray.create().add(Long.MIN_VALUE).add(0L).add(Long.MAX_VALUE))
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
                          .put("number_array", JsonArray.create().add(Long.MIN_VALUE).add(0L).add(Long.MAX_VALUE))
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

  @BeforeClass
  public static void prepareTestData() throws Exception {
    TEST_DOCUMENTS.forEach(bucket::insert);
  }

  @Test
  public void testSource() throws Exception {
    String query = String.format("SELECT * from `%s` ORDER BY created", BASE_PROPERTIES.get(CouchbaseConstants.BUCKET));
    Map<String, String> properties = sourceProperties(query, SCHEMA);
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(TEST_DOCUMENTS.size(), records.size());
    for (int i = 0; i < TEST_DOCUMENTS.size(); i++) {
      assertEquals(TEST_DOCUMENTS.get(i), records.get(i));
    }
  }

  @Test
  public void testSourceSingleField() throws Exception {
    String bucketName = BASE_PROPERTIES.get(CouchbaseConstants.BUCKET);
    String query = String.format("SELECT created from `%s` ORDER BY created", bucketName);
    Schema schema = Schema.recordOf("inner-object-schema", Schema.Field.of("created", Schema.of(Schema.Type.LONG)));
    Map<String, String> properties = sourceProperties(query, schema);
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(TEST_DOCUMENTS.size(), records.size());
    for (int i = 0; i < TEST_DOCUMENTS.size(); i++) {
      JsonObject content = TEST_DOCUMENTS.get(i).content();
      StructuredRecord actual = records.get(i);
      Assert.assertEquals(content.getLong("created"), actual.<Long>get("created"));
    }
  }

  @Test
  public void testSourceIncludeId() throws Exception {
    String bucketName = BASE_PROPERTIES.get(CouchbaseConstants.BUCKET);
    String query = String.format("SELECT meta(`%s`).id, * from `%s` ORDER BY created", bucketName, bucketName);
    Schema schemaWithIdIncluded = Schema.recordOf(
      "schema",
      ImmutableList.<Schema.Field>builder()
        .add(Schema.Field.of("id", Schema.of(Schema.Type.STRING)))
        .addAll(SCHEMA.getFields())
        .build()
    );
    Map<String, String> properties = sourceProperties(query, schemaWithIdIncluded);
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(TEST_DOCUMENTS.size(), records.size());
    for (int i = 0; i < TEST_DOCUMENTS.size(); i++) {
      JsonDocument expected = TEST_DOCUMENTS.get(i);
      StructuredRecord actual = records.get(i);
      Assert.assertEquals(expected.id(), actual.get("id"));
      assertEquals(expected, actual);
    }
  }

  private void assertEquals(JsonDocument expected, StructuredRecord actual) {
    JsonObject content = expected.content();
    Assert.assertEquals(content.getBoolean("boolean"), actual.<Boolean>get("boolean"));
    Assert.assertEquals(content.getDouble("number_double"), actual.<Double>get("number_double"));
    Assert.assertEquals(content.getInt("number_int"), actual.<Integer>get("number_int"));
    Assert.assertEquals(content.getLong("number_long"), actual.<Long>get("number_long"));
    Assert.assertEquals(content.getBigDecimal("number_decimal"), actual.getDecimal("number_decimal"));
    Assert.assertEquals(content.getBigInteger("number_big_int"), actual.getDecimal("number_big_int").unscaledValue());
    Assert.assertEquals(content.getString("string"), actual.<String>get("string"));

    JsonObject innerObjectExpected = content.getObject("object");
    StructuredRecord innerObjectActual = actual.get("object");
    Assert.assertEquals(innerObjectExpected.getString("key"), innerObjectActual.<String>get("key"));

    JsonObject innerObjectMapExpected = content.getObject("object_map");
    Map<String, String> innerObjectMapActual = actual.get("object_map");
    Assert.assertEquals(innerObjectMapExpected.getString("key"), innerObjectMapActual.get("key"));

    Assert.assertNull(actual.get("null"));
    Assert.assertEquals(content.getString("date_as_string"), actual.<String>get("date_as_string"));

    JsonArray booleanArrayExpected = content.getArray("boolean_array");
    List<Boolean> booleanArrayActual = actual.get("boolean_array");
    Assert.assertEquals(booleanArrayExpected.toList(), booleanArrayActual);

    JsonArray numberArrayExpected = content.getArray("number_array");
    List<Number> numberArrayActual = actual.get("number_array");
    Assert.assertEquals(numberArrayExpected.toList(), numberArrayActual);

    JsonArray objectArrayExpected = content.getArray("object_array");
    List<StructuredRecord> objectArrayActual = actual.get("object_array");
    for (int i = 0; i < objectArrayExpected.size(); i++) {
      JsonObject itemExpected = objectArrayExpected.getObject(i);
      StructuredRecord itemActual = objectArrayActual.get(i);
      Assert.assertEquals(itemExpected.getString("key"), itemActual.<String>get("key"));
    }
  }

  private Map<String, String> sourceProperties(String query, Schema schema) {
    return new ImmutableMap.Builder<String, String>()
      .put(CouchbaseConstants.NODES, BASE_PROPERTIES.get(CouchbaseConstants.NODES))
      .put(CouchbaseConstants.USERNAME, BASE_PROPERTIES.get(CouchbaseConstants.USERNAME))
      .put(CouchbaseConstants.PASSWORD, BASE_PROPERTIES.get(CouchbaseConstants.PASSWORD))
      .put(CouchbaseConstants.BUCKET, BASE_PROPERTIES.get(CouchbaseConstants.BUCKET))
      .put(CouchbaseConstants.ON_ERROR, ErrorHandling.FAIL_PIPELINE.getDisplayName())
      .put(CouchbaseConstants.SCHEMA, schema.toString())
      .put(CouchbaseConstants.QUERY, query)
      .build();
  }
}
