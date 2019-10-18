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
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * {@link RecordToJsonDocumentTransformer} test.
 */
public class RecordToJsonDocumentTransformerTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testTransform() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("int_field", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                                    Schema.Field.of("long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                                    Schema.Field.of("double_field", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                                    Schema.Field.of("float_field", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
                                    Schema.Field.of("string_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("boolean_field", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                                    Schema.Field.of("bytes_field", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
                                    Schema.Field.of("null_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("array_field",
                                                    Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.LONG)))));

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("id", UUID.randomUUID().toString())
      .set("int_field", 15)
      .set("long_field", 10L)
      .set("double_field", 10.5D)
      .set("float_field", 15.5F)
      .set("string_field", "string_value")
      .set("boolean_field", true)
      .set("bytes_field", "test_blob".getBytes())
      .set("null_field", null)
      .set("array_field", Arrays.asList(1L, null, 2L, null, 3L))
      .build();

    RecordToJsonDocumentTransformer transformer = new RecordToJsonDocumentTransformer("id");
    JsonDocument jsonDocument = transformer.transform(inputRecord);
    JsonObject jsonObject = jsonDocument.content();

    Assert.assertEquals(inputRecord.get("int_field"), jsonObject.get("int_field"));
    Assert.assertEquals(inputRecord.get("long_field"), jsonObject.get("long_field"));
    Assert.assertEquals(inputRecord.get("double_field"), jsonObject.get("double_field"));
    Assert.assertEquals(inputRecord.<Float>get("float_field").doubleValue(), jsonObject.get("float_field"));
    Assert.assertEquals(inputRecord.get("string_field"), jsonObject.get("string_field"));
    Assert.assertEquals(inputRecord.get("boolean_field"), jsonObject.get("boolean_field"));
    Assert.assertNull(jsonObject.get("null_field"));
    Assert.assertEquals(inputRecord.get("array_field"), jsonObject.getArray("array_field").toList());

    // bytes  will be transformed to base64 encoded string
    Assert.assertTrue(jsonObject.get("bytes_field") instanceof String);
    byte[] encoded = jsonObject.getString("bytes_field").getBytes();
    byte[] decoded = Base64.getDecoder().decode(encoded);
    Assert.assertEquals("test_blob", new String(decoded));
  }

  @Test
  public void testTransformNestedMapsSimpleTypes() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("nested_string_maps", Schema.mapOf(
                                      Schema.of(Schema.Type.STRING), Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                                                  Schema.of(Schema.Type.STRING)))),
                                    Schema.Field.of("nested_int_maps", Schema.mapOf(
                                      Schema.of(Schema.Type.STRING), Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                                                  Schema.of(Schema.Type.INT)))),
                                    Schema.Field.of("nested_boolean_maps", Schema.mapOf(
                                      Schema.of(Schema.Type.STRING), Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                                                  Schema.of(Schema.Type.BOOLEAN))))
    );

    Map<String, Map<String, String>> stringMap = ImmutableMap.<String, Map<String, String>>builder()
      .put("nested_map1", ImmutableMap.<String, String>builder().put("k1", "v1").build())
      .put("nested_map2", ImmutableMap.<String, String>builder().put("k2", "v2").build())
      .put("nested_map3", ImmutableMap.<String, String>builder().put("k3", "v3").build())
      .build();

    Map<String, Map<String, Integer>> intMap = ImmutableMap.<String, Map<String, Integer>>builder()
      .put("nested_map1", ImmutableMap.<String, Integer>builder().put("k1", 1).build())
      .put("nested_map2", ImmutableMap.<String, Integer>builder().put("k2", 2).build())
      .put("nested_map3", ImmutableMap.<String, Integer>builder().put("k3", 3).build())
      .build();

    Map<String, Map<String, Boolean>> booleanMap = ImmutableMap.<String, Map<String, Boolean>>builder()
      .put("nested_map1", ImmutableMap.<String, Boolean>builder().put("k1", false).build())
      .put("nested_map2", ImmutableMap.<String, Boolean>builder().put("k2", true).build())
      .put("nested_map3", ImmutableMap.<String, Boolean>builder().put("k3", false).build())
      .build();

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("id", UUID.randomUUID().toString())
      .set("nested_string_maps", stringMap)
      .set("nested_int_maps", intMap)
      .set("nested_boolean_maps", booleanMap)
      .build();

    RecordToJsonDocumentTransformer transformer = new RecordToJsonDocumentTransformer("id");
    JsonDocument jsonDocument = transformer.transform(inputRecord);
    JsonObject jsonObject = jsonDocument.content();

    Assert.assertNotNull(jsonObject.get("nested_string_maps"));
    JsonObject actualStringMap = jsonObject.getObject("nested_string_maps");
    Assert.assertTrue(actualStringMap.containsKey("nested_map1"));
    Assert.assertTrue(actualStringMap.containsKey("nested_map2"));
    Assert.assertTrue(actualStringMap.containsKey("nested_map3"));

    // TODO avoid duplication
    JsonObject stringMap1 = actualStringMap.getObject("nested_map1");
    Assert.assertEquals(stringMap.get("nested_map1").get("k1"), stringMap1.get("k1"));
    JsonObject stringMap2 = actualStringMap.getObject("nested_map2");
    Assert.assertEquals(stringMap.get("nested_map2").get("k2"), stringMap2.get("k2"));
    JsonObject stringMap3 = actualStringMap.getObject("nested_map3");
    Assert.assertEquals(stringMap.get("nested_map3").get("k3"), stringMap3.get("k3"));

    Assert.assertNotNull(jsonObject.get("nested_int_maps"));
    JsonObject actualIntMap = jsonObject.getObject("nested_int_maps");
    Assert.assertTrue(actualIntMap.containsKey("nested_map1"));
    Assert.assertTrue(actualIntMap.containsKey("nested_map2"));
    Assert.assertTrue(actualIntMap.containsKey("nested_map3"));

    JsonObject intMap1 = actualIntMap.getObject("nested_map1");
    Assert.assertEquals(intMap.get("nested_map1").get("k1"), intMap1.get("k1"));
    JsonObject intMap2 = actualIntMap.getObject("nested_map2");
    Assert.assertEquals(intMap.get("nested_map2").get("k2"), intMap2.get("k2"));
    JsonObject intMap3 = actualIntMap.getObject("nested_map3");
    Assert.assertEquals(intMap.get("nested_map3").get("k3"), intMap3.get("k3"));

    Assert.assertNotNull(jsonObject.get("nested_boolean_maps"));
    JsonObject actualBooleanMap = jsonObject.getObject("nested_boolean_maps");
    Assert.assertTrue(actualBooleanMap.containsKey("nested_map1"));
    Assert.assertTrue(actualBooleanMap.containsKey("nested_map2"));
    Assert.assertTrue(actualBooleanMap.containsKey("nested_map3"));

    JsonObject booleanMap1 = actualBooleanMap.getObject("nested_map1");
    Assert.assertEquals(booleanMap.get("nested_map1").get("k1"), booleanMap1.get("k1"));
    JsonObject booleanMap2 = actualBooleanMap.getObject("nested_map2");
    Assert.assertEquals(booleanMap.get("nested_map2").get("k2"), booleanMap2.get("k2"));
    JsonObject booleanMap3 = actualBooleanMap.getObject("nested_map3");
    Assert.assertEquals(booleanMap.get("nested_map3").get("k3"), booleanMap3.get("k3"));
  }

  @Test
  public void testTransformComplexNestedMaps() {

    Schema nestedRecordSchema = Schema.recordOf("nested_object",
                                                Schema.Field.of("nested_string_field", Schema.of(Schema.Type.STRING)),
                                                Schema.Field.of("nested_decimal_field", Schema.decimalOf(4, 2)));

    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("map_field", Schema.nullableOf(
                                      Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                   Schema.mapOf(Schema.of(Schema.Type.STRING), nestedRecordSchema)))));

    StructuredRecord nestedRecord1 = StructuredRecord.builder(nestedRecordSchema)
      .set("nested_string_field", "some value")
      .setDecimal("nested_decimal_field", new BigDecimal("12.34"))
      .build();

    StructuredRecord nestedRecord2 = StructuredRecord.builder(nestedRecordSchema)
      .set("nested_string_field", "some value")
      .setDecimal("nested_decimal_field", new BigDecimal("10.00"))
      .build();

    StructuredRecord nestedRecord3 = StructuredRecord.builder(nestedRecordSchema)
      .set("nested_string_field", "some value")
      .setDecimal("nested_decimal_field", new BigDecimal("10.01"))
      .build();

    Map<String, Map<String, StructuredRecord>> map = ImmutableMap.<String, Map<String, StructuredRecord>>builder()
      .put("nested_map1", ImmutableMap.<String, StructuredRecord>builder().put("k1", nestedRecord1).build())
      .put("nested_map2", ImmutableMap.<String, StructuredRecord>builder().put("k2", nestedRecord2).build())
      .put("nested_map3", ImmutableMap.<String, StructuredRecord>builder().put("k3", nestedRecord3).build())
      .build();

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("id", UUID.randomUUID().toString())
      .set("map_field", map)
      .build();

    RecordToJsonDocumentTransformer transformer = new RecordToJsonDocumentTransformer("id");
    JsonDocument jsonDocument = transformer.transform(inputRecord);
    JsonObject jsonObject = jsonDocument.content();

    JsonObject actualNestedMap1 = jsonObject.getObject("map_field").getObject("nested_map1");
    Assert.assertTrue(actualNestedMap1.containsKey("k1"));
    Assert.assertEquals(nestedRecord1.getDecimal("nested_decimal_field"),
                        actualNestedMap1.getObject("k1").getBigDecimal("nested_decimal_field"));

    JsonObject actualNestedMap2 = jsonObject.getObject("map_field").getObject("nested_map2");
    Assert.assertTrue(actualNestedMap2.containsKey("k2"));
    Assert.assertEquals(nestedRecord2.getDecimal("nested_decimal_field"),
                        actualNestedMap2.getObject("k2").getBigDecimal("nested_decimal_field"));

    JsonObject actualNestedMap3 = jsonObject.getObject("map_field").getObject("nested_map3");
    Assert.assertTrue(actualNestedMap3.containsKey("k3"));
    Assert.assertEquals(nestedRecord3.getDecimal("nested_decimal_field"),
                        actualNestedMap3.getObject("k3").getBigDecimal("nested_decimal_field"));
  }

  @Test
  public void testTransformNestedArraysSimpleTypes() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("nested_string_array", Schema.arrayOf(
                                      Schema.arrayOf(Schema.of(Schema.Type.STRING)))),
                                    Schema.Field.of("nested_int_array", Schema.arrayOf(
                                      Schema.arrayOf(Schema.of(Schema.Type.INT)))),
                                    Schema.Field.of("nested_boolean_array", Schema.arrayOf(
                                      Schema.arrayOf(Schema.of(Schema.Type.BOOLEAN))))
    );

    List<List<String>> stringArray = Arrays.asList(
      Arrays.asList("1", "2", "3"),
      Arrays.asList("1", "2", "3"),
      Arrays.asList("1", "2", "3")
    );

    List<List<Integer>> intArray = Arrays.asList(
      Arrays.asList(1, 2, 3),
      Arrays.asList(1, 2, 3),
      Arrays.asList(1, 2, 3)
    );

    List<List<Boolean>> bytesArray = Arrays.asList(
      Arrays.asList(true, false, false),
      Arrays.asList(false, true, false),
      Arrays.asList(false, false, true)
    );

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("id", UUID.randomUUID().toString())
      .set("nested_string_array", stringArray)
      .set("nested_int_array", intArray)
      .set("nested_boolean_array", bytesArray)
      .build();

    RecordToJsonDocumentTransformer transformer = new RecordToJsonDocumentTransformer("id");
    JsonDocument jsonDocument = transformer.transform(inputRecord);
    JsonObject jsonObject = jsonDocument.content();

    Assert.assertEquals(inputRecord.get("nested_string_array"), jsonObject.getArray("nested_string_array").toList());
    Assert.assertEquals(inputRecord.get("nested_int_array"), jsonObject.getArray("nested_int_array").toList());
    Assert.assertEquals(inputRecord.get("nested_boolean_array"), jsonObject.getArray("nested_boolean_array").toList());
  }

  @Test
  public void testTransformComplexNestedArrays() {

    Schema nestedRecordSchema = Schema.recordOf("nested_object",
                                                Schema.Field.of("nested_string_field", Schema.of(Schema.Type.STRING)),
                                                Schema.Field.of("nested_decimal_field", Schema.decimalOf(4, 2)));
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("nested_array_of_maps", Schema.arrayOf(
                                      Schema.arrayOf(Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                                  Schema.of(Schema.Type.STRING))))),
                                    Schema.Field.of("nested_array_of_records", Schema.arrayOf(
                                      Schema.arrayOf(nestedRecordSchema)))
    );

    List<List<Map<String, String>>> arrayOfMaps = Arrays.asList(
      Collections.singletonList(ImmutableMap.<String, String>builder().put("k1", "v1").build()),
      Collections.singletonList(ImmutableMap.<String, String>builder().put("k2", "v2").build()),
      Collections.singletonList(ImmutableMap.<String, String>builder().put("k3", "v3").build())
    );

    List<List<StructuredRecord>> arrayOfRecords = Arrays.asList(
      Collections.singletonList(
        StructuredRecord.builder(nestedRecordSchema)
          .set("nested_string_field", "some value")
          .setDecimal("nested_decimal_field", new BigDecimal("12.34"))
          .build()
      ),

      Collections.singletonList(
        StructuredRecord.builder(nestedRecordSchema)
          .set("nested_string_field", "some value")
          .setDecimal("nested_decimal_field", new BigDecimal("10.00"))
          .build()
      ),

      Collections.singletonList(
        StructuredRecord.builder(nestedRecordSchema)
          .set("nested_string_field", "some value")
          .setDecimal("nested_decimal_field", new BigDecimal("10.01"))
          .build()
      )
    );

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("id", UUID.randomUUID().toString())
      .set("nested_array_of_maps", arrayOfMaps)
      .set("nested_array_of_records", arrayOfRecords)
      .build();

    RecordToJsonDocumentTransformer transformer = new RecordToJsonDocumentTransformer("id");
    JsonDocument jsonDocument = transformer.transform(inputRecord);
    JsonObject jsonObject = jsonDocument.content();

    JsonArray actualArrayOfMaps = jsonObject.getArray("nested_array_of_maps");
    Assert.assertNotNull(actualArrayOfMaps);
    Assert.assertEquals(arrayOfMaps.size(), actualArrayOfMaps.size());
    Assert.assertEquals(arrayOfMaps.get(0).get(0).get("k1"),
                        actualArrayOfMaps.getArray(0).getObject(0).getString("k1"));
    Assert.assertEquals(arrayOfMaps.get(1).get(0).get("k2"),
                        actualArrayOfMaps.getArray(1).getObject(0).getString("k2"));
    Assert.assertEquals(arrayOfMaps.get(2).get(0).get("k3"),
                        actualArrayOfMaps.getArray(2).getObject(0).getString("k3"));


    JsonArray actualArrayOfRecords = jsonObject.getArray("nested_array_of_records");
    Assert.assertNotNull(actualArrayOfRecords);
    Assert.assertEquals(arrayOfRecords.size(), actualArrayOfRecords.size());
    Assert.assertEquals(arrayOfRecords.get(0).get(0).getDecimal("nested_decimal_field"),
                        actualArrayOfRecords.getArray(0).getObject(0).getBigDecimal("nested_decimal_field"));
    Assert.assertEquals(arrayOfRecords.get(1).get(0).getDecimal("nested_decimal_field"),
                        actualArrayOfRecords.getArray(1).getObject(0).getBigDecimal("nested_decimal_field"));
    Assert.assertEquals(arrayOfRecords.get(2).get(0).getDecimal("nested_decimal_field"),
                        actualArrayOfRecords.getArray(2).getObject(0).getBigDecimal("nested_decimal_field"));
  }

  @Test
  public void testTransformUnionBytes() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("union_field", Schema.unionOf(
                                      Schema.of(Schema.Type.STRING),
                                      Schema.of(Schema.Type.BYTES)
                                    )));


    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("id", UUID.randomUUID().toString())
      .set("union_field", ByteBuffer.wrap("bytes".getBytes()))
      .build();

    RecordToJsonDocumentTransformer transformer = new RecordToJsonDocumentTransformer("id");
    JsonDocument jsonDocument = transformer.transform(inputRecord);
    JsonObject content = jsonDocument.content();

    // byte array will be transformed to base64 encoded string
    Assert.assertTrue(content.get("union_field") instanceof String);
    byte[] encoded = content.getString("union_field").getBytes();
    byte[] decoded = Base64.getDecoder().decode(encoded);
    Assert.assertEquals("bytes", new String(decoded));
  }

  @Test
  public void testTransformUnionRecord() {
    Schema recordSchema = Schema.recordOf("nested", Schema.Field.of("bytes_field", Schema.of(Schema.Type.BYTES)));
    Schema schema = Schema.recordOf(
      "schema",
      Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("union_field", Schema.unionOf(Schema.of(Schema.Type.STRING), recordSchema)));

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("id", UUID.randomUUID().toString())
      .set("union_field", StructuredRecord.builder(recordSchema)
        .set("bytes_field", ByteBuffer.wrap("bytes".getBytes()))
        .build())
      .build();

    RecordToJsonDocumentTransformer transformer = new RecordToJsonDocumentTransformer("id");
    JsonDocument jsonDocument = transformer.transform(inputRecord);
    JsonObject jsonObject = jsonDocument.content();

    Assert.assertTrue(jsonObject.containsKey("union_field"));
    // Record must be transformed to JsonObject
    Assert.assertTrue(jsonObject.get("union_field") instanceof JsonObject);

    // ByteBuffer will be transformed to base64 encoded string
    JsonObject unionObject = jsonObject.getObject("union_field");
    Assert.assertTrue(unionObject.get("bytes_field") instanceof String);
    byte[] encoded = unionObject.getString("bytes_field").getBytes();
    byte[] decoded = Base64.getDecoder().decode(encoded);
    Assert.assertEquals("bytes", new String(decoded));
  }

  @Test
  public void testTransformEmpty() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("int_field", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                                    Schema.Field.of("long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                                    Schema.Field.of("double_field", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                                    Schema.Field.of("float_field", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
                                    Schema.Field.of("string_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("boolean_field", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                                    Schema.Field.of("bytes_field", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
                                    Schema.Field.of("null_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("decimal", Schema.nullableOf(Schema.decimalOf(6, 4))),
                                    Schema.Field.of("date", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
                                    Schema.Field.of("array_field",
                                                    Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.LONG)))),
                                    Schema.Field.of("timestamp_millis",
                                                    Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS))),
                                    Schema.Field.of("timestamp_micros",
                                                    Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
                                    Schema.Field.of("time_millis",
                                                    Schema.nullableOf(Schema.of(Schema.LogicalType.TIME_MILLIS))),
                                    Schema.Field.of("time_micros",
                                                    Schema.nullableOf(Schema.of(Schema.LogicalType.TIME_MILLIS)))
    );

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("id", UUID.randomUUID().toString())
      .build();

    RecordToJsonDocumentTransformer transformer = new RecordToJsonDocumentTransformer("id");
    JsonDocument jsonDocument = transformer.transform(inputRecord);
    JsonObject jsonObject = jsonDocument.content();

    Assert.assertNotNull(jsonObject);
    Assert.assertNull(inputRecord.get("int_field"));
    Assert.assertNull(inputRecord.get("long_field"));
    Assert.assertNull(inputRecord.get("double_field"));
    Assert.assertNull(inputRecord.get("float_field"));
    Assert.assertNull(inputRecord.get("string_field"));
    Assert.assertNull(inputRecord.get("boolean_field"));
    Assert.assertNull(inputRecord.get("bytes_field"));
    Assert.assertNull(jsonObject.get("null_field"));
    Assert.assertNull(inputRecord.get("array_field"));
    Assert.assertNull(inputRecord.get("decimal"));
    Assert.assertNull(inputRecord.get("timestamp_millis"));
    Assert.assertNull(inputRecord.get("timestamp_micros"));
    Assert.assertNull(inputRecord.get("time_millis"));
    Assert.assertNull(inputRecord.get("time_micros"));
    Assert.assertNull(inputRecord.get("date"));
  }


  @Test
  public void testTransformInvalidIdField() {
    Schema schema = Schema.recordOf("schema", Schema.Field.of("invalid_id_field", Schema.of(Schema.Type.STRING)));
    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("invalid_id_field", "") // Identifier must be a non-empty string
      .build();

    thrown.expect(UnexpectedFormatException.class);
    new RecordToJsonDocumentTransformer("invalid_id_field").transform(inputRecord);
  }

  @Test
  public void testTransformArrays() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("list_field", Schema.arrayOf(Schema.of(Schema.Type.LONG))),
                                    Schema.Field.of("set_field", Schema.arrayOf(Schema.of(Schema.Type.LONG))),
                                    Schema.Field.of("array_field", Schema.arrayOf(Schema.of(Schema.Type.LONG))));

    List<Long> expected = Arrays.asList(1L, 2L, null, 3L);
    Set<Long> expectedSet = new HashSet<>(expected);
    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("id", UUID.randomUUID().toString())
      .set("set_field", expectedSet)
      .set("list_field", expected)
      .set("array_field", expected.toArray())
      .build();

    RecordToJsonDocumentTransformer transformer = new RecordToJsonDocumentTransformer("id");
    JsonDocument jsonDocument = transformer.transform(inputRecord);
    JsonObject jsonObject = jsonDocument.content();
    Assert.assertNotNull(jsonObject);
    Assert.assertEquals(expectedSet, new HashSet<>(jsonObject.getArray("set_field").toList()));
    Assert.assertEquals(expected, jsonObject.getArray("list_field").toList());
    Assert.assertEquals(expected, jsonObject.getArray("array_field").toList());
  }

  @Test
  public void testTransformLogicalTypes() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("decimal", Schema.decimalOf(4, 2)),
                                    Schema.Field.of("timestamp_millis", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
                                    Schema.Field.of("timestamp_micros", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                                    Schema.Field.of("time_millis", Schema.of(Schema.LogicalType.TIME_MILLIS)),
                                    Schema.Field.of("time_micros", Schema.of(Schema.LogicalType.TIME_MICROS)),
                                    Schema.Field.of("date", Schema.of(Schema.LogicalType.DATE)));

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("id", UUID.randomUUID().toString())
      .setDecimal("decimal", new BigDecimal("10.01"))
      .setTimestamp("timestamp_millis", ZonedDateTime.now(ZoneOffset.UTC))
      .setTimestamp("timestamp_micros", ZonedDateTime.now(ZoneOffset.UTC))
      .setTime("time_millis", LocalTime.now(ZoneOffset.UTC))
      .setTime("time_micros", LocalTime.now(ZoneOffset.UTC))
      .setDate("date", LocalDate.now(ZoneOffset.UTC))
      .build();

    RecordToJsonDocumentTransformer transformer = new RecordToJsonDocumentTransformer("id");
    JsonDocument jsonDocument = transformer.transform(inputRecord);
    JsonObject jsonObject = jsonDocument.content();
    Assert.assertNotNull(jsonObject);
    Assert.assertEquals(inputRecord.getDecimal("decimal"), jsonObject.getBigDecimal("decimal"));
    Assert.assertEquals(inputRecord.getTimestamp("timestamp_millis").toString(),
                        jsonObject.getString("timestamp_millis"));
    Assert.assertEquals(inputRecord.getTimestamp("timestamp_micros").toString(),
                        jsonObject.getString("timestamp_micros"));
    Assert.assertEquals(inputRecord.getTime("time_millis").toString(), jsonObject.getString("time_millis"));
    Assert.assertEquals(inputRecord.getTime("time_micros").toString(), jsonObject.getString("time_micros"));
    Assert.assertEquals(inputRecord.getDate("date").toString(), jsonObject.getString("date"));
  }

  @Test
  public void testTransformNestedLogicalTypes() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("decimal_array", Schema.arrayOf(Schema.decimalOf(4, 2))),
                                    Schema.Field.of("timestamp_millis_array",
                                                    Schema.arrayOf(Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS))),
                                    Schema.Field.of("timestamp_micros_array",
                                                    Schema.arrayOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
                                    Schema.Field.of("time_millis_array",
                                                    Schema.arrayOf(Schema.of(Schema.LogicalType.TIME_MILLIS))),
                                    Schema.Field.of("time_micros_array",
                                                    Schema.arrayOf(Schema.of(Schema.LogicalType.TIME_MICROS))),
                                    Schema.Field.of("date_array",
                                                    Schema.arrayOf(Schema.of(Schema.LogicalType.DATE)))
    );

    // Set values of logical types on temporary record to get their physical representation
    Schema tmpSchema = Schema.recordOf("schema",
                                       Schema.Field.of("decimal", Schema.decimalOf(4, 2)),
                                       Schema.Field.of("timestamp_millis",
                                                       Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
                                       Schema.Field.of("timestamp_micros",
                                                       Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                                       Schema.Field.of("time_millis", Schema.of(Schema.LogicalType.TIME_MILLIS)),
                                       Schema.Field.of("time_micros", Schema.of(Schema.LogicalType.TIME_MICROS)),
                                       Schema.Field.of("date", Schema.of(Schema.LogicalType.DATE)));

    StructuredRecord tmpRecord = StructuredRecord.builder(tmpSchema)
      .setDecimal("decimal", new BigDecimal("10.01"))
      .setTimestamp("timestamp_millis", ZonedDateTime.now(ZoneOffset.UTC))
      .setTimestamp("timestamp_micros", ZonedDateTime.now(ZoneOffset.UTC))
      .setTime("time_millis", LocalTime.now(ZoneOffset.UTC))
      .setTime("time_micros", LocalTime.now(ZoneOffset.UTC))
      .setDate("date", LocalDate.now(ZoneOffset.UTC))
      .build();

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("id", UUID.randomUUID().toString())
      .set("decimal_array", new byte[][]{tmpRecord.get("decimal")})
      .set("timestamp_millis_array", Collections.singleton(tmpRecord.get("timestamp_millis")))
      .set("timestamp_micros_array", Collections.singleton(tmpRecord.get("timestamp_micros")))
      .set("time_millis_array", Collections.singleton(tmpRecord.get("time_millis")))
      .set("time_micros_array", Collections.singleton(tmpRecord.get("time_micros")))
      .set("date_array", Collections.singleton(tmpRecord.get("date")))
      .build();

    RecordToJsonDocumentTransformer transformer = new RecordToJsonDocumentTransformer("id");
    JsonDocument jsonDocument = transformer.transform(inputRecord);
    JsonObject jsonObject = jsonDocument.content();
    Assert.assertNotNull(jsonObject);
    Assert.assertEquals(tmpRecord.getDecimal("decimal"), jsonObject.getArray("decimal_array").getBigDecimal(0));
    Assert.assertEquals(tmpRecord.getTimestamp("timestamp_millis").toString(),
                        jsonObject.getArray("timestamp_millis_array").getString(0));
    Assert.assertEquals(tmpRecord.getTimestamp("timestamp_micros").toString(),
                        jsonObject.getArray("timestamp_micros_array").getString(0));
    Assert.assertEquals(tmpRecord.getTime("time_millis").toString(),
                        jsonObject.getArray("time_millis_array").getString(0));
    Assert.assertEquals(tmpRecord.getTime("time_micros").toString(),
                        jsonObject.getArray("time_micros_array").getString(0));
    Assert.assertEquals(tmpRecord.getDate("date").toString(), jsonObject.getArray("date_array").getString(0));
  }

  @Test
  public void testTransformUnionLogicalTypes() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("decimal", Schema.unionOf(
                                      Schema.decimalOf(4, 2),
                                      Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS))),
                                    Schema.Field.of("timestamp_millis", Schema.unionOf(
                                      Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS),
                                      Schema.of(Schema.LogicalType.TIME_MILLIS))),
                                    Schema.Field.of("timestamp_micros", Schema.unionOf(
                                      Schema.of(Schema.LogicalType.TIMESTAMP_MICROS),
                                      Schema.of(Schema.LogicalType.TIME_MILLIS))),
                                    Schema.Field.of("time_millis", Schema.unionOf(
                                      Schema.of(Schema.LogicalType.TIME_MILLIS),
                                      Schema.of(Schema.LogicalType.TIME_MICROS))),
                                    Schema.Field.of("time_micros", Schema.unionOf(
                                      Schema.of(Schema.LogicalType.TIME_MICROS),
                                      Schema.of(Schema.LogicalType.TIME_MILLIS))),
                                    Schema.Field.of("date", Schema.unionOf(
                                      Schema.of(Schema.LogicalType.DATE),
                                      Schema.of(Schema.LogicalType.TIME_MICROS))));

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("id", UUID.randomUUID().toString())
      .setDecimal("decimal", new BigDecimal("10.01"))
      .setTimestamp("timestamp_millis", ZonedDateTime.now(ZoneOffset.UTC))
      .setTimestamp("timestamp_micros", ZonedDateTime.now(ZoneOffset.UTC))
      .setTime("time_millis", LocalTime.now(ZoneOffset.UTC))
      .setTime("time_micros", LocalTime.now(ZoneOffset.UTC))
      .setDate("date", LocalDate.now(ZoneOffset.UTC))
      .build();

    RecordToJsonDocumentTransformer transformer = new RecordToJsonDocumentTransformer("id");
    JsonDocument jsonDocument = transformer.transform(inputRecord);
    JsonObject jsonObject = jsonDocument.content();
    Assert.assertNotNull(jsonObject);
    Assert.assertEquals(inputRecord.getDecimal("decimal"), jsonObject.getBigDecimal("decimal"));
    Assert.assertEquals(inputRecord.getTimestamp("timestamp_millis").toString(),
                        jsonObject.getString("timestamp_millis"));
    Assert.assertEquals(inputRecord.getTimestamp("timestamp_micros").toString(),
                        jsonObject.getString("timestamp_micros"));
    Assert.assertEquals(inputRecord.getTime("time_millis").toString(), jsonObject.getString("time_millis"));
    Assert.assertEquals(inputRecord.getTime("time_micros").toString(), jsonObject.getString("time_micros"));
    Assert.assertEquals(inputRecord.getDate("date").toString(), jsonObject.getString("date"));
  }
}
