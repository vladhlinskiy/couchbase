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

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.google.common.collect.Lists;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.couchbase.CouchbaseUtil;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Transforms {@link JsonObject} to {@link StructuredRecord}.
 */
public class JsonObjectToRecordTransformer {

  private final CouchbaseSourceConfig config;
  private final Schema schema;

  public JsonObjectToRecordTransformer(CouchbaseSourceConfig config, Schema schema) {
    this.config = config;
    this.schema = schema;
  }

  /**
   * Transforms given {@link JsonObject} to {@link StructuredRecord}.
   *
   * @param jsonObject JSON object to be transformed.
   * @return {@link StructuredRecord} that corresponds to the given {@link JsonObject}.
   */
  public StructuredRecord transform(JsonObject jsonObject) {
    JsonObject uniformObject = toUniformJsonObject(jsonObject);
    return extractRecord(null, uniformObject, schema);
  }

  /**
   * Transforms given {@link JsonObject} to it's uniform representation, since returned document set is not required to
   * be uniform, though it can be. A SELECT statement that specifies a fixed set of attribute (column) names results
   * in a uniform set of documents and a SELECT statement that specifies the wild card (*) results in a non-uniform
   * result set. Wild card statements result in an object with single key that matches bucket name.
   * <p>
   * For example:
   * </p>
   * 1) For statement "SELECT * FROM `travel-sample`", where `travel-sample` is a bucket name,
   * JsonObject will have the following structure:
   * <p>
   * {
   * "travel-sample": {
   * "callsign": "MILE-AIR",
   * "country": "United States"
   * }
   * }
   * </p>
   * this method will convert such object to:
   * <p>
   * {
   * "callsign": "MILE-AIR",
   * "country": "United States"
   * }
   * </p>
   * </p>
   * 2) For statement "SELECT meta(`travel-sample`).id, * FROM `travel-sample`", where `travel-sample` is a bucket name,
   * JsonObject will have the following structure:
   * <p>
   * {
   * "id": "airline_10",
   * "travel-sample": {
   * "callsign": "MILE-AIR",
   * "country": "United States"
   * }
   * }
   * </p>
   * this method will convert such object to:
   * <p>
   * {
   * "id": "airline_10",
   * "callsign": "MILE-AIR",
   * "country": "United States"
   * }
   * </p>
   * 3) For statement "SELECT callsign, country FROM `travel-sample`", where `travel-sample` is a bucket name,
   * JsonObject will have the following structure:
   * <p>
   * {
   * "callsign": "MILE-AIR",
   * "country": "United States"
   * }
   * </p>
   * and will be returned as it is.
   * <p>
   * See: <a href="https://docs.couchbase.com/server/current/n1ql/n1ql-intro/queriesandresults.html#queries">
   * N1QL Queries and Results
   * </a>
   *
   * @param jsonObject result document, possibly in a non-uniform representation.
   * @return uniform representation of the given result document.
   */
  private JsonObject toUniformJsonObject(JsonObject jsonObject) {
    boolean isWildCardQuery = config.getSelectFields().contains("*");
    if (isWildCardQuery && jsonObject.containsKey(config.getBucket())) {
      JsonObject payload = jsonObject.getObject(config.getBucket());
      if (jsonObject.size() == 1) {
        return payload;
      }
      // Result object can contain other fields for queries such as: SELECT meta(`test-bucket`).id, * from `test-bucket`
      // Include them as well
      jsonObject.removeKey(config.getBucket());
      for (String name : jsonObject.getNames()) {
        payload.put(name, jsonObject.get(name));
      }
      return payload;
    }

    return jsonObject;
  }

  private StructuredRecord extractRecord(@Nullable String fieldName, JsonObject object, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    for (String propertyName : object.getNames()) {
      String validPropertyName = CouchbaseUtil.fieldName(propertyName);
      Schema.Field field = schema.getField(validPropertyName);
      if (field == null) {
        continue;
      }

      // Use full field name for nested records to construct meaningful errors messages.
      // Full field names will be in the following format: 'record_field_name.nested_record_field_name'
      String fullFieldName = fieldName != null ? String.format("%s.%s", fieldName, field.getName()) : field.getName();
      Schema nonNullableSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable()
        : field.getSchema();
      Object value = extractValue(fullFieldName, object.get(propertyName), nonNullableSchema);
      builder.set(field.getName(), value);
    }
    return builder.build();
  }

  private Object extractValue(String fieldName, Object object, Schema schema) {
    if (object == null) {
      return null;
    }

    Schema.LogicalType fieldLogicalType = schema.getLogicalType();
    if (fieldLogicalType != null) {
      switch (fieldLogicalType) {
        case DECIMAL:
          ensureTypeValid(fieldName, object, Number.class);
          return extractDecimal(fieldName, (Number) object, schema);
        default:
          throw new UnexpectedFormatException(String.format("Field '%s' is of unsupported type '%s'", fieldName,
                                                            fieldLogicalType.name().toLowerCase()));
      }
    }

    Schema.Type fieldType = schema.getType();
    switch (fieldType) {
      case BOOLEAN:
        ensureTypeValid(fieldName, object, Boolean.class);
        return object;
      case INT:
        ensureTypeValid(fieldName, object, Integer.class);
        return object;
      case DOUBLE:
        ensureTypeValid(fieldName, object, Double.class);
        return object;
      case LONG:
        ensureTypeValid(fieldName, object, Integer.class, Long.class);
        return ((Number) object).longValue();
      case STRING:
        ensureTypeValid(fieldName, object, String.class, Number.class);
        return object.toString();
      case MAP:
        ensureTypeValid(fieldName, object, JsonObject.class);
        Schema valueSchema = schema.getMapSchema().getValue();
        Schema nonNullableValueSchema = valueSchema.isNullable() ? valueSchema.getNonNullable() : valueSchema;
        return extractMap(fieldName, (JsonObject) object, nonNullableValueSchema);
      case ARRAY:
        ensureTypeValid(fieldName, object, JsonArray.class);
        Schema componentSchema = schema.getComponentSchema().isNullable() ? schema.getComponentSchema().getNonNullable()
          : schema.getComponentSchema();
        return extractArray(fieldName, (JsonArray) object, componentSchema);
      case RECORD:
        ensureTypeValid(fieldName, object, JsonObject.class);
        return extractRecord(fieldName, (JsonObject) object, schema);
      default:
        throw new UnexpectedFormatException(String.format("Field '%s' is of unsupported type '%s'", fieldName,
                                                          fieldType.name().toLowerCase()));
    }
  }

  private Map<String, Object> extractMap(String fieldName, JsonObject jsonObject, Schema valueSchema) {
    Map<String, Object> extracted = new HashMap<>();
    for (String key : jsonObject.getNames()) {
      Object value = jsonObject.get(key);
      extracted.put(key, extractValue(fieldName, value, valueSchema));
    }
    return extracted;
  }

  private List<Object> extractArray(String fieldName, JsonArray array, Schema componentSchema) {
    List<Object> values = Lists.newArrayListWithCapacity(array.size());
    for (Object obj : array) {
      values.add(extractValue(fieldName, obj, componentSchema));
    }
    return values;
  }

  private byte[] extractDecimal(String fieldName, Number value, Schema schema) {
    int schemaPrecision = schema.getPrecision();
    int schemaScale = schema.getScale();
    BigDecimal decimal = extractBigDecimal(value, schema);
    if (decimal.precision() > schemaPrecision) {
      throw new UnexpectedFormatException(
        String.format("Field '%s' has precision '%s' which is higher than schema precision '%s'.",
                      fieldName, decimal.precision(), schemaPrecision));
    }

    if (decimal.scale() > schemaScale) {
      throw new UnexpectedFormatException(
        String.format("Field '%s' has scale '%s' which is not equal to schema scale '%s'.",
                      fieldName, decimal.scale(), schemaScale));
    }

    return decimal.setScale(schemaScale).unscaledValue().toByteArray();
  }

  /**
   * In Couchbase SDK a decimal value will be represented as primitive if it's possible. For example, value stored as:
   * <pre>
   * {@code
   * JsonDocument doc = JsonDocument.create("document_id", JsonObject.create().put("decimal", new BigDecimal("3.14")));
   * bucket.insert(doc);
   * }
   * </pre> will then be read by as instance of {@link Double}.
   */
  private BigDecimal extractBigDecimal(Number value, Schema schema) {
    if (value instanceof BigDecimal) {
      return (BigDecimal) value;
    }
    if (value instanceof BigInteger) {
      return new BigDecimal((BigInteger) value);
    }
    if (value instanceof Double) {
      double doubleValue = value.doubleValue();
      int precision = schema.getPrecision();
      int scale = schema.getScale();
      return new BigDecimal(doubleValue, new MathContext(precision)).setScale(scale, BigDecimal.ROUND_HALF_EVEN);
    }

    // Integer, Long
    long longValue = value.longValue();
    return new BigDecimal(longValue);
  }

  private void ensureTypeValid(String fieldName, Object value, Class... expectedTypes) {
    for (Class expectedType : expectedTypes) {
      if (expectedType.isInstance(value)) {
        return;
      }
    }

    String expectedTypeNames = Stream.of(expectedTypes)
      .map(Class::getName)
      .collect(Collectors.joining(", "));
    throw new UnexpectedFormatException(
      String.format("Document field '%s' is expected to be of type '%s', but found a '%s'.", fieldName,
                    expectedTypeNames, value.getClass().getSimpleName()));
  }
}
