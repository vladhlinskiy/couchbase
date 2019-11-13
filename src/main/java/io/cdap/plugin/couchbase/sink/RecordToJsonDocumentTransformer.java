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
import com.google.common.base.Strings;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Transforms {@link StructuredRecord} to {@link JsonDocument}.
 */
public class RecordToJsonDocumentTransformer {

  private final String keyField;

  /**
   * @param keyField specifies which of the incoming fields should be used as an document identifier.
   */
  public RecordToJsonDocumentTransformer(String keyField) {
    this.keyField = keyField;
  }

  /**
   * Transforms given {@link StructuredRecord} to {@link JsonDocument}.
   *
   * @param record structured record to be transformed.
   * @return {@link JsonDocument} that corresponds to the given {@link StructuredRecord}.
   */
  public JsonDocument transform(StructuredRecord record) {
    String id = record.get(keyField);
    if (Strings.isNullOrEmpty(id)) {
      throw new UnexpectedFormatException(String.format("Invalid value '%s' for key field '%s'. " +
                                                          "Identifier must be a non-empty string.", id, keyField));
    }
    return JsonDocument.create(id, extractRecord(null, record));
  }

  @Nullable
  private JsonObject extractRecord(@Nullable String fieldName, @Nullable StructuredRecord record) {
    if (record == null) {
      // Return 'null' value as it is
      return null;
    }
    List<Schema.Field> fields = Objects.requireNonNull(record.getSchema().getFields(), "Schema fields cannot be empty");
    JsonObject jsonObject = JsonObject.create();
    for (Schema.Field field : fields) {
      // Use full field name for nested records to construct meaningful errors messages.
      // Full field names will be in the following format: 'record_field_name.nested_record_field_name'
      String fullFieldName = fieldName != null ? String.format("%s.%s", fieldName, field.getName()) : field.getName();
      Schema nonNullableSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable()
        : field.getSchema();

      jsonObject.put(field.getName(), extractValue(fullFieldName, record.get(field.getName()), nonNullableSchema));
    }
    return jsonObject;
  }

  private Object extractValue(String fieldName, Object value, Schema schema) {
    if (value == null) {
      // Return 'null' value as it is
      return null;
    }

    Schema.LogicalType fieldLogicalType = schema.getLogicalType();
    // Get values of logical types properly
    if (fieldLogicalType != null) {
      switch (fieldLogicalType) {
        case TIMESTAMP_MILLIS:
          return extractUTCDateTime(TimeUnit.MILLISECONDS.toMicros((long) value)).toString();
        case TIMESTAMP_MICROS:
          return extractUTCDateTime((Long) value).toString();
        case TIME_MILLIS:
          LocalTime time = LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(((Integer) value)));
          return time.toString();
        case TIME_MICROS:
          LocalTime localTime = LocalTime.ofNanoOfDay(TimeUnit.MICROSECONDS.toNanos((Long) value));
          return localTime.toString();
        case DATE:
          long epochDay = ((Integer) value).longValue();
          return LocalDate.ofEpochDay(epochDay).toString();
        case DECIMAL:
          int scale = schema.getScale();
          return value instanceof ByteBuffer
            ? new BigDecimal(new BigInteger(Bytes.toBytes((ByteBuffer) value)), scale)
            : new BigDecimal(new BigInteger((byte[]) value), scale);
        default:
          throw new UnexpectedFormatException(String.format("Field '%s' is of unsupported type '%s'", fieldName,
                                                            fieldLogicalType.name().toLowerCase()));
      }
    }

    Schema.Type fieldType = schema.getType();
    switch (fieldType) {
      case BOOLEAN:
        return (Boolean) value;
      case INT:
        return (Integer) value;
      case DOUBLE:
        return (Double) value;
      case FLOAT:
        return ((Float) value).doubleValue();
      case BYTES:
        byte[] bytes = value instanceof ByteBuffer ? Bytes.getBytes((ByteBuffer) value) : (byte[]) value;
        byte[] encoded = Base64.getEncoder().encode(bytes);
        return new String(encoded);
      case LONG:
        return (Long) value;
      case STRING:
        return (String) value;
      case MAP:
        return extractMap(fieldName, (Map<String, ?>) value, schema);
      case ARRAY:
        return extractArray(fieldName, value, schema);
      case RECORD:
        return extractRecord(fieldName, (StructuredRecord) value);
      case UNION:
        return extractUnion(fieldName, value, schema);
      case ENUM:
        String enumValue = (String) value;
        if (!Objects.requireNonNull(schema.getEnumValues()).contains(enumValue)) {
          throw new UnexpectedFormatException(
            String.format("Value '%s' of the field '%s' is not enum value. Enum values are: '%s'", enumValue, fieldName,
                          schema.getEnumValues()));
        }
        return enumValue;
      default:
        throw new UnexpectedFormatException(String.format("Field '%s' is of unsupported type '%s'", fieldName,
                                                          fieldLogicalType.name().toLowerCase()));
    }
  }

  /**
   * Get UTC zoned date and time represented by the specified timestamp in microseconds.
   *
   * @param micros timestamp in microseconds
   * @return UTC {@link ZonedDateTime} corresponding to the specified timestamp
   */
  private ZonedDateTime extractUTCDateTime(long micros) {
    ZoneId zoneId = ZoneId.ofOffset("UTC", ZoneOffset.UTC);
    long mod = TimeUnit.MICROSECONDS.convert(1, TimeUnit.SECONDS);
    int fraction = (int) (micros % mod);
    long tsInSeconds = TimeUnit.MICROSECONDS.toSeconds(micros);
    // create an Instant with time in seconds and fraction which will be stored as nano seconds.
    Instant instant = Instant.ofEpochSecond(tsInSeconds, TimeUnit.MICROSECONDS.toNanos(fraction));
    return ZonedDateTime.ofInstant(instant, zoneId);
  }

  private JsonObject extractMap(String fieldName, Map<String, ?> map, Schema schema) {
    if (map == null) {
      // Return 'null' value as it is
      return null;
    }
    Map.Entry<Schema, Schema> mapSchema = Objects.requireNonNull(schema.getMapSchema());

    JsonObject jsonObject = JsonObject.create();
    Schema valueSchema = mapSchema.getValue();
    Schema nonNullableSchema = mapSchema.getValue().isNullable() ? valueSchema.getNonNullable() : valueSchema;
    map.forEach((k, v) -> jsonObject.put(k, extractValue(fieldName, v, nonNullableSchema)));

    return jsonObject;
  }

  private JsonArray extractArray(String fieldName, Object value, Schema schema) {
    if (value == null) {
      // Return 'null' value as it is
      return null;
    }

    Schema componentSchema = schema.getComponentSchema();
    Schema nonNullableSchema = componentSchema.isNullable() ? componentSchema.getNonNullable() : componentSchema;
    JsonArray extracted = JsonArray.create();
    if (value.getClass().isArray()) {
      int length = Array.getLength(value);
      for (int i = 0; i < length; i++) {
        Object arrayElement = Array.get(value, i);
        extracted.add(extractValue(fieldName, arrayElement, nonNullableSchema));
      }
      return extracted;
    }
    // An 'array' field can be a java.util.Collection or an array.
    Collection values = (Collection) value;
    for (Object obj : values) {
      extracted.add(extractValue(fieldName, obj, nonNullableSchema));
    }
    return extracted;
  }

  private Object extractUnion(String fieldName, Object value, Schema schema) {
    if (value == null) {
      // Return 'null' value as it is
      return null;
    }
    for (Schema s : schema.getUnionSchemas()) {
      try {
        return extractValue(fieldName, value, s);
      } catch (ClassCastException e) {
        // expected if this schema is not the correct one for the value
      }
    }
    // Should never happen
    throw new IllegalStateException(
      String.format("None of the union schemas '%s' of the field '%s' matches the value '%s'.",
                    schema.getUnionSchemas(), fieldName, value));
  }
}
