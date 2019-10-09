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
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

/**
 * Transforms {@link JsonObject} to {@link StructuredRecord}.
 */
public class JsonObjectToRecordTransformer {

  private final Schema schema;

  public JsonObjectToRecordTransformer(Schema schema) {
    this.schema = schema;
  }

  /**
   * Transforms given {@link JsonObject} to {@link StructuredRecord}.
   *
   * @param jsonObject JSON object to be transformed.
   * @return {@link StructuredRecord} that corresponds to the given {@link JsonObject}.
   */
  public StructuredRecord transform(JsonObject jsonObject) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    for (Schema.Field field : schema.getFields()) {
      Schema nonNullableSchema = field.getSchema().isNullable() ?
        field.getSchema().getNonNullable() : field.getSchema();
      String fieldName = field.getName();
      Object value = jsonObject.get(fieldName);
      builder.set(fieldName, extractValue(fieldName, value, nonNullableSchema));
    }
    return builder.build();
  }

  private Object extractValue(String fieldName, Object object, Schema schema) {
    if (object == null) {
      return null;
    }
    // TODO implement
    return object;
  }
}
