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
import com.couchbase.client.java.document.json.JsonObject;
import io.cdap.cdap.api.data.format.StructuredRecord;

/**
 * Transforms {@link StructuredRecord} to {@link JsonDocument}.
 */
public class RecordToJsonDocumentTransformer {

  private final String idFieldName;

  /**
   * @param idFieldName specifies which of the incoming fields should be used as an document identifier.
   */
  public RecordToJsonDocumentTransformer(String idFieldName) {
    this.idFieldName = idFieldName;
  }

  /**
   * Transforms given {@link StructuredRecord} to {@link JsonDocument}.
   *
   * @param record structured record to be transformed.
   * @return {@link JsonDocument} that corresponds to the given {@link StructuredRecord}.
   */
  public JsonDocument transform(StructuredRecord record) {
    String id = record.get(idFieldName);
    // TODO implement
    return JsonDocument.create(id, JsonObject.empty());
  }
}
