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

import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.Select;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.dsl.Expression;
import com.google.common.base.Strings;

import java.io.Serializable;

/**
 * Couchbase query used by {@link CouchbaseSplit}.
 */
public class Query implements Serializable {

  protected final String bucket;
  protected final String selectFields;
  protected final String conditions;

  public Query(String bucket, String selectFields, String conditions) {
    this.bucket = bucket;
    this.selectFields = selectFields;
    this.conditions = conditions;
  }

  public N1qlQuery getN1qlQuery() {
    Statement statement = Strings.isNullOrEmpty(conditions)
      ? Select.select(selectFields).from(Expression.i(bucket))
      : Select.select(selectFields).from(Expression.i(bucket)).where(conditions);

    return N1qlQuery.simple(statement);
  }
}
