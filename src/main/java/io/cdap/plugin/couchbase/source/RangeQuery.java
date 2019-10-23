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

import java.math.BigInteger;

/**
 * Couchbase range query used by {@link CouchbaseSplit}.
 */
public class RangeQuery extends Query {

  private BigInteger offset;
  private final BigInteger limit;

  public RangeQuery(String bucket, String selectFields, String conditions, BigInteger offset, BigInteger limit) {
    super(bucket, selectFields, conditions);
    this.offset = offset;
    this.limit = limit;
  }

  @Override
  public N1qlQuery getN1qlQuery() {
    Statement statement = Strings.isNullOrEmpty(conditions)
      ? Select.select(selectFields).from(Expression.i(bucket))
      : Select.select(selectFields).from(Expression.i(bucket)).where(conditions);

    // OffsetPath#offset(int offset) can not be used, since it does not work with offsets greater than Integer.MAX_VALUE
    String offsetLimitQuery = String.format("%s OFFSET %s LIMIT %s", statement.toString(), offset.toString(),
                                            limit.toString());
    return N1qlQuery.simple(offsetLimitQuery);
  }
}
