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
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.Select;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.dsl.Expression;
import com.google.common.base.Strings;

import java.math.BigInteger;

/**
 * Couchbase chunk query used by {@link CouchbaseSplit}. Allows, to read data by chunks of the specified size.
 * Chunk size must be less than or equal to {@value Integer#MAX_VALUE} since Couchbbase Java SDK
 * {@link N1qlQueryResult} can not contain more than {@value Integer#MAX_VALUE} rows whereas Couchbase has no limit on
 * documents number.
 */
public class ChunkQuery extends Query {

  public static final int CHUNK_SIZE = Integer.MAX_VALUE;

  private BigInteger offset;
  private final BigInteger limit;

  public ChunkQuery(String bucket, String selectFields, String conditions, BigInteger offset,
                    BigInteger limit) {
    super(bucket, selectFields, conditions);
    this.offset = offset;
    this.limit = limit;
  }

  @Override
  public N1qlQuery getNextN1qlQuery() {
    if (!hasNext) {
      return null;
    }
    if (limit.subtract(offset).compareTo(BigInteger.valueOf(CHUNK_SIZE)) <= 0) {
      hasNext = false;
      return rangedQuery(limit, offset);
    }

    BigInteger curLimit = offset.add(BigInteger.valueOf(CHUNK_SIZE));
    N1qlQuery rangedQuery = rangedQuery(curLimit, offset);
    offset = offset.add(curLimit);

    return rangedQuery;
  }

  private N1qlQuery rangedQuery(BigInteger limit, BigInteger offset) {
    Statement statement = Strings.isNullOrEmpty(conditions)
      ? Select.select(selectFields).from(Expression.i(bucket))
      : Select.select(selectFields).from(Expression.i(bucket)).where(conditions);

    // OffsetPath#offset(int offset) can not be used, since it does not work with offsets greater than Integer.MAX_VALUE
    String offsetLimitQuery = String.format("%s OFFSET %d LIMIT %d", statement.toString(), offset, limit);
    return N1qlQuery.simple(offsetLimitQuery);
  }
}
