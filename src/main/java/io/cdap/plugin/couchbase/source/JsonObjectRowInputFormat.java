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

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.plugin.couchbase.exception.CouchbaseExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Couchbase input format for mapreduce job, deserializes configuration and creates input splits according to the
 * specified number of splits.
 */
public class JsonObjectRowInputFormat extends InputFormat {

  private static final Gson gson = new GsonBuilder().create();

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) {
    Configuration conf = jobContext.getConfiguration();
    String confJson = conf.get(JsonObjectInputFormatProvider.PROPERTY_CONFIG_JSON);
    CouchbaseSourceConfig config = gson.fromJson(confJson, CouchbaseSourceConfig.class);
    int mapTasks = conf.getInt(MRJobConfig.NUM_MAPS, 1);
    BigInteger numSplits = config.getNumSplits() == 0 ? BigInteger.valueOf(mapTasks)
      : BigInteger.valueOf(config.getNumSplits());
    if (numSplits.intValue() == 1) {
      // single split specified
      Query query = new Query(config.getBucket(), config.getSelectFields(), config.getConditions());
      return Collections.singletonList(new CouchbaseSplit(query));
    }

    BigInteger totalDocumentsNumber = getDocumentsNumber(config);
    if (totalDocumentsNumber.compareTo(numSplits) < 0) {
      // number of documents is less than specified number of splits
      Query query = new Query(config.getBucket(), config.getSelectFields(), config.getConditions());
      return Collections.singletonList(new CouchbaseSplit(query));
    }

    List<InputSplit> splits = new ArrayList<>(numSplits.intValue());
    BigInteger documentsPerSplit = totalDocumentsNumber.divide(numSplits);
    BigInteger remainder = totalDocumentsNumber.remainder(numSplits);
    for (int i = 0; i < numSplits.intValue(); i++) {
      BigInteger offset = documentsPerSplit.multiply(BigInteger.valueOf(i));
      BigInteger limit = i == numSplits.intValue() - 1 ? documentsPerSplit.add(remainder) : documentsPerSplit;
      Query query = new RangeQuery(config.getBucket(), config.getSelectFields(), config.getConditions(), offset, limit);
      splits.add(new CouchbaseSplit(query));
    }

    return splits;
  }

  private BigInteger getDocumentsNumber(CouchbaseSourceConfig config) {
    Cluster cluster = CouchbaseCluster.create(config.getNodeList());
    if (!Strings.isNullOrEmpty(config.getUser()) || !Strings.isNullOrEmpty(config.getPassword())) {
      cluster.authenticate(config.getUser(), config.getPassword());
    }

    Bucket bucket = cluster.openBucket(config.getBucket());
    String statement = String.format("SELECT COUNT(*) AS doc_num FROM `%s`", config.getBucket());
    N1qlQueryResult result = bucket.query(N1qlQuery.simple(statement));
    if (!result.finalSuccess()) {
      String errorMessage = result.errors().stream()
        .map(JsonObject::toString)
        .collect(Collectors.joining("\n"));
      throw new CouchbaseExecutionException(errorMessage);
    }

    N1qlQueryRow documentsNumberRow = result.rows().next();
    Object documentsNumber = documentsNumberRow.value().get("doc_num");
    bucket.close();

    return extractBigInteger(documentsNumber);
  }

  /**
   * Convert specified number of documents {@link Object} to corresponding {@link BigInteger} value.
   * Couchbase has no limit on documents number. Thus, actual value may be instance of one of the Java types:
   * {@link Integer}, {@link Long}, {@link BigInteger}
   *
   * @param value number of documents as instance of one of the Java types:
   *              {@link Integer}, {@link Long}, {@link BigInteger}
   * @return specified number of documents as instance of {@link BigInteger}
   */
  private BigInteger extractBigInteger(Object value) {
    if (value instanceof BigInteger) {
      return (BigInteger) value;
    }

    long longValue = ((Number) value).longValue();
    return BigInteger.valueOf(longValue);
  }

  @Override
  public RecordReader<NullWritable, JsonObject> createRecordReader(InputSplit inputSplit,
                                                                   TaskAttemptContext taskAttemptContext) {
    return new JsonObjectRecordReader();
  }
}
