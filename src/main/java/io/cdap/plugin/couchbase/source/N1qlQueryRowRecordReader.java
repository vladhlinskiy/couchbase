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
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.plugin.couchbase.CouchbaseConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Iterator;

/**
 * RecordReader implementation, which reads N1qlQueryRow entries.
 */
public class N1qlQueryRowRecordReader extends RecordReader<NullWritable, N1qlQueryRow> {

  private static final Gson gson = new GsonBuilder().create();

  private Iterator<N1qlQueryRow> iterator;
  private N1qlQueryRow value;

  /**
   * Initialize an iterator and config.
   *
   * @param inputSplit         specifies batch details
   * @param taskAttemptContext task context
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
    Configuration conf = taskAttemptContext.getConfiguration();
    String configJson = conf.get(N1qlQueryRowInputFormatProvider.PROPERTY_CONFIG_JSON);
    CouchbaseConfig config = gson.fromJson(configJson, CouchbaseConfig.class);

    // TODO
    Cluster cluster = CouchbaseCluster.create(config.getNodes());
    cluster.authenticate(config.getUser(), config.getPassword());
    Bucket bucket = cluster.openBucket(config.getBucket());

    // Perform a N1QL Query
    N1qlQueryResult result = bucket.query(
      N1qlQuery.simple(config.getQuery())
    );

    // TODO result error handling
    this.iterator = result.rows();
  }

  @Override
  public boolean nextKeyValue() {
    if (!iterator.hasNext()) {
      return false;
    }
    value = iterator.next();
    return true;
  }

  @Override
  public NullWritable getCurrentKey() {
    return null;
  }

  @Override
  public N1qlQueryRow getCurrentValue() {
    return value;
  }

  @Override
  public float getProgress() {
    // progress is unknown
    return 0.0f;
  }

  @Override
  public void close() throws IOException {
  }
}
