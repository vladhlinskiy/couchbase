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
import com.couchbase.client.java.query.AsyncN1qlQueryRow;
import com.couchbase.client.java.query.N1qlQuery;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.plugin.couchbase.exception.CouchbaseExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * RecordReader implementation, which reads N1qlQueryRow entries.
 */
public class JsonObjectRecordReader extends RecordReader<NullWritable, JsonObject> {
  private static final Logger LOG = LoggerFactory.getLogger(JsonObjectRecordReader.class);
  private static final Gson gson = new GsonBuilder().create();

  private Cluster cluster;
  private Bucket bucket;
  private Iterator<AsyncN1qlQueryRow> iterator;
  private JsonObject value;

  /**
   * Initialize an iterator and config.
   *
   * @param inputSplit         specifies batch details
   * @param taskAttemptContext task context
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
    Configuration conf = taskAttemptContext.getConfiguration();
    String confJson = conf.get(JsonObjectInputFormatProvider.PROPERTY_CONFIG_JSON);
    CouchbaseSourceConfig config = gson.fromJson(confJson, CouchbaseSourceConfig.class);

    this.cluster = CouchbaseCluster.create(config.getNodeList());
    if (!Strings.isNullOrEmpty(config.getUser()) || !Strings.isNullOrEmpty(config.getPassword())) {
      cluster.authenticate(config.getUser(), config.getPassword());
    }
    this.bucket = cluster.openBucket(config.getBucket());
    CouchbaseSplit split = (CouchbaseSplit) inputSplit;

    N1qlQuery query = split.getQuery().getN1qlQuery();
    query.params()
      .consistency(config.getScanConsistency().getScanConsistency())
      .maxParallelism(config.getMaxParallelism())
      .serverSideTimeout(config.getTimeout(), TimeUnit.SECONDS);

    this.iterator = query(query).toBlocking().toIterable().iterator();
  }

  /**
   * For a long-running N1QL query, we need a way of streaming the rows and correctly handling errors.
   */
  public Observable<AsyncN1qlQueryRow> query(N1qlQuery query) {
    return bucket.async()
      .query(query)
      .flatMap(result -> Observable.merge(result.rows(), result.errors())
        .map(rowOrError -> {
          if (rowOrError instanceof AsyncN1qlQueryRow) {
            return (AsyncN1qlQueryRow) rowOrError;
          } else {
            throw new CouchbaseExecutionException(rowOrError.toString());
          }
        }));
  }

  @Override
  public boolean nextKeyValue() {
    if (!iterator.hasNext()) {
      return false;
    }
    value = iterator.next().value();
    return true;
  }

  @Override
  public NullWritable getCurrentKey() {
    return null;
  }

  @Override
  public JsonObject getCurrentValue() {
    return value;
  }

  @Override
  public float getProgress() {
    // progress is unknown
    return 0.0f;
  }

  @Override
  public void close() throws IOException {
    LOG.trace("Closing Record reader");
    if (bucket != null) {
      bucket.close();
    }
    if (cluster != null) {
      cluster.disconnect();
    }
  }
}
