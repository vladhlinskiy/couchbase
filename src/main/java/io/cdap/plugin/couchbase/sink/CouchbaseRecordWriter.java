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

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.plugin.couchbase.CouchbaseSinkConfig;
import io.cdap.plugin.couchbase.OperationType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link CouchbaseRecordWriter} writes the job outputs to the Couchbase. Accepts <code>null</code> key,
 * {@link JsonDocument} pairs but writes only {@link JsonDocument} to the couchbase.
 */
public class CouchbaseRecordWriter extends RecordWriter<NullWritable, JsonDocument> {

  private static final Logger LOG = LoggerFactory.getLogger(CouchbaseRecordWriter.class);
  private static final Gson gson = new GsonBuilder().create();

  private final Func1<JsonDocument, Observable<JsonDocument>> operation;
  private final Bucket bucket;
  private final int batchSize;
  private List<JsonDocument> batch;
  private int totalCount;

  public CouchbaseRecordWriter(TaskAttemptContext taskAttemptContext) {
    Configuration conf = taskAttemptContext.getConfiguration();
    String configJson = conf.get(CouchbaseOutputFormatProvider.PROPERTY_CONFIG_JSON);
    CouchbaseSinkConfig config = gson.fromJson(configJson, CouchbaseSinkConfig.class);

    Cluster cluster = CouchbaseCluster.create(config.getNodeList());
    if (!Strings.isNullOrEmpty(config.getUser()) || !Strings.isNullOrEmpty(config.getPassword())) {
      cluster.authenticate(config.getUser(), config.getPassword());
    }
    this.bucket = cluster.openBucket(config.getBucket());
    this.batchSize = config.getBatchSize();
    this.batch = new ArrayList<>();
    this.totalCount = 0;
    OperationType operationType = config.getOperationType();
    this.operation = operationType == OperationType.INSERT ? docToInsert -> bucket.async().insert(docToInsert)
      : operationType == OperationType.REPLACE ? docToInsert -> bucket.async().replace(docToInsert)
      : docToInsert -> bucket.async().upsert(docToInsert);
  }

  @Override
  public void write(NullWritable key, JsonDocument document) {
    LOG.trace("RecordWriter write({})", document);
    batch.add(document);
    ++totalCount;
    if (totalCount % batchSize == 0) {
      flush();
    }
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) {
    flush();
    LOG.debug("Total number of values written to Couchbase: {}", totalCount);
    bucket.close();
  }

  private void flush() {
    if (batch.size() > 0) {
      LOG.debug("Writing a batch of {} values to Couchbase.", batch.size());
      // Insert documents in one batch, waiting until the last one is done.
      Observable
        .from(batch)
        .flatMap(operation)
        .last()
        .toBlocking()
        .last();

      batch.clear();
    }
  }
}
