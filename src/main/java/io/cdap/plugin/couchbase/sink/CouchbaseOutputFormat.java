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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * An OutputFormat that sends the output of a Hadoop job to the Couchbase. CouchbaseOutputFormat accepts key,
 * value pairs, but the returned {@link CouchbaseRecordWriter} writes only the value to the bucket as each
 * Couchbase document already contains an identifier.
 */
public class CouchbaseOutputFormat extends OutputFormat<NullWritable, JsonDocument> {

  private static final Gson gson = new GsonBuilder().create();

  @Override
  public RecordWriter<NullWritable, JsonDocument> getRecordWriter(TaskAttemptContext taskAttemptContext) {
    Configuration conf = taskAttemptContext.getConfiguration();
    String configJson = conf.get(CouchbaseOutputFormatProvider.PROPERTY_CONFIG_JSON);
    CouchbaseSinkConfig config = gson.fromJson(configJson, CouchbaseSinkConfig.class);
    return new CouchbaseRecordWriter(config);
  }

  @Override
  public void checkOutputSpecs(JobContext jobContext) {
    //no-op
  }

  /**
   * No op output committer
   */
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) {
    return new OutputCommitter() {
      @Override
      public void setupJob(JobContext jobContext) {

      }

      @Override
      public void setupTask(TaskAttemptContext taskAttemptContext) {

      }

      @Override
      public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) {
        return false;
      }

      @Override
      public void commitTask(TaskAttemptContext taskAttemptContext) {

      }

      @Override
      public void abortTask(TaskAttemptContext taskAttemptContext) {

      }
    };
  }
}
