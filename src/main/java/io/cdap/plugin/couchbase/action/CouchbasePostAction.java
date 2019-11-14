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

package io.cdap.plugin.couchbase.action;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.query.N1qlQuery;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchActionContext;
import io.cdap.cdap.etl.api.batch.PostAction;
import io.cdap.plugin.couchbase.CouchbaseConstants;
import io.cdap.plugin.couchbase.exception.CouchbaseExecutionException;

import java.util.concurrent.TimeUnit;

/**
 * Cocuhbase post-action runs a query after a pipeline run.
 */
@Plugin(type = PostAction.PLUGIN_TYPE)
@Name(CouchbaseConstants.PLUGIN_NAME)
@Description("Runs a N1QL query after a pipeline run.")
public class CouchbasePostAction extends PostAction {

  private final CouchbaseActionConfig config;

  public CouchbasePostAction(CouchbaseActionConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(failureCollector);
    failureCollector.getOrThrowException();
  }

  @Override
  public void run(BatchActionContext context) throws Exception {
    FailureCollector failureCollector = context.getFailureCollector();
    config.validate(failureCollector);
    failureCollector.getOrThrowException();

    Cluster cluster = CouchbaseCluster.create(config.getNodeList());
    if (!Strings.isNullOrEmpty(config.getUser()) || !Strings.isNullOrEmpty(config.getPassword())) {
      cluster.authenticate(config.getUser(), config.getPassword());
    }
    Bucket bucket = cluster.openBucket(config.getBucket());
    try {
      N1qlQuery query = N1qlQuery.simple(config.getQuery());
      query.params()
        .consistency(config.getScanConsistency().getScanConsistency())
        .maxParallelism(config.getMaxParallelism())
        .serverSideTimeout(config.getTimeout(), TimeUnit.SECONDS);
      bucket.query(query);
    } catch (Throwable e) {
      throw new CouchbaseExecutionException(String.format("Failed to run N1QL query: '%s'", config.getQuery()), e);
    } finally {
      bucket.close();
      cluster.disconnect();
    }
  }
}
