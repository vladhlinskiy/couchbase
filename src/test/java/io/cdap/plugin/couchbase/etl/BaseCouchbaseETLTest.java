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
package io.cdap.plugin.couchbase.etl;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.BucketSettings;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.plugin.couchbase.CouchbaseConstants;
import io.cdap.plugin.couchbase.sink.CouchbaseSink;
import io.cdap.plugin.couchbase.source.CouchbaseSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class BaseCouchbaseETLTest extends HydratorTestBase {

  public static final int COUCHBASE_TIMEOUT = 600;

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  protected static final Map<String, String> BASE_PROPERTIES = ImmutableMap.<String, String>builder()
    .put(CouchbaseConstants.NODES, System.getProperty("nodes", "localhost"))
    .put(CouchbaseConstants.USERNAME, System.getProperty("username", "Administrator"))
    .put(CouchbaseConstants.PASSWORD, System.getProperty("password", "password"))
    .put(CouchbaseConstants.BUCKET, System.getProperty("bucket", "test-bucket"))
    .build();

  protected static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");

  protected static Cluster cluster;
  protected static Bucket bucket;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setupTestClass() throws Exception {
    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    // add the artifact and mock plugins
    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

    // add our plugins artifact with the artifact as its parent.
    // this will make our plugins available.
    addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"), parentArtifact,
                      CouchbaseSource.class, CouchbaseSink.class);

    List<String> nodes = Arrays.asList(BASE_PROPERTIES.get(CouchbaseConstants.NODES).split(","));
    String username = BASE_PROPERTIES.get(CouchbaseConstants.USERNAME);
    String password = BASE_PROPERTIES.get(CouchbaseConstants.PASSWORD);
    String bucketName = BASE_PROPERTIES.get(CouchbaseConstants.BUCKET);
    cluster = CouchbaseCluster.create(nodes);
    if (!Strings.isNullOrEmpty(username) || !Strings.isNullOrEmpty(password)) {
      cluster.authenticate(username, password);
    }

    bucket = createOrReplaceBucket(bucketName);
  }

  protected static Bucket createOrReplaceBucket(String bucketName) throws Exception {
    ClusterManager clusterManager = cluster.clusterManager();
    clusterManager.removeBucket(bucketName);
    BucketSettings bucketSettings = new DefaultBucketSettings.Builder()
      .type(BucketType.COUCHBASE)
      .name(bucketName)
      .quota(100) // megabytes
      .replicas(1)
      .indexReplicas(true)
      .enableFlush(true)
      .build();

    clusterManager.insertBucket(bucketSettings);
    Bucket bucket = cluster.openBucket(bucketName);
    // Buckets with no index cannot be queried. Documents can only be retrieved by making use of the USE KEYS operator
    bucket.bucketManager().createN1qlPrimaryIndex(true, false, COUCHBASE_TIMEOUT, TimeUnit.SECONDS);

    return bucket;
  }

  @AfterClass
  public static void afterTestClass() throws Exception {
    bucket.close();
    cluster.disconnect();
  }
}
