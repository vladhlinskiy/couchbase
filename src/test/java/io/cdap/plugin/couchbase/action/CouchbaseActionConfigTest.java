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

import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.couchbase.Consistency;
import io.cdap.plugin.couchbase.CouchbaseConfigBuilder;
import io.cdap.plugin.couchbase.CouchbaseConfigTest;
import io.cdap.plugin.couchbase.CouchbaseConstants;
import org.junit.Test;

/**
 * Tests of {@link CouchbaseActionConfig} methods.
 */
public class CouchbaseActionConfigTest extends CouchbaseConfigTest {

  private static final CouchbaseActionConfig VALID_CONFIG = CouchbaseActionConfigBuilder.builder()
    .setReferenceName("CouchbaseSource")
    .setNodes("localhost")
    .setBucket("travel-sample")
    .setQuery("DELETE FROM `travel-sample` WHERE type = \"hotel\"")
    .setUser("Administrator")
    .setPassword("password")
    .setScanConsistency(Consistency.NOT_BOUNDED.getDisplayName())
    .setMaxParallelism(0)
    .setQueryTimeout(600)
    .build();

  @Override
  protected CouchbaseConfigBuilder getValidConfigBuilder() {
    return CouchbaseActionConfigBuilder.builder(VALID_CONFIG);
  }

  @Test
  public void testValidateScanConsistencyNull() {
    CouchbaseActionConfig config = CouchbaseActionConfigBuilder.builder(VALID_CONFIG)
      .setScanConsistency(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.SCAN_CONSISTENCY);
  }

  @Test
  public void testValidateScanConsistencyEmpty() {
    CouchbaseActionConfig config = CouchbaseActionConfigBuilder.builder(VALID_CONFIG)
      .setScanConsistency("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.SCAN_CONSISTENCY);
  }

  @Test
  public void testValidateScanConsistencyInvalid() {
    CouchbaseActionConfig config = CouchbaseActionConfigBuilder.builder(VALID_CONFIG)
      .setScanConsistency("unknown-scan-consistency")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.SCAN_CONSISTENCY);
  }

  @Test
  public void testValidateQueryNull() {
    CouchbaseActionConfig config = CouchbaseActionConfigBuilder.builder(VALID_CONFIG)
      .setQuery(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.QUERY);
  }

  @Test
  public void testValidateSelectFieldsEmpty() {
    CouchbaseActionConfig config = CouchbaseActionConfigBuilder.builder(VALID_CONFIG)
      .setQuery("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.QUERY);
  }

  @Test
  public void testValidateQueryTimeoutZero() {
    CouchbaseActionConfig config = CouchbaseActionConfigBuilder.builder(VALID_CONFIG)
      .setQueryTimeout(0)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.QUERY_TIMEOUT);
  }

  @Test
  public void testValidateQueryTimeoutInvalid() {
    CouchbaseActionConfig config = CouchbaseActionConfigBuilder.builder(VALID_CONFIG)
      .setQueryTimeout(-100)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.QUERY_TIMEOUT);
  }
}
