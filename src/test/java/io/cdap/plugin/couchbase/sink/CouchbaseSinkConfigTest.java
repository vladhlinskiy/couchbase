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

import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.couchbase.CouchbaseConfigBuilder;
import io.cdap.plugin.couchbase.CouchbaseConfigTest;
import io.cdap.plugin.couchbase.CouchbaseConstants;
import io.cdap.plugin.couchbase.OperationType;
import org.junit.Test;

/**
 * Tests of {@link CouchbaseSinkConfig} methods.
 */
public class CouchbaseSinkConfigTest extends CouchbaseConfigTest {

  private static final CouchbaseSinkConfig VALID_CONFIG = CouchbaseSinkConfigBuilder.builder()
    .setReferenceName("CouchbaseSource")
    .setNodes("localhost")
    .setBucket("travel-sample")
    .setKeyField("id")
    .setUser("Administrator")
    .setPassword("password")
    .setOperation(OperationType.INSERT.name())
    .setBatchSize(100)
    .build();

  @Override
  protected CouchbaseConfigBuilder getValidConfigBuilder() {
    return CouchbaseSinkConfigBuilder.builder(VALID_CONFIG);
  }

  @Test
  public void testValidateOperationTypeNull() {
    CouchbaseSinkConfig config = CouchbaseSinkConfigBuilder.builder(VALID_CONFIG)
      .setOperation(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.OPERATION);
  }

  @Test
  public void testValidateOperationTypeEmpty() {
    CouchbaseSinkConfig config = CouchbaseSinkConfigBuilder.builder(VALID_CONFIG)
      .setOperation("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.OPERATION);
  }

  @Test
  public void testValidateOperationTypeInvalid() {
    CouchbaseSinkConfig config = CouchbaseSinkConfigBuilder.builder(VALID_CONFIG)
      .setOperation("unknown-operation-type")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.OPERATION);
  }

  @Test
  public void testValidateBatchSizeZero() {
    CouchbaseSinkConfig config = CouchbaseSinkConfigBuilder.builder(VALID_CONFIG)
      .setBatchSize(0)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.BATCH_SIZE);
  }

  @Test
  public void testValidateBatchSizeInvalid() {
    CouchbaseSinkConfig config = CouchbaseSinkConfigBuilder.builder(VALID_CONFIG)
      .setBatchSize(-1)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.BATCH_SIZE);
  }

  @Test
  public void testValidateKeyFieldNull() {
    CouchbaseSinkConfig config = CouchbaseSinkConfigBuilder.builder(VALID_CONFIG)
      .setKeyField(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.KEY_FIELD);
  }

  @Test
  public void testValidateKeyFieldEmpty() {
    CouchbaseSinkConfig config = CouchbaseSinkConfigBuilder.builder(VALID_CONFIG)
      .setKeyField("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.KEY_FIELD);
  }
}
