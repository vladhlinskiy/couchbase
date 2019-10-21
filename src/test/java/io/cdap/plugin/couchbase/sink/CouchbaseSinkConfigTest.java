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

import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.couchbase.CouchbaseConstants;
import io.cdap.plugin.couchbase.OperationType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * Tests of {@link CouchbaseSink.CouchbaseSinkConfig} methods.
 */
public class CouchbaseSinkConfigTest {

  private static final String MOCK_STAGE = "mockstage";

  private static final CouchbaseSink.CouchbaseSinkConfig VALID_CONFIG = CouchbaseSinkConfigBuilder.builder()
    .setReferenceName("CouchbaseSource")
    .setNodes("localhost")
    .setBucket("travel-sample")
    .setKeyField("id")
    .setUser("Administrator")
    .setPassword("password")
    .setOperation(OperationType.INSERT.name())
    .setBatchSize(100)
    .build();

  @Test
  public void testValidateValid() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateReferenceNameNull() {
    CouchbaseSink.CouchbaseSinkConfig config = CouchbaseSinkConfigBuilder.builder(VALID_CONFIG)
      .setReferenceName(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, Constants.Reference.REFERENCE_NAME);
  }

  @Test
  public void testValidateReferenceNameEmpty() {
    CouchbaseSink.CouchbaseSinkConfig config = CouchbaseSinkConfigBuilder.builder(VALID_CONFIG)
      .setReferenceName("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, Constants.Reference.REFERENCE_NAME);
  }

  @Test
  public void testValidateReferenceNameInvalid() {
    CouchbaseSink.CouchbaseSinkConfig config = CouchbaseSinkConfigBuilder.builder(VALID_CONFIG)
      .setReferenceName("**********")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, Constants.Reference.REFERENCE_NAME);
  }

  @Test
  public void testValidateNodesNull() {
    CouchbaseSink.CouchbaseSinkConfig config = CouchbaseSinkConfigBuilder.builder(VALID_CONFIG)
      .setNodes(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.NODES);
  }

  @Test
  public void testValidateNodesEmpty() {
    CouchbaseSink.CouchbaseSinkConfig config = CouchbaseSinkConfigBuilder.builder(VALID_CONFIG)
      .setNodes("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.NODES);
  }

  @Test
  public void testValidateBucketNull() {
    CouchbaseSink.CouchbaseSinkConfig config = CouchbaseSinkConfigBuilder.builder(VALID_CONFIG)
      .setBucket(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.BUCKET);
  }

  @Test
  public void testValidateBucketEmpty() {
    CouchbaseSink.CouchbaseSinkConfig config = CouchbaseSinkConfigBuilder.builder(VALID_CONFIG)
      .setBucket("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.BUCKET);
  }

  @Test
  public void testValidateOperationTypeNull() {
    CouchbaseSink.CouchbaseSinkConfig config = CouchbaseSinkConfigBuilder.builder(VALID_CONFIG)
      .setOperation(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.OPERATION);
  }

  @Test
  public void testValidateOperationTypeEmpty() {
    CouchbaseSink.CouchbaseSinkConfig config = CouchbaseSinkConfigBuilder.builder(VALID_CONFIG)
      .setOperation("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.OPERATION);
  }

  @Test
  public void testValidateOperationTypeInvalid() {
    CouchbaseSink.CouchbaseSinkConfig config = CouchbaseSinkConfigBuilder.builder(VALID_CONFIG)
      .setOperation("unknown-operation-type")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.OPERATION);
  }

  @Test
  public void testValidateBatchSizeZero() {
    CouchbaseSink.CouchbaseSinkConfig config = CouchbaseSinkConfigBuilder.builder(VALID_CONFIG)
      .setBatchSize(0)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.BATCH_SIZE);
  }

  @Test
  public void testValidateBatchSizeInvalid() {
    CouchbaseSink.CouchbaseSinkConfig config = CouchbaseSinkConfigBuilder.builder(VALID_CONFIG)
      .setBatchSize(-1)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.BATCH_SIZE);
  }

  @Test
  public void testValidateKeyFieldNull() {
    CouchbaseSink.CouchbaseSinkConfig config = CouchbaseSinkConfigBuilder.builder(VALID_CONFIG)
      .setKeyField(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.KEY_FIELD);
  }

  @Test
  public void testValidateKeyFieldEmpty() {
    CouchbaseSink.CouchbaseSinkConfig config = CouchbaseSinkConfigBuilder.builder(VALID_CONFIG)
      .setKeyField("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.KEY_FIELD);
  }

  @Test
  public void testValidateUsernameNull() {
    CouchbaseSink.CouchbaseSinkConfig config = CouchbaseSinkConfigBuilder.builder(VALID_CONFIG)
      .setUser(null)
      .setPassword("username is null, but password specified")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.USERNAME);
  }

  @Test
  public void testValidateUsernameEmpty() {
    CouchbaseSink.CouchbaseSinkConfig config = CouchbaseSinkConfigBuilder.builder(VALID_CONFIG)
      .setUser("")
      .setPassword("username is empty, but password specified")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.USERNAME);
  }

  @Test
  public void testValidatePasswordNull() {
    CouchbaseSink.CouchbaseSinkConfig config = CouchbaseSinkConfigBuilder.builder(VALID_CONFIG)
      .setUser("username specified, but password is null")
      .setPassword(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.PASSWORD);
  }

  @Test
  public void testValidatePasswordEmpty() {
    CouchbaseSink.CouchbaseSinkConfig config = CouchbaseSinkConfigBuilder.builder(VALID_CONFIG)
      .setUser("username specified, but password is empty")
      .setPassword("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.PASSWORD);
  }

  @Test
  public void testValidateUsernameAndPasswordNull() {
    CouchbaseSink.CouchbaseSinkConfig config = CouchbaseSinkConfigBuilder.builder(VALID_CONFIG)
      .setUser(null)
      .setPassword(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateUsernameAndPasswordEmpty() {
    CouchbaseSink.CouchbaseSinkConfig config = CouchbaseSinkConfigBuilder.builder(VALID_CONFIG)
      .setUser("")
      .setPassword("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testNodeList() {
    List<String> nodeList = CouchbaseSinkConfigBuilder.builder(VALID_CONFIG)
      .setNodes("node1,node2,node3")
      .build()
      .getNodeList();

    Assert.assertEquals(Arrays.asList("node1", "node2", "node3"), nodeList);
  }

  private static void assertValidationFailed(MockFailureCollector failureCollector, String paramName) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();
    Assert.assertEquals(1, failureList.size());
    ValidationFailure failure = failureList.get(0);
    List<ValidationFailure.Cause> causeList = getCauses(failure, CauseAttributes.STAGE_CONFIG);
    Assert.assertEquals(1, causeList.size());
    ValidationFailure.Cause cause = causeList.get(0);
    Assert.assertEquals(paramName, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Nonnull
  private static List<ValidationFailure.Cause> getCauses(ValidationFailure failure, String attribute) {
    return failure.getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(attribute) != null)
      .collect(Collectors.toList());
  }
}
