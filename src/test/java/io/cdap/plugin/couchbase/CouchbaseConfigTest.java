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

package io.cdap.plugin.couchbase;

import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.common.Constants;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * Tests of {@link CouchbaseConfig} methods.
 */
public abstract class CouchbaseConfigTest {

  protected static final String MOCK_STAGE = "mockstage";

  protected abstract CouchbaseConfigBuilder getValidConfigBuilder();

  @Test
  public void testValidateValid() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    getValidConfigBuilder().build().validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateReferenceNameNull() {
    CouchbaseConfig config = getValidConfigBuilder()
      .setReferenceName(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, Constants.Reference.REFERENCE_NAME);
  }

  @Test
  public void testValidateReferenceNameEmpty() {
    CouchbaseConfig config = getValidConfigBuilder()
      .setReferenceName("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, Constants.Reference.REFERENCE_NAME);
  }

  @Test
  public void testValidateReferenceNameInvalid() {
    CouchbaseConfig config = getValidConfigBuilder()
      .setReferenceName("**********")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, Constants.Reference.REFERENCE_NAME);
  }

  @Test
  public void testValidateNodesNull() {
    CouchbaseConfig config = getValidConfigBuilder()
      .setNodes(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.NODES);
  }

  @Test
  public void testValidateNodesEmpty() {
    CouchbaseConfig config = getValidConfigBuilder()
      .setNodes("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.NODES);
  }

  @Test
  public void testValidateBucketNull() {
    CouchbaseConfig config = getValidConfigBuilder()
      .setBucket(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.BUCKET);
  }

  @Test
  public void testValidateBucketEmpty() {
    CouchbaseConfig config = getValidConfigBuilder()
      .setBucket("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.BUCKET);
  }

  @Test
  public void testValidateUsernameNull() {
    CouchbaseConfig config = getValidConfigBuilder()
      .setUser(null)
      .setPassword("username is null, but password specified")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.USERNAME);
  }

  @Test
  public void testValidateUsernameEmpty() {
    CouchbaseConfig config = getValidConfigBuilder()
      .setUser("")
      .setPassword("username is empty, but password specified")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.USERNAME);
  }

  @Test
  public void testValidatePasswordNull() {
    CouchbaseConfig config = getValidConfigBuilder()
      .setUser("username specified, but password is null")
      .setPassword(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.PASSWORD);
  }

  @Test
  public void testValidatePasswordEmpty() {
    CouchbaseConfig config = getValidConfigBuilder()
      .setUser("username specified, but password is empty")
      .setPassword("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.PASSWORD);
  }

  @Test
  public void testValidateUsernameAndPasswordNull() {
    CouchbaseConfig config = getValidConfigBuilder()
      .setUser(null)
      .setPassword(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateUsernameAndPasswordEmpty() {
    CouchbaseConfig config = getValidConfigBuilder()
      .setUser("")
      .setPassword("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testNodeList() {
    List<String> nodeList = getValidConfigBuilder()
      .setNodes("node1,node2,node3")
      .build()
      .getNodeList();

    Assert.assertEquals(Arrays.asList("node1", "node2", "node3"), nodeList);
  }

  protected static void assertOutputSchemaValidationFailed(MockFailureCollector failureCollector, String fieldName) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();
    Assert.assertEquals(1, failureList.size());
    ValidationFailure failure = failureList.get(0);
    List<ValidationFailure.Cause> causeList = getCauses(failure, CauseAttributes.OUTPUT_SCHEMA_FIELD);
    Assert.assertEquals(1, causeList.size());
    ValidationFailure.Cause cause = causeList.get(0);
    Assert.assertEquals(fieldName, cause.getAttribute(CauseAttributes.OUTPUT_SCHEMA_FIELD));
  }

  protected static void assertValidationFailed(MockFailureCollector failureCollector, String paramName) {
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
