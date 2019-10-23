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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.couchbase.Consistency;
import io.cdap.plugin.couchbase.CouchbaseConstants;
import io.cdap.plugin.couchbase.ErrorHandling;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * Tests of {@link CouchbaseSourceConfig} methods.
 */
public class CouchbaseSourceConfigTest {

  private static final String MOCK_STAGE = "mockstage";

  private static final Schema VALID_SCHEMA =
    Schema.recordOf("schema",
                    Schema.Field.of("int_field", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                    Schema.Field.of("long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                    Schema.Field.of("double_field", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("string_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("boolean_field", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                    Schema.Field.of("decimal_field", Schema.nullableOf(Schema.decimalOf(10, 4))));

  private static final CouchbaseSourceConfig VALID_CONFIG = CouchbaseSourceConfigBuilder.builder()
    .setReferenceName("CouchbaseSource")
    .setNodes("localhost")
    .setBucket("travel-sample")
    .setSelectFields("meta(`travel-sample`).id, *")
    .setUser("Administrator")
    .setPassword("password")
    .setNumSplits(1)
    .setOnError(ErrorHandling.FAIL_PIPELINE.getDisplayName())
    .setSchema(VALID_SCHEMA.toString())
    .setScanConsistency(Consistency.NOT_BOUNDED.getDisplayName())
    .setSampleSize(1000)
    .setMaxParallelism(0)
    .setQueryTimeout(600)
    .build();

  @Test
  public void testValidateValid() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testGetParsedSchema() {
    Assert.assertEquals(VALID_SCHEMA, VALID_CONFIG.getParsedSchema());
  }

  @Test
  public void testValidateReferenceNameNull() {
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setReferenceName(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, Constants.Reference.REFERENCE_NAME);
  }

  @Test
  public void testValidateReferenceNameEmpty() {
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setReferenceName("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, Constants.Reference.REFERENCE_NAME);
  }

  @Test
  public void testValidateReferenceNameInvalid() {
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setReferenceName("**********")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, Constants.Reference.REFERENCE_NAME);
  }

  @Test
  public void testValidateNodesNull() {
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setNodes(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.NODES);
  }

  @Test
  public void testValidateNodesEmpty() {
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setNodes("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.NODES);
  }

  @Test
  public void testValidateBucketNull() {
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setBucket(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.BUCKET);
  }

  @Test
  public void testValidateBucketEmpty() {
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setBucket("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.BUCKET);
  }

  @Test
  public void testValidateErrorHandlingNull() {
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setOnError(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.ON_ERROR);
  }

  @Test
  public void testValidateErrorHandlingEmpty() {
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setOnError("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.ON_ERROR);
  }

  @Test
  public void testValidateErrorHandlingInvalid() {
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setOnError("unknown-error-handling-strategy")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.ON_ERROR);
  }

  @Test
  public void testValidateScanConsistencyNull() {
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setScanConsistency(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.SCAN_CONSISTENCY);
  }

  @Test
  public void testValidateScanConsistencyEmpty() {
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setScanConsistency("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.SCAN_CONSISTENCY);
  }

  @Test
  public void testValidateScanConsistencyInvalid() {
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setScanConsistency("unknown-scan-consistency")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.SCAN_CONSISTENCY);
  }

  @Test
  public void testValidateSelectFieldsNull() {
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setSelectFields(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.SELECT_FIELDS);
  }

  @Test
  public void testValidateSelectFieldsEmpty() {
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setSelectFields("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.SELECT_FIELDS);
  }

  @Test
  public void testValidateQueryTimeoutZero() {
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setQueryTimeout(0)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.QUERY_TIMEOUT);
  }

  @Test
  public void testValidateQueryTimeoutInvalid() {
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setQueryTimeout(-100)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.QUERY_TIMEOUT);
  }

  @Test
  public void testValidateSampleSizeZero() {
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setSampleSize(0)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.SAMPLE_SIZE);
  }

  @Test
  public void testValidateSampleSizeInvalid() {
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setSampleSize(-100)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.SAMPLE_SIZE);
  }

  @Test
  public void testValidateNumSplitsInvalid() {
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setNumSplits(-100)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.NUM_SPLITS);
  }

  @Test
  public void testValidateUsernameNull() {
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setUser(null)
      .setPassword("username is null, but password specified")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.USERNAME);
  }

  @Test
  public void testValidateUsernameEmpty() {
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setUser("")
      .setPassword("username is empty, but password specified")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.USERNAME);
  }

  @Test
  public void testValidatePasswordNull() {
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setUser("username specified, but password is null")
      .setPassword(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.PASSWORD);
  }

  @Test
  public void testValidatePasswordEmpty() {
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setUser("username specified, but password is empty")
      .setPassword("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, CouchbaseConstants.PASSWORD);
  }

  @Test
  public void testValidateUsernameAndPasswordNull() {
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setUser(null)
      .setPassword(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateUsernameAndPasswordEmpty() {
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setUser("")
      .setPassword("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testNodeList() {
    List<String> nodeList = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setNodes("node1,node2,node3")
      .build()
      .getNodeList();

    Assert.assertEquals(Arrays.asList("node1", "node2", "node3"), nodeList);
  }

  @Test
  public void testValidateSchemaInvalid() {
    Schema schema = Schema.recordOf("invalid-schema", Schema.Field.of("unsupported", Schema.of(Schema.Type.BYTES)));
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setSchema(schema.toString())
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertOutputSchemaValidationFailed(failureCollector, "unsupported");
  }

  @Test
  public void testValidateComponentSchemaInvalid() {
    Schema schema = Schema.recordOf("invalid-schema",
                                    Schema.Field.of("unsupported", Schema.arrayOf(Schema.of(Schema.Type.BYTES))));
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setSchema(schema.toString())
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertOutputSchemaValidationFailed(failureCollector, "unsupported");
  }

  @Test
  public void testValidateMapSchemaInvalid() {
    Schema schema = Schema.recordOf(
      "invalid-schema",
      Schema.Field.of("unsupported-key", Schema.mapOf(Schema.of(Schema.Type.LONG), Schema.of(Schema.Type.STRING))));
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setSchema(schema.toString())
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertOutputSchemaValidationFailed(failureCollector, "unsupported-key");
  }

  @Test
  public void testValidateNestedFieldSchemaInvalid() {
    Schema nestedRecordSchema = Schema.recordOf("invalid-schema-nested",
                                                Schema.Field.of("nested", Schema.of(Schema.Type.BYTES)));
    Schema schema = Schema.recordOf("invalid-schema", Schema.Field.of("object", nestedRecordSchema));
    CouchbaseSourceConfig config = CouchbaseSourceConfigBuilder.builder(VALID_CONFIG)
      .setSchema(schema.toString())
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertOutputSchemaValidationFailed(failureCollector, "nested");
  }

  private static void assertOutputSchemaValidationFailed(MockFailureCollector failureCollector, String fieldName) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();
    Assert.assertEquals(1, failureList.size());
    ValidationFailure failure = failureList.get(0);
    List<ValidationFailure.Cause> causeList = getCauses(failure, CauseAttributes.OUTPUT_SCHEMA_FIELD);
    Assert.assertEquals(1, causeList.size());
    ValidationFailure.Cause cause = causeList.get(0);
    Assert.assertEquals(fieldName, cause.getAttribute(CauseAttributes.OUTPUT_SCHEMA_FIELD));
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
