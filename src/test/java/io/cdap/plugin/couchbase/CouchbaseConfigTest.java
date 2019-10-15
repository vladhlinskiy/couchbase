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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.common.Constants;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests of {@link CouchbaseConfig} methods.
 */
public class CouchbaseConfigTest {

  private static final String MOCK_STAGE_NAME = "mockstage";
  private static final String STAGE = "stage";

  private static final Schema VALID_SCHEMA =
    Schema.recordOf("schema",
                    Schema.Field.of("int_field", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                    Schema.Field.of("long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                    Schema.Field.of("double_field", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("string_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("boolean_field", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                    Schema.Field.of("decimal_field", Schema.nullableOf(Schema.decimalOf(10, 4))));

  private static final CouchbaseConfig VALID_CONFIG = CouchbaseConfigBuilder.builder()
    .setReferenceName("CouchbaseSource")
    .setNodes("localhost")
    .setBucket("travel-sample")
    .setQuery("SELECT meta(`travel-sample`).id, * from `travel-sample`")
    .setUser("Administrator")
    .setPassword("password")
    .setOnError(ErrorHandling.FAIL_PIPELINE.getDisplayName())
    .setSchema(VALID_SCHEMA.toString())
    .build();

  @Test
  public void testValidateValid() {
    try {
      VALID_CONFIG.validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      // MockFailureCollector throws an exception even if there are no validation errors
      Assert.assertTrue(e.getFailures().isEmpty());
    }
  }

  @Test
  public void testGetParsedSchema() {
    Assert.assertEquals(VALID_SCHEMA, VALID_CONFIG.getParsedSchema());
  }

  @Test
  public void testValidateReferenceNameNull() {
    try {
      CouchbaseConfigBuilder.builder(VALID_CONFIG)
        .setReferenceName(null)
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure validationFailure = e.getFailures().get(0);
      Assert.assertEquals("Reference name must be specified", validationFailure.getMessage());
      Assert.assertEquals(1, validationFailure.getCauses().size());
      ValidationFailure.Cause cause = validationFailure.getCauses().get(0);
      Assert.assertEquals(Constants.Reference.REFERENCE_NAME, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(MOCK_STAGE_NAME, cause.getAttribute(STAGE));
    }
  }

  @Test
  public void testValidateReferenceNameEmpty() {
    try {
      CouchbaseConfigBuilder.builder(VALID_CONFIG)
        .setReferenceName("")
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure validationFailure = e.getFailures().get(0);
      Assert.assertEquals("Reference name must be specified", validationFailure.getMessage());
      Assert.assertEquals(1, validationFailure.getCauses().size());
      ValidationFailure.Cause cause = validationFailure.getCauses().get(0);
      Assert.assertEquals(Constants.Reference.REFERENCE_NAME, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(MOCK_STAGE_NAME, cause.getAttribute(STAGE));
    }
  }

  @Test
  public void testValidateReferenceNameInvalid() {
    try {
      CouchbaseConfigBuilder.builder(VALID_CONFIG)
        .setReferenceName("**********")
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure validationFailure = e.getFailures().get(0);
      Assert.assertEquals("Invalid reference name", validationFailure.getMessage());
      Assert.assertEquals(1, validationFailure.getCauses().size());
      ValidationFailure.Cause cause = validationFailure.getCauses().get(0);
      Assert.assertEquals(Constants.Reference.REFERENCE_NAME, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(MOCK_STAGE_NAME, cause.getAttribute(STAGE));
    }
  }

  @Test
  public void testValidateNodesNull() {
    try {
      CouchbaseConfigBuilder.builder(VALID_CONFIG)
        .setNodes(null)
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure validationFailure = e.getFailures().get(0);
      Assert.assertEquals("Couchbase nodes must be specified", validationFailure.getMessage());
      Assert.assertEquals(1, validationFailure.getCauses().size());
      ValidationFailure.Cause cause = validationFailure.getCauses().get(0);
      Assert.assertEquals(CouchbaseConstants.NODES, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(MOCK_STAGE_NAME, cause.getAttribute(STAGE));
    }
  }

  @Test
  public void testValidateNodesEmpty() {
    try {
      CouchbaseConfigBuilder.builder(VALID_CONFIG)
        .setNodes("")
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure validationFailure = e.getFailures().get(0);
      Assert.assertEquals("Couchbase nodes must be specified", validationFailure.getMessage());
      Assert.assertEquals(1, validationFailure.getCauses().size());
      ValidationFailure.Cause cause = validationFailure.getCauses().get(0);
      Assert.assertEquals(CouchbaseConstants.NODES, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(MOCK_STAGE_NAME, cause.getAttribute(STAGE));
    }
  }

  @Test
  public void testValidateBucketNull() {
    try {
      CouchbaseConfigBuilder.builder(VALID_CONFIG)
        .setBucket(null)
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure validationFailure = e.getFailures().get(0);
      Assert.assertEquals("Bucket name must be specified", validationFailure.getMessage());
      Assert.assertEquals(1, validationFailure.getCauses().size());
      ValidationFailure.Cause cause = validationFailure.getCauses().get(0);
      Assert.assertEquals(CouchbaseConstants.BUCKET, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(MOCK_STAGE_NAME, cause.getAttribute(STAGE));
    }
  }

  @Test
  public void testValidateBucketEmpty() {
    try {
      CouchbaseConfigBuilder.builder(VALID_CONFIG)
        .setBucket("")
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure validationFailure = e.getFailures().get(0);
      Assert.assertEquals("Bucket name must be specified", validationFailure.getMessage());
      Assert.assertEquals(1, validationFailure.getCauses().size());
      ValidationFailure.Cause cause = validationFailure.getCauses().get(0);
      Assert.assertEquals(CouchbaseConstants.BUCKET, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(MOCK_STAGE_NAME, cause.getAttribute(STAGE));
    }
  }

  @Test
  public void testValidateErrorHandlingNull() {
    try {
      CouchbaseConfigBuilder.builder(VALID_CONFIG)
        .setOnError(null)
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure validationFailure = e.getFailures().get(0);
      Assert.assertEquals("Error handling must be specified", validationFailure.getMessage());
      Assert.assertEquals(1, validationFailure.getCauses().size());
      ValidationFailure.Cause cause = validationFailure.getCauses().get(0);
      Assert.assertEquals(CouchbaseConstants.ON_ERROR, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(MOCK_STAGE_NAME, cause.getAttribute(STAGE));
    }
  }

  @Test
  public void testValidateErrorHandlingEmpty() {
    try {
      CouchbaseConfigBuilder.builder(VALID_CONFIG)
        .setOnError("")
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure validationFailure = e.getFailures().get(0);
      Assert.assertEquals("Error handling must be specified", validationFailure.getMessage());
      Assert.assertEquals(1, validationFailure.getCauses().size());
      ValidationFailure.Cause cause = validationFailure.getCauses().get(0);
      Assert.assertEquals(CouchbaseConstants.ON_ERROR, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(MOCK_STAGE_NAME, cause.getAttribute(STAGE));
    }
  }

  @Test
  public void testValidateErrorHandlingInvalid() {
    try {
      CouchbaseConfigBuilder.builder(VALID_CONFIG)
        .setOnError("unknown-error-handling-strategy")
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure validationFailure = e.getFailures().get(0);
      Assert.assertEquals("Invalid record error handling strategy name", validationFailure.getMessage());
      Assert.assertEquals(1, validationFailure.getCauses().size());
      ValidationFailure.Cause cause = validationFailure.getCauses().get(0);
      Assert.assertEquals(CouchbaseConstants.ON_ERROR, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(MOCK_STAGE_NAME, cause.getAttribute(STAGE));
    }
  }

  @Test
  public void testValidateQueryNull() {
    try {
      CouchbaseConfigBuilder.builder(VALID_CONFIG)
        .setQuery(null)
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure validationFailure = e.getFailures().get(0);
      Assert.assertEquals("Query must be specified", validationFailure.getMessage());
      Assert.assertEquals(1, validationFailure.getCauses().size());
      ValidationFailure.Cause cause = validationFailure.getCauses().get(0);
      Assert.assertEquals(CouchbaseConstants.QUERY, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(MOCK_STAGE_NAME, cause.getAttribute(STAGE));
    }
  }

  @Test
  public void testValidateQueryEmpty() {
    try {
      CouchbaseConfigBuilder.builder(VALID_CONFIG)
        .setQuery("")
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure validationFailure = e.getFailures().get(0);
      Assert.assertEquals("Query must be specified", validationFailure.getMessage());
      Assert.assertEquals(1, validationFailure.getCauses().size());
      ValidationFailure.Cause cause = validationFailure.getCauses().get(0);
      Assert.assertEquals(CouchbaseConstants.QUERY, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(MOCK_STAGE_NAME, cause.getAttribute(STAGE));
    }
  }

  @Test
  public void testValidateSchemaNull() {
    try {
      CouchbaseConfigBuilder.builder(VALID_CONFIG)
        .setSchema(null)
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure validationFailure = e.getFailures().get(0);
      Assert.assertEquals("Output schema must be specified", validationFailure.getMessage());
      Assert.assertEquals(1, validationFailure.getCauses().size());
      ValidationFailure.Cause cause = validationFailure.getCauses().get(0);
      Assert.assertEquals(CouchbaseConstants.SCHEMA, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(MOCK_STAGE_NAME, cause.getAttribute(STAGE));
    }
  }

  @Test
  public void testValidateSchemaEmpty() {
    try {
      CouchbaseConfigBuilder.builder(VALID_CONFIG)
        .setSchema("")
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure validationFailure = e.getFailures().get(0);
      Assert.assertEquals("Output schema must be specified", validationFailure.getMessage());
      Assert.assertEquals(1, validationFailure.getCauses().size());
      ValidationFailure.Cause cause = validationFailure.getCauses().get(0);
      Assert.assertEquals(CouchbaseConstants.SCHEMA, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(MOCK_STAGE_NAME, cause.getAttribute(STAGE));
    }
  }

  @Test
  public void testValidateUsernameNull() {
    try {
      CouchbaseConfigBuilder.builder(VALID_CONFIG)
        .setUser(null)
        .setPassword("username is null, but password specified")
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure validationFailure = e.getFailures().get(0);
      Assert.assertEquals("Username must be specified", validationFailure.getMessage());
      Assert.assertEquals(1, validationFailure.getCauses().size());
      ValidationFailure.Cause cause = validationFailure.getCauses().get(0);
      Assert.assertEquals(CouchbaseConstants.USERNAME, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(MOCK_STAGE_NAME, cause.getAttribute(STAGE));
    }
  }

  @Test
  public void testValidateUsernameEmpty() {
    try {
      CouchbaseConfigBuilder.builder(VALID_CONFIG)
        .setUser("")
        .setPassword("username is empty, but password specified")
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure validationFailure = e.getFailures().get(0);
      Assert.assertEquals("Username must be specified", validationFailure.getMessage());
      Assert.assertEquals(1, validationFailure.getCauses().size());
      ValidationFailure.Cause cause = validationFailure.getCauses().get(0);
      Assert.assertEquals(CouchbaseConstants.USERNAME, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(MOCK_STAGE_NAME, cause.getAttribute(STAGE));
    }
  }

  @Test
  public void testValidatePasswordNull() {
    try {
      CouchbaseConfigBuilder.builder(VALID_CONFIG)
        .setUser("username specified, but password is null")
        .setPassword(null)
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure validationFailure = e.getFailures().get(0);
      Assert.assertEquals("Password must be specified", validationFailure.getMessage());
      Assert.assertEquals(1, validationFailure.getCauses().size());
      ValidationFailure.Cause cause = validationFailure.getCauses().get(0);
      Assert.assertEquals(CouchbaseConstants.PASSWORD, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(MOCK_STAGE_NAME, cause.getAttribute(STAGE));
    }
  }

  @Test
  public void testValidatePasswordEmpty() {
    try {
      CouchbaseConfigBuilder.builder(VALID_CONFIG)
        .setUser("username specified, but password is empty")
        .setPassword("")
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure validationFailure = e.getFailures().get(0);
      Assert.assertEquals("Password must be specified", validationFailure.getMessage());
      Assert.assertEquals(1, validationFailure.getCauses().size());
      ValidationFailure.Cause cause = validationFailure.getCauses().get(0);
      Assert.assertEquals(CouchbaseConstants.PASSWORD, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(MOCK_STAGE_NAME, cause.getAttribute(STAGE));
    }
  }

  @Test
  public void testValidateUsernameAndPasswordNull() {
    try {
      CouchbaseConfigBuilder.builder(VALID_CONFIG)
        .setUser(null)
        .setPassword(null)
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      // MockFailureCollector throws an exception even if there are no validation errors
      Assert.assertTrue(e.getFailures().isEmpty());
    }
  }

  @Test
  public void testValidateUsernameAndPasswordEmpty() {
    try {
      CouchbaseConfigBuilder.builder(VALID_CONFIG)
        .setUser("")
        .setPassword("")
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      // MockFailureCollector throws an exception even if there are no validation errors
      Assert.assertTrue(e.getFailures().isEmpty());
    }
  }

  @Test
  public void testNodeList() {
    List<String> nodeList = CouchbaseConfigBuilder.builder(VALID_CONFIG)
      .setNodes("node1,node2,node3")
      .build()
      .getNodeList();

    Assert.assertEquals(Arrays.asList("node1", "node2", "node3"), nodeList);
  }

  @Test
  public void testValidateSchemaInvalid() {
    Schema schema = Schema.recordOf("invalid-schema", Schema.Field.of("unsupported", Schema.of(Schema.Type.BYTES)));
    try {
      CouchbaseConfigBuilder.builder(VALID_CONFIG)
        .setSchema(schema.toString())
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      // MockFailureCollector throws an exception even if there are no validation errors
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure validationFailure = e.getFailures().get(0);
      Assert.assertTrue(validationFailure.getMessage().contains("Field 'unsupported' is of unsupported type 'bytes'."));
      Assert.assertEquals(1, validationFailure.getCauses().size());
      ValidationFailure.Cause cause = validationFailure.getCauses().get(0);
      Assert.assertEquals("unsupported", cause.getAttribute(CauseAttributes.OUTPUT_SCHEMA_FIELD));
      Assert.assertEquals(MOCK_STAGE_NAME, cause.getAttribute(STAGE));
    }
  }

  @Test
  public void testValidateComponentSchemaInvalid() {
    Schema schema = Schema.recordOf("invalid-schema",
                                    Schema.Field.of("unsupported", Schema.arrayOf(Schema.of(Schema.Type.BYTES))));
    try {
      CouchbaseConfigBuilder.builder(VALID_CONFIG)
        .setSchema(schema.toString())
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      // MockFailureCollector throws an exception even if there are no validation errors
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure validationFailure = e.getFailures().get(0);
      Assert.assertTrue(validationFailure.getMessage().contains("Field 'unsupported' is of unsupported type 'bytes'."));
      Assert.assertEquals(1, validationFailure.getCauses().size());
      ValidationFailure.Cause cause = validationFailure.getCauses().get(0);
      Assert.assertEquals("unsupported", cause.getAttribute(CauseAttributes.OUTPUT_SCHEMA_FIELD));
      Assert.assertEquals(MOCK_STAGE_NAME, cause.getAttribute(STAGE));
    }
  }

  @Test
  public void testValidateMapSchemaInvalid() {
    Schema schema = Schema.recordOf(
      "invalid-schema",
      Schema.Field.of("unsupported-key", Schema.mapOf(Schema.of(Schema.Type.LONG), Schema.of(Schema.Type.STRING))));
    try {
      CouchbaseConfigBuilder.builder(VALID_CONFIG)
        .setSchema(schema.toString())
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      // MockFailureCollector throws an exception even if there are no validation errors
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure validationFailure = e.getFailures().get(0);
      Assert.assertEquals("Map keys must be a non-nullable string", validationFailure.getMessage());
      Assert.assertEquals(1, validationFailure.getCauses().size());
      ValidationFailure.Cause cause = validationFailure.getCauses().get(0);
      Assert.assertEquals("unsupported-key", cause.getAttribute(CauseAttributes.OUTPUT_SCHEMA_FIELD));
      Assert.assertEquals(MOCK_STAGE_NAME, cause.getAttribute(STAGE));
    }
  }

  @Test
  public void testValidateNestedFieldSchemaInvalid() {
    Schema nestedRecordSchema = Schema.recordOf("invalid-schema-nested",
                                                Schema.Field.of("nested", Schema.of(Schema.Type.BYTES)));
    Schema schema = Schema.recordOf("invalid-schema", Schema.Field.of("object", nestedRecordSchema));
    try {
      CouchbaseConfigBuilder.builder(VALID_CONFIG)
        .setSchema(schema.toString())
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      // MockFailureCollector throws an exception even if there are no validation errors
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure validationFailure = e.getFailures().get(0);
      Assert.assertTrue(validationFailure.getMessage().contains("Field 'nested' is of unsupported type 'bytes'."));
      Assert.assertEquals(1, validationFailure.getCauses().size());
      ValidationFailure.Cause cause = validationFailure.getCauses().get(0);
      Assert.assertEquals("nested", cause.getAttribute(CauseAttributes.OUTPUT_SCHEMA_FIELD));
      Assert.assertEquals(MOCK_STAGE_NAME, cause.getAttribute(STAGE));
    }
  }
}
