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

import javax.annotation.Nullable;

/**
 * Builder class that provides handy methods to construct {@link CouchbaseConfig} for testing.
 */
public abstract class CouchbaseConfigBuilder<T extends CouchbaseConfigBuilder> {

  protected String referenceName;
  protected String nodes;
  protected String bucket;

  @Nullable
  protected String user;

  @Nullable
  protected String password;

  public T setReferenceName(String referenceName) {
    this.referenceName = referenceName;
    return (T) this;
  }

  public T setNodes(String nodes) {
    this.nodes = nodes;
    return (T) this;
  }

  public T setBucket(String bucket) {
    this.bucket = bucket;
    return (T) this;
  }

  public T setUser(@Nullable String user) {
    this.user = user;
    return (T) this;
  }

  public T setPassword(@Nullable String password) {
    this.password = password;
    return (T) this;
  }

  public abstract CouchbaseConfig build();
}
