/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.core.model.kafka;

/**
 * Represents Cluster authorizable in Kafka model.
 */
public class Cluster implements KafkaAuthorizable {
  private String name;

  /**
   * Create a Cluster authorizable for Kafka cluster of a given name.
   *
   * @param name Name of Kafka cluster.
   */
  public Cluster(String name) {
    this.name = name;
  }

  /**
   * Get type of Kafka's cluster authorizable.
   *
   * @return Type of Kafka's cluster authorizable.
   */
  @Override
  public AuthorizableType getAuthzType() {
    return AuthorizableType.CLUSTER;
  }

  /**
   * Get name of Kafka's cluster.
   *
   * @return Name of Kafka's cluster.
   */
  @Override
  public String getName() {
    return name;
  }

  /**
   * Get type name of Kafka's cluster authorizable.
   *
   * @return Type name of Kafka's cluster authorizable.
   */
  @Override
  public String getTypeName() {
    return getAuthzType().name();
  }
}