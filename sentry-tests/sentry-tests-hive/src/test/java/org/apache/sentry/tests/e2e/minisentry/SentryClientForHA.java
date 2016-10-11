/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.tests.e2e.minisentry;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.service.thrift.SentryServiceClientFactory;
import org.apache.sentry.service.thrift.ServiceConstants;
import org.apache.sentry.tests.e2e.hive.AbstractTestWithStaticConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

public class SentryClientForHA extends AbstractTestWithStaticConfiguration{
  private static final Logger LOGGER = LoggerFactory.getLogger(SentryClientForHA.class);
  private SentryPolicyServiceClient client;

  public void start (String server_rpc_address, boolean poolEnabled) throws Exception{
    sentryConf = new Configuration(false);

    sentryConf.set(ServiceConstants.ClientConfig.SERVER_RPC_ADDRESS, server_rpc_address);
    sentryConf.set(ServiceConstants.ServerConfig.ALLOW_CONNECT, "hive");
    sentryConf.set(ServiceConstants.ServerConfig.SECURITY_MODE, "NONE");
    sentryConf.set(ServiceConstants.ClientConfig.SENTRY_POOL_RETRY_TOTAL, "4");
    sentryConf.setBoolean(ServiceConstants.ClientConfig.SENTRY_POOL_ENABLED, poolEnabled);
    Scanner scanner = new Scanner(System.in);
    String roleName;
    client = SentryServiceClientFactory.create(sentryConf);
    LOGGER.info("Connect to Sentry service: " + server_rpc_address);
    System.out.println("create / drop role or exit?");
    while (scanner.hasNext()) {
      String next = scanner.next();
      if (next.equalsIgnoreCase("exit")) {
        System.out.println("exit now. Bye!");
        break;
      } else if (next.startsWith("drop")){
        roleName = next.split("_")[1];
        drop(roleName);
      } else {
        roleName = next;
        create(roleName);
      }
      System.out.println("create / drop role or exit?");
    }
  }

  public void drop (String role) throws Exception {
    System.out.println("drop role: " + role);
    client.dropRoleIfExists("hive", role);
  }

  public void create (String role) throws Exception {
    System.out.println("create role: " + role);
    client.createRole("hive", role);
  }
  public static void main (String[] args) throws Exception {
    SentryClientForHA sentryClientForHA = new SentryClientForHA();
    sentryClientForHA.start("127.0.0.1:59081,127.0.0.1:59083", false);
  }
}
