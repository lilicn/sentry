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

import org.apache.sentry.tests.e2e.hive.AbstractTestWithStaticConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

public class SentryServicesForHA extends AbstractTestWithStaticConfiguration {
  private static final Logger LOGGER = LoggerFactory.getLogger(SentryServicesForHA.class);
  public void start () throws Exception {
    useSentryService = true;
    enableSentryHA = true;
    clearDbPerTest = false;
    enableSentryWeb = true;
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
  }

  public static void main (String[] args) throws Exception {
    LOGGER.info("create sentry services...");
    final SentryServicesForHA sentryServicesForHA = new SentryServicesForHA();
    new Thread() {
      public void run () {
        try {
          sentryServicesForHA.start();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }.start();

    Scanner scanner = new Scanner(System.in);

    while (scanner.hasNext()) {
      String next = scanner.next();
      if (next.equalsIgnoreCase("help")) {
        System.out.println("Sentry services started: " + sentryServicesForHA.sentryServer.get(0).getAddress()
            + ", " + sentryServicesForHA.sentryServer.get(1).getAddress());
      } else if (next.equalsIgnoreCase("stop_1")) {
        System.out.println("Stop server " + sentryServicesForHA.sentryServer.get(1).getAddress());
        if (sentryServicesForHA.sentryServer.get(1).isRunning()) {
          sentryServicesForHA.sentryServer.stop(1);
        } else {
          System.out.println("server 1 is not running, no need to stop.");
        }
      } else if (next.equalsIgnoreCase("stop_0")) {
        System.out.println("Stop server: " + sentryServicesForHA.sentryServer.get(0).getAddress());
        if (sentryServicesForHA.sentryServer.get(0).isRunning()) {
          sentryServicesForHA.sentryServer.stop(0);
        } else {
          System.out.println("server 0 is not running, no need to stop.");
        }
      } else if (next.equalsIgnoreCase("stop_all")) {
        System.out.println("Stop all ...");
        sentryServicesForHA.sentryServer.stopAll();
      } else if (next.equalsIgnoreCase("start_1")) {
        System.out.println("Start server: " + sentryServicesForHA.sentryServer.get(1).getAddress());
        if (!sentryServicesForHA.sentryServer.get(1).isRunning()) {
          sentryServicesForHA.sentryServer.start(1);
        } else {
          System.out.println("server 1 is already running, no need to start.");
        }
      } else if (next.equalsIgnoreCase("start_0")) {
        System.out.println("Start server: " + sentryServicesForHA.sentryServer.get(0).getAddress());
        if (!sentryServicesForHA.sentryServer.get(0).isRunning()) {
          sentryServicesForHA.sentryServer.start(0);
        } else {
          System.out.println("server 0 is already running, no need to start.");
        }
      } else if (next.equalsIgnoreCase("exit")){
        System.out.println("Stop all and exit! Bye!");
        sentryServicesForHA.sentryServer.stopAll();
        System.exit(0);
      }
      System.out.println("stop / start / exit ?");
    }


  }
}
