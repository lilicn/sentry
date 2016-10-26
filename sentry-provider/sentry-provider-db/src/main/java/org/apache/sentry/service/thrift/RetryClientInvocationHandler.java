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
package org.apache.sentry.service.thrift;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClientDefaultImpl;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * The RetryClientInvocationHandler is a proxy class for handling thrift call for non-pool
 * model. Currently only one client connection is allowed, and it's using lazy connection.
 * The client is not connected to the sentry server until there is any rpc call.
 *
 * For every rpc call, if it is failed with connection problem, it will retry (reconnect
 * and resend the thrift call) with less than rpcRetryTotal times.
 * {@link #invokeImpl(Object, Method, Object[])}
 * During reconnection, it will firstly cycle through all the available sentry servers,
 * and then retry the whole server list with less than connectionFullRetryTotal times. In
 * this case, it won't introduce more latency when some server fails. Also to prevent all
 * clients connecting to the same server, it will reorder the endpoints randomly after a
 * full retry.
 * {@link SentryPolicyServiceClientDefaultImpl#connectWithRetry()}
 *
 * TODO: allow multiple client connections
 */
class RetryClientInvocationHandler extends SentryClientInvocationHandler{
  private static final Logger LOGGER =
      LoggerFactory.getLogger(RetryClientInvocationHandler.class);
  private final Configuration conf;
  private SentryPolicyServiceClientDefaultImpl client = null;
  private final int rpcRetryTotal;
  private boolean isConnected = false;

  RetryClientInvocationHandler(Configuration conf) throws IOException {
    this.conf = conf;
    Preconditions.checkNotNull(this.conf, "Configuration object cannot be null");
    this.rpcRetryTotal = conf.getInt(ServiceConstants.ClientConfig.SENTRY_RPC_RETRY_TOTAL,
        ServiceConstants.ClientConfig.SENTRY_RPC_RETRY_TOTAL_DEFAULT);
    client = new SentryPolicyServiceClientDefaultImpl(conf, false);
  }

  /**
   * For every rpc call, if it is failed with connection problem, it will retry (reconnect
   * and resend the thrift call) with less than rpcRetryTotal times.
   */
  @Override
  Object invokeImpl(Object proxy, Method method, Object[] args) throws Exception {
    int retryCount = 0;
    Exception lastExc;

    while (retryCount < rpcRetryTotal) {
      try {
        if (!isConnected) {
          connectWithRetry();
        }
        // do the thrift call
        return method.invoke(client, args);
      } catch (IOException e) {
        // Retry when the exception is caused by connection problem.
        isConnected = false;
        throw e;
      } catch (InvocationTargetException e) {
        // Get the target exception, check if SentryUserException or TTransportException is wrapped.
        // TTransportException means there has connection problem with the pool.
        Throwable targetException = e.getCause();
        if (targetException instanceof SentryUserException) {
          Throwable sentryTargetException = targetException.getCause();
          // If there has connection problem, eg, invalid connection if the service restarted,
          // sentryTargetException instanceof TTransportException = true.
          if (sentryTargetException instanceof TTransportException) {
            // Retry when the exception is caused by connection problem.
            isConnected = false;
            lastExc = new TTransportException(sentryTargetException);
          } else {
            // The exception is thrown by thrift call, eg, SentryAccessDeniedException.
            // Do not need to reconnect to the sentry server.
            throw (SentryUserException) targetException;
          }
        } else {
          throw e;
        }
      }

      // Increase the retry num, and throw the exception if reaching the max rpc retry num.
      retryCount++;
      if (retryCount == rpcRetryTotal) {
        LOGGER.error("Reach the max rpc retry num ", lastExc);
        throw new SentryUserException("Reach the max function retry num ", lastExc);
      }
    }
    return null;
  }

  @Override
  public void close() {
    client.close();
  }

  /**
   * Reconnect only when isConnected is false.
   */
  private synchronized void connectWithRetry() throws IOException {
    if (!isConnected) {
      client.connectWithRetry();
      isConnected = true;
    }
  }
}
