/*
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

package org.apache.flink.connector.cassandra.sink.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;

/**
 * Handler for Cassandra write failures in the sink writer.
 *
 * <p>This handler provides policies for how to respond to failures during Cassandra writes,
 * including fatal exception classification and failure reporting.
 *
 * <p>The handler determines whether exceptions should be treated as fatal (failing the sink) or
 * retriable (subject to retry logic based on the configured retry policy).
 *
 * <p><strong>Important:</strong> When the sink exhausts its retry budget for a record, it calls
 * {@link #onFailure(Throwable)} with a {@link MaxRetriesExceededException}. The default handler
 * fails the job to preserve at-least-once semantics. Providing a custom classifier does not change
 * this; to opt into best-effort delivery, subclass the handler and override {@code onFailure}.
 */
public class CassandraFailureHandler implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraFailureHandler.class);

    private static final long serialVersionUID = 1L;

    /**
     * Determines whether an exception should be considered fatal and fail the sink.
     *
     * @param cause the exception to classify
     * @return true if the exception is fatal and should fail the sink
     */
    public boolean isFatal(Throwable cause) {
        return CassandraFatalExceptionClassifier.getFatalExceptionMessage(cause).isPresent();
    }

    /**
     * Central failure policy that handles terminal failures.
     *
     * <p>This method is called when a write operation fails terminally (either immediately fatal or
     * after exhausting retries). The default implementation:
     *
     * <ul>
     *   <li>Always fails on {@link MaxRetriesExceededException} to preserve at-least-once semantics
     *   <li>Fails on exceptions classified as fatal by the classifier
     *   <li>Logs and swallows other non-fatal exceptions (best-effort for transient errors)
     * </ul>
     *
     * <p>Users who want best-effort delivery (allowing record loss) can override this method to
     * swallow {@link MaxRetriesExceededException}.
     *
     * @param cause the exception that caused the failure
     * @throws IOException if the failure should fail the sink
     */
    public void onFailure(Throwable cause) throws IOException {
        if (cause instanceof MaxRetriesExceededException) {
            throw new IOException(
                    "Cassandra write failed after exhausting retries. "
                            + "To allow record loss, override CassandraFailureHandler.onFailure()",
                    cause);
        }
        Optional<String> fatalMessage =
                CassandraFatalExceptionClassifier.getFatalExceptionMessage(cause);
        if (fatalMessage.isPresent()) {
            throw new IOException(fatalMessage.get(), cause);
        }
        LOG.warn(
                "Ignoring non-fatal Cassandra error (best-effort delivery): {}",
                cause.getMessage(),
                cause);
    }
}
