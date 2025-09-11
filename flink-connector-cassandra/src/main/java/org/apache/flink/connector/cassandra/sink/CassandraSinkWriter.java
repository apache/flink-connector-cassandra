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

package org.apache.flink.connector.cassandra.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.connector.cassandra.sink.config.CassandraSinkConfig;
import org.apache.flink.connector.cassandra.sink.config.write.RequestConfiguration;
import org.apache.flink.connector.cassandra.sink.exception.CassandraFailureHandler;
import org.apache.flink.connector.cassandra.sink.exception.MaxRetriesExceededException;
import org.apache.flink.connector.cassandra.sink.util.RecordWriterFactory;
import org.apache.flink.connector.cassandra.sink.writer.CassandraRecordWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.util.function.ThrowingRunnable;

import com.datastax.driver.core.ResultSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A {@link SinkWriter} implementation for Cassandra that handles async write operations with
 * Flink's MailboxExecutor.
 *
 * <p>This writer provides:
 *
 * <ul>
 *   <li>Semaphore-based backpressure control to limit concurrent requests
 *   <li>Retry logic with configurable maximum attempts
 *   <li>Fatal vs retryable exception classification
 *   <li>Comprehensive metrics (success, failure, retry counts, latency)
 * </ul>
 *
 * <h3>Error Handling Contract</h3>
 *
 * <p>This sink writer handles different types of failures according to the following contract:
 *
 * <ul>
 *   <li><strong>Permit acquisition timeout (backpressure):</strong> write() throws IOException
 *       immediately (synchronous). Flink's restart strategy applies for at-least-once delivery.
 *   <li><strong>Retriable driver errors</strong> (WriteTimeoutException, UnavailableException,
 *       NoHostAvailableException, OperationTimedOutException): Retries up to maxRetries. Success →
 *       counted as success; exhausted → handled as max retries exhausted.
 *   <li><strong>Max retries exhausted:</strong> Default CassandraFailureHandler throws IOException
 *       → task fails (at-least-once). Users can override to swallow for best-effort.
 *   <li><strong>Fatal/config errors</strong> (Authn/Authz, InvalidQueryException, SyntaxError,
 *       codec/driver config issues): Immediate IOException (no retry) for at-least-once delivery.
 * </ul>
 *
 * @param <INPUT> The input record type
 */
@Internal
class CassandraSinkWriter<INPUT> implements SinkWriter<INPUT> {

    private static final int PERMITS_PER_REQUEST = 1;

    /** Provider interface for creating CassandraRecordWriter instances. */
    @FunctionalInterface
    interface RecordWriterProvider<INPUT> {
        CassandraRecordWriter<INPUT> create(
                ClusterBuilder clusterBuilder, CassandraSinkConfig<INPUT> config);
    }

    private final Semaphore semaphore;
    private final MailboxExecutor mailboxExecutor;
    private final RequestConfiguration requestConfiguration;
    private final Counter successfulRecordsCounter;
    private final Counter failedRecordsCounter;
    private final Counter retryCounter;
    private final CassandraFailureHandler failureHandler;
    private final CassandraRecordWriter<INPUT> cassandraRecordWriter;
    private final Histogram writeLatencyHistogram;

    /** Creates a new CassandraSinkWriter. */
    public CassandraSinkWriter(
            RequestConfiguration requestConfiguration,
            CassandraSinkConfig<INPUT> cassandraSinkConfig,
            ClusterBuilder clusterBuilder,
            CassandraFailureHandler failureHandler,
            MailboxExecutor mailboxExecutor,
            SinkWriterMetricGroup metricGroup) {

        this(
                requestConfiguration,
                cassandraSinkConfig,
                clusterBuilder,
                failureHandler,
                mailboxExecutor,
                metricGroup,
                RecordWriterFactory::create);
    }

    /** Creates a new CassandraSinkWriter with injectable RecordWriterProvider for testing. */
    CassandraSinkWriter(
            RequestConfiguration requestConfiguration,
            CassandraSinkConfig<INPUT> cassandraSinkConfig,
            ClusterBuilder clusterBuilder,
            CassandraFailureHandler failureHandler,
            MailboxExecutor mailboxExecutor,
            SinkWriterMetricGroup metricGroup,
            RecordWriterProvider<INPUT> recordWriterProvider) {
        this.semaphore = new Semaphore(requestConfiguration.getMaxConcurrentRequests(), true);
        this.requestConfiguration = requestConfiguration;
        this.mailboxExecutor = mailboxExecutor;
        this.failureHandler = failureHandler;
        ClosureCleaner.clean(clusterBuilder, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
        this.successfulRecordsCounter = metricGroup.getIOMetricGroup().getNumRecordsOutCounter();
        this.failedRecordsCounter = metricGroup.getNumRecordsOutErrorsCounter();
        this.retryCounter = metricGroup.counter("retries");
        this.writeLatencyHistogram =
                metricGroup.histogram(
                        "writeLatencyMillis", new DescriptiveStatisticsHistogram(1000));
        metricGroup.gauge(
                "inflightRequests",
                () ->
                        requestConfiguration.getMaxConcurrentRequests()
                                - semaphore.availablePermits());
        this.cassandraRecordWriter =
                recordWriterProvider.create(clusterBuilder, cassandraSinkConfig);
    }

    /**
     * Executes a runnable on the mailbox executor, ensuring permits are released if scheduling
     * fails.
     *
     * @param action the task to execute
     * @param description description of the task for debugging
     */
    private void executeOnMailbox(
            ThrowingRunnable<? extends Exception> action, String description) {
        try {
            mailboxExecutor.execute(action, description);
        } catch (Throwable ex) {
            semaphore.release();
            throw ex;
        }
    }

    /**
     * Writes an input element to Cassandra asynchronously.
     *
     * @param input the input element
     * @param context the sink context
     * @throws IOException if an unrecoverable error occurs
     * @throws InterruptedException if the thread is interrupted while waiting for a permit
     */
    @Override
    public void write(INPUT input, Context context) throws IOException, InterruptedException {
        acquirePermitWithTimeout();
        submitWithRetry(input, 0);
    }

    /**
     * Acquires a permit with timeout.
     *
     * @throws IOException if timeout is exceeded
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    private void acquirePermitWithTimeout() throws IOException, InterruptedException {
        long startTime = System.nanoTime();
        long timeoutNanos = requestConfiguration.getMaxTimeout().toNanos();

        while (!semaphore.tryAcquire(PERMITS_PER_REQUEST)) {
            if (System.nanoTime() - startTime > timeoutNanos) {
                throw new IOException(
                        String.format(
                                "Failed to acquire %d out of %d permits to send value within %s",
                                PERMITS_PER_REQUEST,
                                requestConfiguration.getMaxConcurrentRequests(),
                                requestConfiguration.getMaxTimeout()));
            }
            mailboxExecutor.yield();
        }
    }

    /**
     * Submits a record for writing with retry logic.
     *
     * @param element the element to write
     * @param attempt current attempt number (0-based)
     */
    void submitWithRetry(INPUT element, int attempt) {
        try {
            long startTimeNanos = System.nanoTime();
            ListenableFuture<ResultSet> future = cassandraRecordWriter.write(element);

            Futures.addCallback(
                    future,
                    new FutureCallback<ResultSet>() {
                        @Override
                        public void onSuccess(@Nullable ResultSet resultSet) {
                            long cassandraLatencyMillis =
                                    TimeUnit.NANOSECONDS.toMillis(
                                            System.nanoTime() - startTimeNanos);
                            executeOnMailbox(
                                    () -> {
                                        writeLatencyHistogram.update(cassandraLatencyMillis);
                                        semaphore.release();
                                        successfulRecordsCounter.inc();
                                    },
                                    "cassandra-success");
                        }

                        @Override
                        public void onFailure(@Nonnull Throwable t) {
                            executeOnMailbox(
                                    () -> handleFailure(element, t, attempt), "cassandra-failure");
                        }
                    },
                    MoreExecutors.directExecutor());

        } catch (Exception e) {
            executeOnMailbox(() -> handleFailure(element, e, attempt), "handle-sync-write-failure");
        }
    }

    /**
     * Handles write failures with retry logic and failure classification.
     *
     * @param element the element that failed to write
     * @param cause the throwable that caused the failure
     * @param attempt current attempt number (0-based)
     * @throws IOException if the failure should fail the sink
     */
    private void handleFailure(INPUT element, Throwable cause, int attempt) throws IOException {
        final boolean isFatal = failureHandler.isFatal(cause);
        final int maxRetries = requestConfiguration.getMaxRetries();
        final boolean hasRetriesLeft = !isFatal && attempt < maxRetries;

        if (hasRetriesLeft) {
            retryCounter.inc();
            submitWithRetry(element, attempt + 1); // keep the permit
            return;
        }

        // Terminal path
        failedRecordsCounter.inc();
        try {
            if (!isFatal) {
                // retries exhausted but not fatal => surface as MaxRetriesExceededException
                failureHandler.onFailure(
                        new MaxRetriesExceededException(element, attempt + 1, cause));
            } else {
                // fatal => immediate fail
                failureHandler.onFailure(cause);
            }
        } finally {
            semaphore.release();
        }
    }

    /**
     * Flushes all pending write operations by waiting for all in-flight requests to complete.
     *
     * @param endOfInput whether this is the final flush call
     * @throws IOException if an error occurs during flushing
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        try {
            executePendingTasks();
        } catch (TimeoutException e) {
            throw new IOException("Unable to flush within timeout: " + e.getMessage(), e);
        }
    }

    /**
     * Waits for all pending tasks to complete within the configured timeout.
     *
     * @throws InterruptedException if the thread is interrupted while waiting
     * @throws TimeoutException if the timeout is exceeded
     */
    void executePendingTasks() throws InterruptedException, TimeoutException {
        final int maxConcurrentRequests = requestConfiguration.getMaxConcurrentRequests();
        final long deadlineNanos =
                System.nanoTime() + requestConfiguration.getFlushTimeout().toNanos();

        if (semaphore.availablePermits() >= maxConcurrentRequests) {
            return;
        }

        while (System.nanoTime() < deadlineNanos) {
            if (semaphore.availablePermits() >= maxConcurrentRequests) {
                return;
            }
            mailboxExecutor.yield();
        }

        final int available = semaphore.availablePermits();
        final int pendingPermits = Math.max(0, maxConcurrentRequests - available);

        throw new TimeoutException(
                String.format(
                        "Timeout after %s while waiting for %d in-flight Cassandra requests to complete. "
                                + "Only %d of %d permits available.",
                        requestConfiguration.getFlushTimeout(),
                        pendingPermits,
                        available,
                        maxConcurrentRequests));
    }

    /**
     * Closes the sink writer, waiting for all pending operations to complete.
     *
     * @throws Exception if an error occurs during cleanup
     */
    @Override
    public void close() throws Exception {
        try {
            executePendingTasks();
        } finally {
            cassandraRecordWriter.close();
        }
    }
}
