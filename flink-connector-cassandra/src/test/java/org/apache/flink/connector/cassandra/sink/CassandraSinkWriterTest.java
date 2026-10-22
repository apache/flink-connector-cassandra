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

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.connector.cassandra.sink.config.CqlSinkConfig;
import org.apache.flink.connector.cassandra.sink.config.write.RequestConfiguration;
import org.apache.flink.connector.cassandra.sink.exception.CassandraFailureHandler;
import org.apache.flink.connector.cassandra.sink.exception.MaxRetriesExceededException;
import org.apache.flink.connector.cassandra.sink.writer.CassandraRecordWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.types.Row;
import org.apache.flink.util.function.ThrowingRunnable;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.RejectedExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Unit tests for {@link CassandraSinkWriter}. */
public class CassandraSinkWriterTest {

    @Mock private SinkWriterMetricGroup metricGroup;
    private final TestCounter successCounter = new TestCounter();
    private final TestCounter failedCounter = new TestCounter();
    private final TestCounter retryCounter = new TestCounter();
    @Mock private Histogram latencyHistogram;
    @Mock private OperatorIOMetricGroup ioMetricGroup;
    private ClusterBuilder clusterBuilder;

    private CassandraFailureHandler failureHandler;
    private CqlSinkConfig<Row> sinkConfig;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        // Setup metric group mocking
        when(metricGroup.getIOMetricGroup()).thenReturn(ioMetricGroup);
        when(ioMetricGroup.getNumRecordsOutCounter()).thenReturn(successCounter);
        when(metricGroup.getNumRecordsOutErrorsCounter()).thenReturn(failedCounter);
        when(metricGroup.counter("retries")).thenReturn(retryCounter);
        when(metricGroup.histogram(any(), any())).thenReturn(latencyHistogram);

        // Create a simple serializable cluster builder for testing
        clusterBuilder = new SerializableClusterBuilder();

        failureHandler = new CassandraFailureHandler();
        sinkConfig = CqlSinkConfig.forRow();
    }

    /** Simple test counter that tracks increments. */
    private static final class TestCounter implements Counter {
        private long count = 0;

        @Override
        public void inc() {
            count++;
        }

        @Override
        public void inc(long n) {
            count += n;
        }

        @Override
        public void dec() {
            count--;
        }

        @Override
        public void dec(long n) {
            count -= n;
        }

        @Override
        public long getCount() {
            return count;
        }
    }

    /** Simple serializable cluster builder for testing. */
    private static final class SerializableClusterBuilder extends ClusterBuilder {
        @Override
        protected Cluster buildCluster(Cluster.Builder builder) {
            return null; // Not needed for tests
        }
    }

    /** Direct mailbox executor that runs commands immediately for testing. */
    private static final class DirectMailbox implements MailboxExecutor {
        @Override
        public void execute(
                ThrowingRunnable<? extends Exception> command,
                String descriptionFormat,
                Object... descriptionArgs) {
            try {
                command.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void yield() {
            /* no-op */
        }

        @Override
        public boolean tryYield() {
            return false;
        }
    }

    /** Mailbox executor that always rejects execution for testing permit leak scenarios. */
    private static final class RejectingMailbox implements MailboxExecutor {
        @Override
        public void execute(
                ThrowingRunnable<? extends Exception> command,
                String descriptionFormat,
                Object... descriptionArgs) {
            throw new RejectedExecutionException("Test rejection");
        }

        @Override
        public void yield() {
            /* no-op */
        }

        @Override
        public boolean tryYield() {
            return false;
        }
    }

    /** Mock record writer that can be configured to return specific futures. */
    private static final class ControllableRecordWriter implements CassandraRecordWriter<Row> {
        private ListenableFuture<ResultSet> nextResult;
        private int callCount = 0;
        private ListenableFuture<ResultSet> firstCallResult;
        private ListenableFuture<ResultSet> secondCallResult;

        void setNextResult(ListenableFuture<ResultSet> future) {
            this.nextResult = future;
        }

        void setFirstThenSecondResult(
                ListenableFuture<ResultSet> first, ListenableFuture<ResultSet> second) {
            this.firstCallResult = first;
            this.secondCallResult = second;
        }

        @Override
        public Session getSession() {
            return mock(Session.class);
        }

        @Override
        public Statement prepareStatement(Row input) {
            return mock(Statement.class);
        }

        @Override
        public ListenableFuture<ResultSet> executeStatement(Statement statement) {
            callCount++;

            if (firstCallResult != null && secondCallResult != null) {
                if (callCount == 1) {
                    return firstCallResult;
                } else if (callCount == 2) {
                    return secondCallResult;
                }
            }

            if (nextResult == null) {
                throw new IllegalStateException("No result configured");
            }
            return nextResult;
        }

        @Override
        public ListenableFuture<ResultSet> write(Row record) {
            return executeStatement(prepareStatement(record));
        }

        int getCallCount() {
            return callCount;
        }

        @Override
        public void close() {
            // no-op
        }
    }

    /** Creates a RequestConfiguration.Builder with tiny values for timeout tests. */
    private RequestConfiguration.Builder tinyRequestConfig() {
        return RequestConfiguration.builder()
                .setMaxConcurrentRequests(1)
                .setMaxRetries(1)
                .setMaxTimeout(Duration.ofMillis(10));
    }

    @Test
    void basicTest() {
        // Just verify the class loads and basic setup works
        assertThat(failureHandler).isNotNull();
        assertThat(sinkConfig).isNotNull();
        assertThat(clusterBuilder).isNotNull();
    }

    @Test
    void testExceptionClassification() {
        // Based on actual CassandraFatalExceptionClassifier behavior:
        // InvalidQueryException IS fatal (non-retriable)
        assertThat(failureHandler.isFatal(new InvalidQueryException("test"))).isTrue();

        // ReadTimeoutException and UnavailableException are NOT fatal (retriable)
        ReadTimeoutException timeout = new ReadTimeoutException(null, null, 0, 1, false);
        assertThat(failureHandler.isFatal(timeout)).isFalse();

        UnavailableException unavailable = new UnavailableException(null, null, 1, 0);
        assertThat(failureHandler.isFatal(unavailable)).isFalse();
    }

    @Test
    void testRequestConfigurationBuilder() {
        // Test that request configuration can be built with various parameters
        RequestConfiguration config =
                RequestConfiguration.builder()
                        .setMaxConcurrentRequests(5)
                        .setMaxRetries(3)
                        .setMaxTimeout(Duration.ofSeconds(30))
                        .build();

        assertThat(config.getMaxConcurrentRequests()).isEqualTo(5);
        assertThat(config.getMaxRetries()).isEqualTo(3);
        assertThat(config.getMaxTimeout()).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void testTinyRequestConfig() {
        // Test the helper method for creating test configurations
        RequestConfiguration config = tinyRequestConfig().build();

        assertThat(config.getMaxConcurrentRequests()).isEqualTo(1);
        assertThat(config.getMaxRetries()).isEqualTo(1);
        assertThat(config.getMaxTimeout()).isEqualTo(Duration.ofMillis(10));
    }

    @Test
    void testFailureHandlerCreation() {
        // Test that failure handler can be created and basic methods work
        CassandraFailureHandler handler = new CassandraFailureHandler();
        assertThat(handler).isNotNull();

        // Test with various exception types
        InvalidQueryException invalidQuery = new InvalidQueryException("Invalid CQL");
        assertThat(handler.isFatal(invalidQuery)).isTrue();

        ReadTimeoutException timeout = new ReadTimeoutException(null, null, 0, 1, false);
        assertThat(handler.isFatal(timeout)).isFalse();

        RuntimeException generic = new RuntimeException("Generic error");
        assertThat(handler.isFatal(generic)).isFalse();
    }

    @Test
    void testComponentCreation() {
        // Test that all components can be created successfully

        // Sink configuration
        CqlSinkConfig<Row> config = CqlSinkConfig.forRow();
        assertThat(config).isNotNull();

        // Cluster builder
        SerializableClusterBuilder builder = new SerializableClusterBuilder();
        assertThat(builder).isNotNull();
        assertThat(builder.buildCluster(null)).isNull(); // Designed for testing

        // Request configuration
        RequestConfiguration requestConfig =
                RequestConfiguration.builder()
                        .setMaxConcurrentRequests(5)
                        .setMaxTimeout(Duration.ofSeconds(30))
                        .build();
        assertThat(requestConfig).isNotNull();

        // Sink writer with various configurations
        CassandraSinkWriter<Row> writer =
                new CassandraSinkWriter<>(
                        requestConfig,
                        sinkConfig,
                        clusterBuilder,
                        failureHandler,
                        new DirectMailbox(),
                        metricGroup,
                        (cb, sc) -> null);

        assertThat(writer).isNotNull();
    }

    @Test
    void testPermitAcquisitionTimeout() throws Exception {
        // Setup: maxConcurrentRequests=1, tiny timeout, first write never completes
        RequestConfiguration config =
                RequestConfiguration.builder()
                        .setMaxConcurrentRequests(1)
                        .setMaxTimeout(Duration.ofMillis(500))
                        .build();

        ControllableRecordWriter recordWriter = new ControllableRecordWriter();
        SettableFuture<ResultSet> neverCompletingFuture = SettableFuture.create();
        recordWriter.setNextResult(neverCompletingFuture);

        CassandraFailureHandler spyHandler = spy(failureHandler);

        CassandraSinkWriter<Row> writer =
                new CassandraSinkWriter<>(
                        config,
                        sinkConfig,
                        clusterBuilder,
                        spyHandler,
                        new DirectMailbox(),
                        metricGroup,
                        (cb, sc) -> recordWriter);

        // First write should succeed in acquiring permit but not complete
        writer.write(
                Row.of(1, "test1"),
                mock(org.apache.flink.api.connector.sink2.SinkWriter.Context.class));

        // Second write should timeout trying to acquire permit
        assertThatThrownBy(
                        () ->
                                writer.write(
                                        Row.of(2, "test2"),
                                        mock(
                                                org.apache.flink.api.connector.sink2.SinkWriter
                                                        .Context.class)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to acquire");

        // Verify failure handler was NOT called (timeout is not handled by failure handler)
        verify(spyHandler, never()).onFailure(any());

        // Verify permit leak by checking flush behavior - should timeout because first write
        // never completed and still holds permit
        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unable to flush within timeout");
    }

    @Test
    void testRetryThenSuccess() throws Exception {
        // Setup: first call fails with retriable exception, second succeeds
        RequestConfiguration config =
                RequestConfiguration.builder().setMaxConcurrentRequests(2).setMaxRetries(2).build();

        ControllableRecordWriter recordWriter = new ControllableRecordWriter();

        // First call fails with retriable exception
        SettableFuture<ResultSet> failingFuture = SettableFuture.create();
        failingFuture.setException(new NoHostAvailableException(Collections.emptyMap()));

        // Second call (retry) succeeds
        SettableFuture<ResultSet> successFuture = SettableFuture.create();
        successFuture.set(mock(ResultSet.class));

        recordWriter.setFirstThenSecondResult(failingFuture, successFuture);

        CassandraSinkWriter<Row> writer =
                new CassandraSinkWriter<>(
                        config,
                        sinkConfig,
                        clusterBuilder,
                        failureHandler,
                        new DirectMailbox(),
                        metricGroup,
                        (cb, sc) -> recordWriter);

        // Write should succeed after retry
        writer.write(
                Row.of(1, "test"),
                mock(org.apache.flink.api.connector.sink2.SinkWriter.Context.class));

        // Verify retry occurred
        assertThat(recordWriter.getCallCount()).isEqualTo(2);
        assertThat(retryCounter.getCount()).isEqualTo(1);

        // Verify success metrics
        assertThat(successCounter.getCount()).isEqualTo(1);
        assertThat(failedCounter.getCount()).isEqualTo(0);

        writer.close();
    }

    @Test
    void testMaxRetriesExhausted() throws Exception {
        RequestConfiguration config =
                RequestConfiguration.builder().setMaxConcurrentRequests(2).setMaxRetries(1).build();

        ControllableRecordWriter recordWriter = new ControllableRecordWriter();
        SettableFuture<ResultSet> failingFuture = SettableFuture.create();
        failingFuture.setException(new ReadTimeoutException(null, null, 1, 0, false));
        recordWriter.setNextResult(failingFuture);

        CassandraFailureHandler spyHandler = spy(failureHandler);

        CassandraSinkWriter<Row> writer =
                new CassandraSinkWriter<>(
                        config,
                        sinkConfig,
                        clusterBuilder,
                        spyHandler,
                        new DirectMailbox(),
                        metricGroup,
                        (cb, sc) -> recordWriter);

        Row testRow = Row.of(1, "test");

        // Write the element - this will submit the async operation
        writer.write(testRow, mock(org.apache.flink.api.connector.sink2.SinkWriter.Context.class));

        // Verify retries occurred (original + 1 retry = 2 total calls)
        assertThat(recordWriter.getCallCount()).isEqualTo(2);
        assertThat(retryCounter.getCount()).isEqualTo(1);

        // Verify failure handler was called with MaxRetriesExceededException
        verify(spyHandler).onFailure(any(MaxRetriesExceededException.class));

        // Verify failure metrics
        assertThat(failedCounter.getCount()).isEqualTo(1);
        assertThat(successCounter.getCount()).isEqualTo(0);

        writer.close();
    }

    @Test
    void testImmediateFatalError() throws Exception {
        // Setup: fail with fatal exception (no retries)
        RequestConfiguration config =
                RequestConfiguration.builder()
                        .setMaxConcurrentRequests(2)
                        .setMaxRetries(3) // Should not matter for fatal errors
                        .build();

        ControllableRecordWriter recordWriter = new ControllableRecordWriter();
        SettableFuture<ResultSet> failingFuture = SettableFuture.create();
        failingFuture.setException(new InvalidQueryException("Invalid table"));
        recordWriter.setNextResult(failingFuture);

        CassandraFailureHandler spyHandler = spy(failureHandler);

        CassandraSinkWriter<Row> writer =
                new CassandraSinkWriter<>(
                        config,
                        sinkConfig,
                        clusterBuilder,
                        spyHandler,
                        new DirectMailbox(),
                        metricGroup,
                        (cb, sc) -> recordWriter);

        // Write the element - this will submit the async operation
        writer.write(
                Row.of(1, "test"),
                mock(org.apache.flink.api.connector.sink2.SinkWriter.Context.class));

        // Verify NO retries occurred (only 1 call)
        assertThat(recordWriter.getCallCount()).isEqualTo(1);
        assertThat(retryCounter.getCount()).isEqualTo(0);

        // Verify failure handler was called with original fatal exception
        verify(spyHandler).onFailure(any(InvalidQueryException.class));

        // Verify failure metrics
        assertThat(failedCounter.getCount()).isEqualTo(1);
        assertThat(successCounter.getCount()).isEqualTo(0);

        writer.close();
    }

    @Test
    void testFlushTimeout() throws Exception {
        // Setup: in-flight request that never completes, small flush timeout
        RequestConfiguration config =
                RequestConfiguration.builder()
                        .setMaxConcurrentRequests(1)
                        .setFlushTimeout(Duration.ofMillis(500))
                        .build();

        ControllableRecordWriter recordWriter = new ControllableRecordWriter();
        SettableFuture<ResultSet> neverCompletingFuture = SettableFuture.create();
        recordWriter.setNextResult(neverCompletingFuture);

        CassandraSinkWriter<Row> writer =
                new CassandraSinkWriter<>(
                        config,
                        sinkConfig,
                        clusterBuilder,
                        failureHandler,
                        new DirectMailbox(),
                        metricGroup,
                        (cb, sc) -> recordWriter);

        // Start a write that will never complete
        writer.write(
                Row.of(1, "test"),
                mock(org.apache.flink.api.connector.sink2.SinkWriter.Context.class));

        // flush() should timeout
        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unable to flush within timeout")
                .hasCauseInstanceOf(java.util.concurrent.TimeoutException.class);

        // Don't call close() as it will also timeout with in-flight requests
    }

    @Test
    void testExecutePendingTasksFastPath() throws Exception {
        // Setup: no in-flight requests
        RequestConfiguration config =
                RequestConfiguration.builder().setMaxConcurrentRequests(2).build();

        ControllableRecordWriter recordWriter = new ControllableRecordWriter();
        CassandraSinkWriter<Row> writer =
                new CassandraSinkWriter<>(
                        config,
                        sinkConfig,
                        clusterBuilder,
                        failureHandler,
                        new DirectMailbox(),
                        metricGroup,
                        (cb, sc) -> recordWriter);

        // executePendingTasks should return immediately when no requests are in-flight
        long startTime = System.nanoTime();
        writer.executePendingTasks();
        long duration = System.nanoTime() - startTime;

        // Should complete very quickly (less than 10ms)
        assertThat(duration).isLessThan(10_000_000L); // 10ms in nanoseconds

        writer.close();
    }
}
