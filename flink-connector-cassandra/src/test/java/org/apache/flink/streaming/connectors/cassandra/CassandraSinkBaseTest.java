/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.cassandra;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;

/** Tests for the {@link CassandraSinkBase}. */
class CassandraSinkBaseTest {

    private static final long DEFAULT_TEST_TIMEOUT_IN_MILLIS = 5000;

    @Test
    void testHostNotFoundErrorHandling() throws Exception {
        CassandraSinkBase base =
                new CassandraSinkBase(
                        new CassandraSessionBuilder() {
                            @Override
                            protected CqlSession buildSession(CqlSessionBuilder builder) {
                                return builder.addContactPoint(
                                                new InetSocketAddress("127.0.0.1", 9042))
                                        .withLocalDatacenter("datacenter1")
                                        .build();
                            }
                        },
                        CassandraSinkBaseConfig.newBuilder().build(),
                        new NoOpCassandraFailureHandler()) {
                    @Override
                    public CompletionStage send(Object value) {
                        return null;
                    }
                };

        assertThatThrownBy(() -> base.open(new Configuration()))
                .isInstanceOf(AllNodesFailedException.class);
    }

    @Test
    @Timeout(value = DEFAULT_TEST_TIMEOUT_IN_MILLIS, unit = TimeUnit.MILLISECONDS)
    void testSuccessfulPath() throws Exception {
        try (TestCassandraSink casSinkFunc = createOpenedTestCassandraSink()) {
            casSinkFunc.enqueueCompletableFuture(CompletableFuture.completedFuture(null));

            final int originalPermits = casSinkFunc.getAvailablePermits();
            assertThat(originalPermits).isGreaterThan(0);
            assertThat(casSinkFunc.getAcquiredPermits()).isZero();

            casSinkFunc.invoke("hello");

            assertThat(casSinkFunc.getAvailablePermits()).isEqualTo(originalPermits);
            assertThat(casSinkFunc.getAcquiredPermits()).isZero();
        }
    }

    @Test
    @Timeout(value = DEFAULT_TEST_TIMEOUT_IN_MILLIS, unit = TimeUnit.MILLISECONDS)
    void testThrowErrorOnClose() throws Exception {
        TestCassandraSink casSinkFunc = new TestCassandraSink();

        casSinkFunc.open(new Configuration());

        Exception cause = new RuntimeException();
        casSinkFunc.enqueueCompletableFuture(FutureUtils.completedExceptionally(cause));
        casSinkFunc.invoke("hello");
        try {
            casSinkFunc.close();

            fail("Close should have thrown an exception.");
        } catch (IOException e) {
            ExceptionUtils.findThrowable(e, candidate -> candidate == cause).orElseThrow(() -> e);
        }
    }

    @Test
    @Timeout(value = DEFAULT_TEST_TIMEOUT_IN_MILLIS, unit = TimeUnit.MILLISECONDS)
    void testThrowErrorOnInvoke() throws Exception {
        try (TestCassandraSink casSinkFunc = createOpenedTestCassandraSink()) {
            Exception cause = new RuntimeException();
            casSinkFunc.enqueueCompletableFuture(FutureUtils.completedExceptionally(cause));

            casSinkFunc.invoke("hello");

            try {
                casSinkFunc.invoke("world");
                fail("Sending of second value should have failed.");
            } catch (IOException e) {
                assertThat(e.getCause()).isEqualTo(cause);
                assertThat(casSinkFunc.getAcquiredPermits()).isZero();
            }
        }
    }

    @Test
    @Timeout(value = DEFAULT_TEST_TIMEOUT_IN_MILLIS, unit = TimeUnit.MILLISECONDS)
    void testIgnoreError() throws Exception {
        Exception cause = new RuntimeException();
        CassandraFailureHandler failureHandler = failure -> assertThat(failure).isEqualTo(cause);

        try (TestCassandraSink casSinkFunc = createOpenedTestCassandraSink(failureHandler)) {

            casSinkFunc.enqueueCompletableFuture(FutureUtils.completedExceptionally(cause));
            casSinkFunc.enqueueCompletableFuture(FutureUtils.completedExceptionally(cause));

            casSinkFunc.invoke("hello");
            casSinkFunc.invoke("world");
        }
    }

    @Test
    @Timeout(value = DEFAULT_TEST_TIMEOUT_IN_MILLIS, unit = TimeUnit.MILLISECONDS)
    void testThrowErrorOnSnapshot() throws Exception {
        TestCassandraSink casSinkFunc = new TestCassandraSink();

        try (OneInputStreamOperatorTestHarness<String, Object> testHarness =
                createOpenedTestHarness(casSinkFunc)) {
            Exception cause = new RuntimeException();
            casSinkFunc.enqueueCompletableFuture(FutureUtils.completedExceptionally(cause));

            casSinkFunc.invoke("hello");
            assertThatThrownBy(() -> testHarness.snapshot(123L, 123L))
                    .hasCauseInstanceOf(IOException.class);
        }
    }

    @Test
    @Timeout(value = DEFAULT_TEST_TIMEOUT_IN_MILLIS, unit = TimeUnit.MILLISECONDS)
    void testWaitForPendingUpdatesOnSnapshot() throws Exception {
        final TestCassandraSink casSinkFunc = new TestCassandraSink();

        try (OneInputStreamOperatorTestHarness<String, Object> testHarness =
                createOpenedTestHarness(casSinkFunc)) {
            CompletableFuture<AsyncResultSet> completableFuture = new CompletableFuture<>();
            casSinkFunc.enqueueCompletableFuture(completableFuture);

            casSinkFunc.invoke("hello");
            assertThat(casSinkFunc.getAcquiredPermits()).isOne();

            final CountDownLatch latch = new CountDownLatch(1);
            Thread t =
                    new CheckedThread("Flink-CassandraSinkBaseTest") {
                        @Override
                        public void go() throws Exception {
                            testHarness.snapshot(123L, 123L);
                            latch.countDown();
                        }
                    };
            t.start();
            while (t.getState() != Thread.State.TIMED_WAITING) {
                Thread.sleep(5);
            }

            assertThat(casSinkFunc.getAcquiredPermits()).isOne();
            completableFuture.complete(null);
            latch.await();
            assertThat(casSinkFunc.getAcquiredPermits()).isZero();
        }
    }

    @Test
    @Timeout(value = DEFAULT_TEST_TIMEOUT_IN_MILLIS, unit = TimeUnit.MILLISECONDS)
    void testWaitForPendingUpdatesOnClose() throws Exception {
        TestCassandraSink casSinkFunc = new TestCassandraSink();

        try (OneInputStreamOperatorTestHarness<String, Object> testHarness =
                createOpenedTestHarness(casSinkFunc)) {

            CompletableFuture<AsyncResultSet> completableFuture = new CompletableFuture<>();
            casSinkFunc.enqueueCompletableFuture(completableFuture);

            casSinkFunc.invoke("hello");
            assertThat(casSinkFunc.getAcquiredPermits()).isOne();

            final CountDownLatch latch = new CountDownLatch(1);
            Thread t =
                    new CheckedThread("Flink-CassandraSinkBaseTest") {
                        @Override
                        public void go() throws Exception {
                            testHarness.close();
                            latch.countDown();
                        }
                    };
            t.start();
            while (t.getState() != Thread.State.TIMED_WAITING) {
                Thread.sleep(5);
            }

            assertThat(casSinkFunc.getAcquiredPermits()).isOne();
            completableFuture.complete(null);
            latch.await();
            assertThat(casSinkFunc.getAcquiredPermits()).isZero();
        }
    }

    @Test
    @Timeout(value = DEFAULT_TEST_TIMEOUT_IN_MILLIS, unit = TimeUnit.MILLISECONDS)
    void testReleaseOnSuccess() throws Exception {
        final CassandraSinkBaseConfig config =
                CassandraSinkBaseConfig.newBuilder().setMaxConcurrentRequests(1).build();

        try (TestCassandraSink testCassandraSink = createOpenedTestCassandraSink(config)) {
            assertThat(testCassandraSink.getAvailablePermits()).isOne();
            assertThat(testCassandraSink.getAcquiredPermits()).isZero();

            CompletableFuture<AsyncResultSet> completableFuture = new CompletableFuture<>();
            testCassandraSink.enqueueCompletableFuture(completableFuture);
            testCassandraSink.invoke("N/A");

            assertThat(testCassandraSink.getAvailablePermits()).isZero();
            assertThat(testCassandraSink.getAcquiredPermits()).isOne();

            completableFuture.complete(null);

            assertThat(testCassandraSink.getAvailablePermits()).isOne();
            assertThat(testCassandraSink.getAcquiredPermits()).isZero();
        }
    }

    @Test
    @Timeout(value = DEFAULT_TEST_TIMEOUT_IN_MILLIS, unit = TimeUnit.MILLISECONDS)
    void testReleaseOnFailure() throws Exception {
        final CassandraSinkBaseConfig config =
                CassandraSinkBaseConfig.newBuilder().setMaxConcurrentRequests(1).build();
        final CassandraFailureHandler failureHandler = ignored -> {};

        try (TestCassandraSink testCassandraSink =
                createOpenedTestCassandraSink(config, failureHandler)) {
            assertThat(testCassandraSink.getAvailablePermits()).isOne();
            assertThat(testCassandraSink.getAcquiredPermits()).isZero();

            CompletableFuture<AsyncResultSet> completableFuture = new CompletableFuture<>();
            testCassandraSink.enqueueCompletableFuture(completableFuture);
            testCassandraSink.invoke("N/A");

            assertThat(testCassandraSink.getAvailablePermits()).isZero();
            assertThat(testCassandraSink.getAcquiredPermits()).isOne();

            completableFuture.completeExceptionally(new RuntimeException());

            assertThat(testCassandraSink.getAvailablePermits()).isOne();
            assertThat(testCassandraSink.getAcquiredPermits()).isZero();
        }
    }

    @Test
    @Timeout(value = DEFAULT_TEST_TIMEOUT_IN_MILLIS, unit = TimeUnit.MILLISECONDS)
    void testReleaseOnThrowingSend() throws Exception {
        final CassandraSinkBaseConfig config =
                CassandraSinkBaseConfig.newBuilder().setMaxConcurrentRequests(1).build();

        Function<String, CompletionStage<AsyncResultSet>> failingSendFunction =
                ignoredMessage -> {
                    throwCheckedAsUnchecked(new Throwable("expected"));
                    //noinspection ReturnOfNull
                    return null;
                };

        try (TestCassandraSink testCassandraSink =
                new MockCassandraSink(config, failingSendFunction)) {
            testCassandraSink.open(new Configuration());
            assertThat(testCassandraSink.getAvailablePermits()).isOne();
            assertThat(testCassandraSink.getAcquiredPermits()).isZero();

            //noinspection OverlyBroadCatchBlock,NestedTryStatement
            try {
                testCassandraSink.invoke("none");
            } catch (Throwable e) {
                assertThat(e).isInstanceOf(Throwable.class);
                assertThat(testCassandraSink.getAvailablePermits()).isOne();
                assertThat(testCassandraSink.getAcquiredPermits()).isZero();
            }
        }
    }

    @Test
    @Timeout(value = DEFAULT_TEST_TIMEOUT_IN_MILLIS, unit = TimeUnit.MILLISECONDS)
    void testTimeoutExceptionOnInvoke() throws Exception {
        final CassandraSinkBaseConfig config =
                CassandraSinkBaseConfig.newBuilder()
                        .setMaxConcurrentRequests(1)
                        .setMaxConcurrentRequestsTimeout(Duration.ofMillis(1))
                        .build();

        try (TestCassandraSink testCassandraSink = createOpenedTestCassandraSink(config)) {
            CompletableFuture<AsyncResultSet> completableFuture = new CompletableFuture<>();
            testCassandraSink.enqueueCompletableFuture(completableFuture);
            testCassandraSink.enqueueCompletableFuture(completableFuture);
            testCassandraSink.invoke("Invoke #1");

            try {
                testCassandraSink.invoke("Invoke #2");
                fail("Sending value should have experienced a TimeoutException");
            } catch (Exception e) {
                assertThat(e).isInstanceOf(TimeoutException.class);
            } finally {
                completableFuture.complete(null);
            }
        }
    }

    private TestCassandraSink createOpenedTestCassandraSink() {
        final TestCassandraSink testCassandraSink = new TestCassandraSink();
        testCassandraSink.open(new Configuration());
        return testCassandraSink;
    }

    private TestCassandraSink createOpenedTestCassandraSink(
            CassandraFailureHandler failureHandler) {
        final TestCassandraSink testCassandraSink = new TestCassandraSink(failureHandler);
        testCassandraSink.open(new Configuration());
        return testCassandraSink;
    }

    private TestCassandraSink createOpenedTestCassandraSink(CassandraSinkBaseConfig config) {
        final TestCassandraSink testCassandraSink = new TestCassandraSink(config);
        testCassandraSink.open(new Configuration());
        return testCassandraSink;
    }

    private TestCassandraSink createOpenedTestCassandraSink(
            CassandraSinkBaseConfig config, CassandraFailureHandler failureHandler) {
        final TestCassandraSink testCassandraSink = new TestCassandraSink(config, failureHandler);
        testCassandraSink.open(new Configuration());
        return testCassandraSink;
    }

    private OneInputStreamOperatorTestHarness<String, Object> createOpenedTestHarness(
            TestCassandraSink testCassandraSink) throws Exception {
        final StreamSink<String> testStreamSink = new StreamSink<>(testCassandraSink);
        final OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(testStreamSink);
        testHarness.open();
        return testHarness;
    }

    private static <T extends Throwable> void throwCheckedAsUnchecked(Throwable ex) throws T {
        //noinspection unchecked
        throw (T) ex;
    }

    private static class TestCassandraSink extends CassandraSinkBase<String, AsyncResultSet>
            implements AutoCloseable {

        private static final CassandraSessionBuilder builder;
        private static final CqlSession session;

        static {
            session = mock(CqlSession.class);

            builder =
                    new CassandraSessionBuilder() {
                        @Override
                        protected CqlSession buildSession(CqlSessionBuilder builder) {
                            return session;
                        }
                    };
        }

        private final Queue<CompletionStage<AsyncResultSet>> resultSetFutures = new LinkedList<>();

        TestCassandraSink() {
            this(CassandraSinkBaseConfig.newBuilder().build());
        }

        TestCassandraSink(CassandraSinkBaseConfig config) {
            this(config, new NoOpCassandraFailureHandler());
        }

        TestCassandraSink(CassandraFailureHandler failureHandler) {
            this(CassandraSinkBaseConfig.newBuilder().build(), failureHandler);
        }

        TestCassandraSink(CassandraSinkBaseConfig config, CassandraFailureHandler failureHandler) {
            super(builder, config, failureHandler);
        }

        @Override
        public CompletionStage<AsyncResultSet> send(String value) {
            return resultSetFutures.poll();
        }

        void enqueueCompletableFuture(CompletableFuture<AsyncResultSet> completableFuture) {
            Preconditions.checkNotNull(completableFuture);
            resultSetFutures.offer(completableFuture);
        }
    }

    private static class MockCassandraSink extends TestCassandraSink {
        private static final long serialVersionUID = -3363195776692829911L;

        private final Function<String, CompletionStage<AsyncResultSet>> sendFunction;

        MockCassandraSink(
                CassandraSinkBaseConfig config,
                Function<String, CompletionStage<AsyncResultSet>> sendFunction) {
            super(config, new NoOpCassandraFailureHandler());
            this.sendFunction = sendFunction;
        }

        @Override
        public CompletionStage<AsyncResultSet> send(String value) {
            return this.sendFunction.apply(value);
        }
    }
}
