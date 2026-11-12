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

package org.apache.flink.streaming.connectors.cassandra;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentMatchers;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for the {@link CassandraTupleWriteAheadSink}. */
class CassandraTupleWriteAheadSinkTest {

    @Test
    @Timeout(value = 20_000, unit = TimeUnit.MILLISECONDS)
    void testAckLoopExitOnException() throws Exception {
        final AtomicReference<CompletableFuture<AsyncResultSet>> pendingFuture =
                new AtomicReference<>();

        final CassandraSessionBuilder sessionBuilder =
                new CassandraSessionBuilder() {
                    private static final long serialVersionUID = 4624400760492936756L;

                    @Override
                    protected CqlSession buildSession(CqlSessionBuilder builder) {
                        BoundStatement boundStatement = mock(BoundStatement.class);
                        when(boundStatement.setQueryTimestamp(any(long.class)))
                                .thenReturn(boundStatement);

                        PreparedStatement preparedStatement = mock(PreparedStatement.class);
                        when(preparedStatement.bind(ArgumentMatchers.any()))
                                .thenReturn(boundStatement);

                        CqlSession session = mock(CqlSession.class);
                        when(session.prepare(anyString())).thenReturn(preparedStatement);
                        when(session.executeAsync(any(BoundStatement.class)))
                                .thenAnswer(
                                        invocationOnMock -> {
                                            CompletableFuture<AsyncResultSet> future =
                                                    new CompletableFuture<>();
                                            synchronized (pendingFuture) {
                                                pendingFuture.set(future);
                                                pendingFuture.notifyAll();
                                            }
                                            return future;
                                        });
                        return session;
                    }
                };

        // Our asynchronous executor thread
        new Thread(
                        new Runnable() {
                            @Override
                            public void run() {
                                synchronized (pendingFuture) {
                                    while (pendingFuture.get() == null) {
                                        try {
                                            pendingFuture.wait();
                                        } catch (InterruptedException e) {
                                            // ignore interrupts
                                        }
                                    }
                                }
                                pendingFuture
                                        .get()
                                        .completeExceptionally(
                                                new RuntimeException("Expected exception."));
                            }
                        })
                .start();

        CheckpointCommitter cc = mock(CheckpointCommitter.class);
        final CassandraTupleWriteAheadSink<Tuple0> sink =
                new CassandraTupleWriteAheadSink<>(
                        "abc",
                        TupleTypeInfo.of(Tuple0.class).createSerializer(new ExecutionConfig()),
                        sessionBuilder,
                        cc);

        OneInputStreamOperatorTestHarness<Tuple0, Tuple0> harness =
                new OneInputStreamOperatorTestHarness<>(sink);
        harness.getEnvironment().getTaskConfiguration().setBoolean("checkpointing", true);

        harness.setup();
        sink.open();

        // we should leave the loop and return false since we've seen an exception
        assertThat(sink.sendValues(Collections.singleton(new Tuple0()), 1L, 0L)).isFalse();

        sink.close();
    }
}
