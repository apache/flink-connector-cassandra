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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/**
 * Sink that emits its input elements into a Cassandra database. This sink stores incoming records
 * within a {@link org.apache.flink.runtime.state.AbstractStateBackend}, and only commits them to
 * cassandra if a checkpoint is completed.
 *
 * @param <IN> Type of the elements emitted by this sink
 */
public class CassandraTupleWriteAheadSink<IN extends Tuple> extends GenericWriteAheadSink<IN> {
    private static final long serialVersionUID = 1L;

    protected transient CqlSession session;

    private final String insertQuery;
    private transient PreparedStatement preparedStatement;

    private CassandraSessionBuilder builder;

    private transient Object[] fields;

    protected CassandraTupleWriteAheadSink(
            String insertQuery,
            TypeSerializer<IN> serializer,
            CassandraSessionBuilder builder,
            CheckpointCommitter committer)
            throws Exception {
        super(committer, serializer, UUID.randomUUID().toString().replace("-", "_"));
        this.insertQuery = insertQuery;
        this.builder = builder;
        ClosureCleaner.clean(builder, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
    }

    public void open() throws Exception {
        super.open();
        if (!getRuntimeContext().isCheckpointingEnabled()) {
            throw new IllegalStateException(
                    "The write-ahead log requires checkpointing to be enabled.");
        }
        session = builder.getSession();
        preparedStatement = session.prepare(insertQuery);

        fields = new Object[((TupleSerializer<IN>) serializer).getArity()];
    }

    @Override
    public void close() throws Exception {
        super.close();
        try {
            if (session != null) {
                session.close();
            }
        } catch (Exception e) {
            LOG.error("Error while closing session.", e);
        }
    }

    @Override
    protected boolean sendValues(Iterable<IN> values, long checkpointId, long timestamp)
            throws Exception {
        final AtomicInteger updatesCount = new AtomicInteger(0);
        final AtomicInteger updatesConfirmed = new AtomicInteger(0);

        final AtomicReference<Throwable> exception = new AtomicReference<>();

        BiConsumer<AsyncResultSet, Throwable> callback =
                (resultSet, throwable) -> {
                    if (throwable == null) {
                        updatesConfirmed.incrementAndGet();
                        if (updatesCount.get() > 0) { // only set if all updates have been sent
                            if (updatesCount.get() == updatesConfirmed.get()) {
                                synchronized (updatesConfirmed) {
                                    updatesConfirmed.notifyAll();
                                }
                            }
                        }
                    } else if (exception.compareAndSet(null, throwable)) {
                        LOG.error("Error while sending value.", throwable);
                        synchronized (updatesConfirmed) {
                            updatesConfirmed.notifyAll();
                        }
                    }
                };

        // set values for prepared statement
        int updatesSent = 0;
        for (IN value : values) {
            for (int x = 0; x < value.getArity(); x++) {
                fields[x] = value.getField(x);
            }
            // insert values and send to cassandra
            BoundStatement s = preparedStatement.bind(fields).setQueryTimestamp(timestamp);
            CompletionStage<AsyncResultSet> result = session.executeAsync(s);
            updatesSent++;
            if (result != null) {
                // add callback to detect errors
                result.whenComplete(callback);
            }
        }
        updatesCount.set(updatesSent);

        synchronized (updatesConfirmed) {
            while (exception.get() == null && updatesSent != updatesConfirmed.get()) {
                updatesConfirmed.wait();
            }
        }

        if (exception.get() != null) {
            LOG.warn("Sending a value failed.", exception.get());
            return false;
        } else {
            return true;
        }
    }
}
