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

package org.apache.flink.connector.cassandra.sink.writer;

import org.apache.flink.annotation.Internal;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * An abstraction that defines how input records are converted into Cassandra writes.
 *
 * <p>Each implementation is responsible for:
 *
 * <ul>
 *   <li>Transforming a given input record into cassandra's {@link
 *       com.datastax.driver.core.BoundStatement}
 *   <li>Executing that statement against a live {@link Session}
 *   <li>Managing lifecycle of the Cassandra connection (via {@link #close()})
 * </ul>
 *
 * @param <INPUT> the type of input record written to Cassandra
 */
@Internal
public interface CassandraRecordWriter<INPUT> extends AutoCloseable {

    /**
     * Returns the live Cassandra session used by this writer.
     *
     * @return the active Cassandra session
     */
    Session getSession();

    /**
     * Converts a record into a <b>fully bound</b> Cassandra {@link Statement}.
     *
     * <p>“Fully bound” means there are no remaining {@code ?} bind markers on the returned
     * statement. If the base query (or options such as TTL/TIMESTAMP) introduced N variables, the
     * returned statement must have all N variables set (to a concrete value, to {@code null}, or
     * marked as {@code UNSET} when supported by the protocol).
     *
     * @param input the input record
     * @return a {@link Statement} ready for immediate execution
     */
    Statement prepareStatement(INPUT input);

    /**
     * Convenience method that performs both preparation and execution.
     *
     * <p>This method combines {@link #prepareStatement(Object)} and {@link
     * #executeStatement(Statement)}.
     *
     * @param input the input record
     * @return a {@link ListenableFuture} for the async operation
     */
    default ListenableFuture<ResultSet> write(INPUT input) {
        return executeStatement(prepareStatement(input));
    }

    /**
     * Executes a <b>fully bound</b> statement asynchronously using the writer's {@link Session}.
     *
     * @param statement the bound statement to execute
     * @return a {@link ListenableFuture} representing the async result
     */
    ListenableFuture<ResultSet> executeStatement(Statement statement);

    /**
     * Closes the writer and any internal Cassandra resources (session, cluster).
     *
     * <p>This method should be idempotent - safe to call multiple times.
     */
    void close();
}
