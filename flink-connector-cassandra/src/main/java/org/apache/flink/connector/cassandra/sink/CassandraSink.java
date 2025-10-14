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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.cassandra.sink.config.CassandraSinkConfig;
import org.apache.flink.connector.cassandra.sink.config.write.RequestConfiguration;
import org.apache.flink.connector.cassandra.sink.exception.CassandraFailureHandler;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import java.io.IOException;

/**
 * A generic Flink {@link Sink} implementation for writing records to Apache Cassandra.
 *
 * <p>This sink supports multiple input types including POJOs, Tuples, Rows, and Scala Products.
 *
 * <p>Use {@link CassandraSinkBuilder} to create and configure instances of this sink.
 *
 * @param <INPUT> The input record type
 */
@PublicEvolving
public class CassandraSink<INPUT> implements Sink<INPUT> {

    private final CassandraSinkConfig<INPUT> sinkConfig;
    private final ClusterBuilder clusterBuilder;
    private final CassandraFailureHandler failureHandler;
    private final RequestConfiguration requestConfiguration;

    /**
     * Package-private constructor - use {@link CassandraSinkBuilder} to create instances.
     *
     * @param sinkConfig The sink-specific configuration
     * @param clusterBuilder Factory for building Cassandra cluster connections
     * @param failureHandler Handler for retriable and fatal failures
     * @param requestConfiguration Settings for concurrency, timeouts, and retries
     */
    CassandraSink(
            CassandraSinkConfig<INPUT> sinkConfig,
            ClusterBuilder clusterBuilder,
            CassandraFailureHandler failureHandler,
            RequestConfiguration requestConfiguration) {
        this.sinkConfig = sinkConfig;
        this.clusterBuilder = clusterBuilder;
        this.failureHandler = failureHandler;
        this.requestConfiguration = requestConfiguration;
    }

    /**
     * Creates a new {@link SinkWriter} for this sink.
     *
     * @param context the sink context providing access to MailboxExecutor and metrics
     * @return a new CassandraSinkWriter instance
     * @throws IOException if writer creation fails
     */
    @Override
    public SinkWriter<INPUT> createWriter(InitContext context) throws IOException {
        return new CassandraSinkWriter<>(
                this.requestConfiguration,
                this.sinkConfig,
                this.clusterBuilder,
                this.failureHandler,
                context.getMailboxExecutor(),
                context.metricGroup());
    }

    // Package-private getters for testing
    CassandraSinkConfig<INPUT> getSinkConfig() {
        return sinkConfig;
    }

    ClusterBuilder getClusterBuilder() {
        return clusterBuilder;
    }

    CassandraFailureHandler getFailureHandler() {
        return failureHandler;
    }

    RequestConfiguration getRequestConfiguration() {
        return requestConfiguration;
    }
}
