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

package org.apache.flink.batch.connectors.cassandra;

import org.apache.flink.connector.cassandra.mapper.PojoMapper;
import org.apache.flink.streaming.connectors.cassandra.CassandraMapperOptions;
import org.apache.flink.streaming.connectors.cassandra.CassandraSessionBuilder;
import org.apache.flink.util.Preconditions;

import com.datastax.oss.driver.api.core.cql.PreparedStatement;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

/**
 * OutputFormat to write data to Apache Cassandra and from a custom Cassandra annotated object.
 * Please read the recommendations in {@linkplain CassandraOutputFormatBase}.
 *
 * @param <OUT> type of outputClass
 */
public class CassandraPojoOutputFormat<OUT> extends CassandraOutputFormatBase<OUT, Void> {

    private static final long serialVersionUID = -1701885135103942460L;

    private final CassandraMapperOptions mapperOptions;
    private final Class<OUT> outputClass;
    private transient PojoMapper<OUT> mapper;
    private transient CassandraMapperOptions options;
    private transient PreparedStatement preparedStatement;

    public CassandraPojoOutputFormat(CassandraSessionBuilder builder, Class<OUT> outputClass) {
        this(builder, outputClass, null);
    }

    public CassandraPojoOutputFormat(
            CassandraSessionBuilder builder,
            Class<OUT> outputClass,
            CassandraMapperOptions mapperOptions) {
        this(
                builder,
                outputClass,
                mapperOptions,
                Integer.MAX_VALUE,
                Duration.ofMillis(Long.MAX_VALUE));
    }

    public CassandraPojoOutputFormat(
            CassandraSessionBuilder builder,
            Class<OUT> outputClass,
            CassandraMapperOptions mapperOptions,
            int maxConcurrentRequests,
            Duration maxConcurrentRequestsTimeout) {
        super(builder, maxConcurrentRequests, maxConcurrentRequestsTimeout);
        Preconditions.checkNotNull(outputClass, "OutputClass cannot be null");
        this.mapperOptions = mapperOptions;
        this.outputClass = outputClass;
    }

    /** Opens a Session to Cassandra and initializes the prepared statement. */
    @Override
    protected void postOpen() {
        super.postOpen();
        this.options = mapperOptions != null ? mapperOptions : new CassandraMapperOptions();
        this.mapper = PojoMapper.create(outputClass);
        this.preparedStatement = session.prepare(mapper.buildInsert(null, options));
    }

    @Override
    protected CompletionStage<Void> send(OUT record) {
        return session.executeAsync(mapper.bind(preparedStatement, record, options))
                .thenApply(resultSet -> null);
    }

    /** Closes all resources used. */
    @Override
    protected void postClose() {
        super.postClose();
        mapper = null;
        preparedStatement = null;
    }
}
