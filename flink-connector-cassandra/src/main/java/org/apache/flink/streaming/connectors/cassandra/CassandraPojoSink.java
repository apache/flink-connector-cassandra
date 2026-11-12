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
import org.apache.flink.connector.cassandra.mapper.PojoMapper;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

import javax.annotation.Nullable;

import java.util.concurrent.CompletionStage;

/**
 * Flink Sink to save POJOs into a Cassandra cluster. The POJOs are mapped with the annotations of
 * the Cassandra Java driver mapper from the package {@code
 * com.datastax.oss.driver.api.mapper.annotations}, the class must be annotated with {@link
 * com.datastax.oss.driver.api.mapper.annotations.Entity}. Please read the recommendations in
 * {@linkplain CassandraSinkBase}.
 *
 * @param <IN> Type of the elements emitted by this sink
 */
public class CassandraPojoSink<IN> extends CassandraSinkBase<IN, AsyncResultSet> {

    private static final long serialVersionUID = 1L;

    protected final Class<IN> clazz;
    private final CassandraMapperOptions options;
    private final String keyspace;
    protected transient PojoMapper<IN> mapper;
    private transient CassandraMapperOptions mapperOptions;
    private transient PreparedStatement preparedStatement;

    /**
     * The main constructor for creating CassandraPojoSink.
     *
     * @param clazz Class instance
     */
    public CassandraPojoSink(Class<IN> clazz, CassandraSessionBuilder builder) {
        this(clazz, builder, null, null);
    }

    public CassandraPojoSink(
            Class<IN> clazz,
            CassandraSessionBuilder builder,
            @Nullable CassandraMapperOptions options) {
        this(clazz, builder, options, null);
    }

    public CassandraPojoSink(Class<IN> clazz, CassandraSessionBuilder builder, String keyspace) {
        this(clazz, builder, null, keyspace);
    }

    public CassandraPojoSink(
            Class<IN> clazz,
            CassandraSessionBuilder builder,
            @Nullable CassandraMapperOptions options,
            String keyspace) {
        this(clazz, builder, options, keyspace, CassandraSinkBaseConfig.newBuilder().build());
    }

    CassandraPojoSink(
            Class<IN> clazz,
            CassandraSessionBuilder builder,
            @Nullable CassandraMapperOptions options,
            String keyspace,
            CassandraSinkBaseConfig config) {
        this(clazz, builder, options, keyspace, config, new NoOpCassandraFailureHandler());
    }

    CassandraPojoSink(
            Class<IN> clazz,
            CassandraSessionBuilder builder,
            @Nullable CassandraMapperOptions options,
            String keyspace,
            CassandraSinkBaseConfig config,
            CassandraFailureHandler failureHandler) {
        super(builder, config, failureHandler);
        this.clazz = clazz;
        this.options = options;
        this.keyspace = keyspace;
    }

    @Override
    public void open(Configuration configuration) {
        super.open(configuration);
        try {
            this.mapperOptions = options != null ? options : new CassandraMapperOptions();
            this.mapper = PojoMapper.create(clazz);
            this.preparedStatement = session.prepare(mapper.buildInsert(keyspace, mapperOptions));
        } catch (Exception e) {
            throw new RuntimeException(
                    "Cannot create CassandraPojoSink with input: " + clazz.getSimpleName(), e);
        }
    }

    @Override
    public CompletionStage<AsyncResultSet> send(IN value) {
        return session.executeAsync(mapper.bind(preparedStatement, value, mapperOptions));
    }
}
