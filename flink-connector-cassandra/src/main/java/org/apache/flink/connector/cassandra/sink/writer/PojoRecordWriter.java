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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.cassandra.sink.config.PojoSinkConfig;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.util.Preconditions;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;

import java.util.Objects;

/**
 * A {@link CassandraRecordWriter} implementation that writes POJO records to Cassandra using the
 * DataStax {@link Mapper}.
 *
 * <p>This writer:
 *
 * <ul>
 *   <li>Creating a Cassandra {@link Session} from a {@link ClusterBuilder}
 *   <li>Instantiating a {@link Mapper} using the provided POJO class
 *   <li>Executing insert/update statements using {@code mapper.saveQuery()}
 *   <li>Applying optional {@link com.datastax.driver.mapping.Mapper.Option}s from the config
 * </ul>
 *
 * @param <INPUT> the type of the input POJO
 */
@Internal
public class PojoRecordWriter<INPUT> extends AbstractRecordWriter<INPUT> {

    private final Session session;
    private final Cluster cluster;
    private final Mapper<INPUT> mapper;

    /**
     * Constructs a PojoRecordWriter using the provided builder and sink config.
     *
     * @param builder the cluster builder used to connect to Cassandra
     * @param config the config containing POJO class, keyspace, and mapper options
     * @throws RuntimeException if initialization fails
     */
    public PojoRecordWriter(ClusterBuilder builder, PojoSinkConfig<INPUT> config) {
        Preconditions.checkArgument(builder != null, "ClusterBuilder cannot be null");
        Preconditions.checkArgument(config != null, "PojoSinkConfig cannot be null");
        try {
            this.cluster = builder.getCluster();
            this.session = cluster.connect(config.getKeyspace());

            // Create mapper using MappingManager
            MappingManager mappingManager = new MappingManager(session);
            this.mapper = mappingManager.mapper(config.getPojoClass());

            // Apply mapper options if provided
            if (config.getMapperOptions() != null) {
                Mapper.Option[] optionsArray = config.getMapperOptions().getMapperOptions();
                if (optionsArray != null && optionsArray.length > 0) {
                    mapper.setDefaultSaveOptions(optionsArray);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to initialize PojoRecordWriter for keyspace '%s' and pojo %s",
                            config.getKeyspace(), config.getPojoClass().getName()),
                    e);
        }
    }

    /**
     * Test-only constructor that accepts all dependencies directly.
     *
     * <p>This constructor is only for testing purposes and bypasses the normal initialization of
     * the MappingManager.
     *
     * @param cluster the Cassandra cluster
     * @param session the Cassandra session
     * @param mapper the DataStax mapper for the POJO type
     */
    @VisibleForTesting
    PojoRecordWriter(Cluster cluster, Session session, Mapper<INPUT> mapper) {
        this.cluster = Objects.requireNonNull(cluster, "cluster");
        this.session = Objects.requireNonNull(session, "session");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    /**
     * Gets the Cassandra session used by this writer.
     *
     * @return the active session
     */
    @Override
    public Session getSession() {
        return session;
    }

    /**
     * Generates a CQL {@link Statement} for the given POJO using the DataStax mapper.
     *
     * <p>The mapper will generate an appropriate INSERT or UPDATE statement based on the POJO
     * annotations and configured mapper options.
     *
     * @param input the POJO to serialize into a statement
     * @return a statement ready for asynchronous execution
     */
    @Override
    public Statement prepareStatement(INPUT input) {
        return mapper.saveQuery(input);
    }

    /**
     * Gets the Cassandra cluster instance.
     *
     * @return the cluster used by this writer
     */
    @Override
    protected Cluster getCluster() {
        return cluster;
    }
}
