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

package org.apache.flink.connector.cassandra.sink.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.connectors.cassandra.MapperOptions;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;

/**
 * Configuration for writing POJO-based records to Cassandra using the DataStax Mapper.
 *
 * <p>This configuration is used with annotated Java classes that map directly to Cassandra tables
 * using DataStax driver annotations like {@code @Table} and {@code @Column}. The POJO class should
 * be properly annotated to define the table structure and field mappings.
 *
 * <p>Example POJO:
 *
 * <pre>{@code
 * @Table(keyspace = "example", name = "users")
 * public class User {
 *     @PartitionKey
 *     private String id;
 *
 *     @Column(name = "full_name")
 *     private String fullName;
 *
 *     // getters and setters
 * }
 * }</pre>
 *
 * @param <INPUT> the POJO type
 */
@PublicEvolving
public class PojoSinkConfig<INPUT> implements CassandraSinkConfig<INPUT> {

    private static final long serialVersionUID = 1L;

    private final Class<INPUT> pojoClass;
    private final String keyspace;
    @Nullable private final MapperOptions mapperOptions;

    /**
     * Creates a new PojoSinkConfig.
     *
     * @param pojoClass the POJO class used for mapping.
     * @param keyspace the Cassandra keyspace where the target table resides
     * @param mapperOptions optional DataStax Mapper options (TTL, consistency level, etc.)
     */
    public PojoSinkConfig(
            Class<INPUT> pojoClass, String keyspace, @Nullable MapperOptions mapperOptions) {
        Preconditions.checkArgument(pojoClass != null, "pojoClass must not be null.");
        Preconditions.checkArgument(!StringUtils.isEmpty(keyspace), "Keyspace cannot be empty.");
        this.pojoClass = pojoClass;
        this.keyspace = keyspace;
        this.mapperOptions = mapperOptions;
    }

    /**
     * Gets the POJO class used for mapping.
     *
     * @return the input class type used to configure the DataStax Mapper
     */
    public Class<INPUT> getPojoClass() {
        return pojoClass;
    }

    /**
     * Gets the keyspace where the target Cassandra table resides.
     *
     * @return the keyspace name
     */
    public String getKeyspace() {
        return keyspace;
    }

    /**
     * Gets the optional {@link MapperOptions} for configuring the DataStax Mapper.
     *
     * @return MapperOptions such as TTL, consistency level, etc., or null if not set
     */
    @Nullable
    public MapperOptions getMapperOptions() {
        return mapperOptions;
    }

    @Override
    public RecordFormatType getRecordFormatType() {
        return RecordFormatType.POJO;
    }
}
