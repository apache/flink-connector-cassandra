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

import org.apache.flink.annotation.PublicEvolving;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * Options used to configure how POJOs annotated with {@link
 * com.datastax.oss.driver.api.mapper.annotations.Entity} are written to and read from Cassandra.
 */
@PublicEvolving
public class CassandraMapperOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    @Nullable private Integer ttl;
    @Nullable private Long timestamp;
    @Nullable private DefaultConsistencyLevel consistencyLevel;
    private boolean tracing = false;
    private boolean saveNullFields = true;
    private boolean ifNotExists = false;

    /**
     * Adds time-to-live option to a mapper operation. This is only valid for save operations.
     *
     * @param ttl the TTL (in seconds).
     */
    public CassandraMapperOptions ttl(int ttl) {
        this.ttl = ttl;
        return this;
    }

    /**
     * Adds a timestamp option to a mapper operation. This is only valid for save operations.
     *
     * @param timestamp the timestamp (in microseconds).
     */
    public CassandraMapperOptions timestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    /**
     * Adds a consistency level value option to a mapper operation. If this option is not specified,
     * the consistency level of the driver configuration is used.
     *
     * @param consistencyLevel the {@link DefaultConsistencyLevel} to use for the operation.
     */
    public CassandraMapperOptions consistencyLevel(DefaultConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return this;
    }

    /**
     * Enables query tracing for a mapper operation.
     *
     * @param enabled whether to enable tracing.
     */
    public CassandraMapperOptions tracing(boolean enabled) {
        this.tracing = enabled;
        return this;
    }

    /**
     * Specifies whether null entity fields should be included in insert queries. This option is
     * valid only for save operations.
     *
     * <p>If this option is not specified, it defaults to {@code true} (null fields are saved).
     *
     * @param enabled whether to include null fields in queries.
     */
    public CassandraMapperOptions saveNullFields(boolean enabled) {
        this.saveNullFields = enabled;
        return this;
    }

    /**
     * Specifies whether an IF NOT EXISTS clause should be included in insert queries. This option
     * is valid only for save operations.
     *
     * <p>If this option is not specified, it defaults to {@code false} (IF NOT EXISTS statements
     * are not used).
     *
     * @param enabled whether to include an IF NOT EXISTS clause in queries.
     */
    public CassandraMapperOptions ifNotExists(boolean enabled) {
        this.ifNotExists = enabled;
        return this;
    }

    @Nullable
    public Integer getTtl() {
        return ttl;
    }

    @Nullable
    public Long getTimestamp() {
        return timestamp;
    }

    @Nullable
    public DefaultConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public boolean isTracing() {
        return tracing;
    }

    public boolean isSaveNullFields() {
        return saveNullFields;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }
}
