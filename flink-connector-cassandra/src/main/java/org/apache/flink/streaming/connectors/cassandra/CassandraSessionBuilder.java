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
import org.apache.flink.connector.cassandra.codec.DateTimestampCodec;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;

import java.io.Serializable;

/**
 * This class is used to configure a {@link CqlSession} after deployment. The session represents the
 * connection that will be established to Cassandra.
 *
 * <p>When contact points are provided explicitly, the driver requires the local datacenter to be
 * set, either with {@link CqlSessionBuilder#withLocalDatacenter(String)} or through the driver
 * configuration. Any driver option can be set with {@link
 * CqlSessionBuilder#withConfigLoader(com.datastax.oss.driver.api.core.config.DriverConfigLoader)},
 * an {@code application.conf} file in the classpath or {@code datastax-java-driver.*} system
 * properties.
 *
 * <p>Cassandra driver metrics are disabled by default and are not integrated with Flink metrics.
 */
@PublicEvolving
public abstract class CassandraSessionBuilder implements Serializable {

    /**
     * Creates the session. A codec mapping CQL {@code timestamp} to {@link java.util.Date} is
     * registered in addition to the driver's default codecs.
     *
     * @return the session connected to Cassandra
     */
    public CqlSession getSession() {
        return buildSession(CqlSession.builder().addTypeCodecs(DateTimestampCodec.INSTANCE));
    }

    /**
     * Configures the connection to Cassandra. The configuration is done by calling methods on the
     * builder object and finalizing the configuration with build().
     *
     * @param builder connection builder
     * @return configured connection
     */
    protected abstract CqlSession buildSession(CqlSessionBuilder builder);
}
