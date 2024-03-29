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

import com.datastax.driver.core.Cluster;

import java.io.Serializable;

/**
 * This class is used to configure a {@link com.datastax.driver.core.Cluster} after deployment. The
 * cluster represents the connection that will be established to Cassandra. Cassandra driver metrics
 * are not integrated with Flink metrics, so they are disabled.
 */
public abstract class ClusterBuilder implements Serializable {

    public Cluster getCluster() {
        return buildCluster(Cluster.builder().withoutMetrics());
    }

    /**
     * Configures the connection to Cassandra. The configuration is done by calling methods on the
     * builder object and finalizing the configuration with build().
     *
     * @param builder connection builder
     * @return configured connection
     */
    protected abstract Cluster buildCluster(Cluster.Builder builder);
}
