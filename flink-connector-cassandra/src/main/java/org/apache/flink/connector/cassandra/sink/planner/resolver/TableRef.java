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

package org.apache.flink.connector.cassandra.sink.planner.resolver;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

/** Reference to a Cassandra table consisting of keyspace and table name. */
@PublicEvolving
public class TableRef implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String keyspace;
    private final String tableName;

    public TableRef(String keyspace, String tableName) {
        Preconditions.checkArgument(
                keyspace != null && !StringUtils.isEmpty(keyspace.trim()),
                "keyspace cannot be null/empty");
        Preconditions.checkArgument(
                tableName != null && !StringUtils.isEmpty(tableName.trim()),
                "tableName cannot be null/empty");
        this.keyspace = keyspace;
        this.tableName = tableName;
    }

    public String keyspace() {
        return keyspace;
    }

    public String tableName() {
        return tableName;
    }

    /** Returns the fully qualified table name in format "keyspace.tableName". */
    public String getFullyQualifiedName() {
        return keyspace + "." + tableName;
    }

    @Override
    public String toString() {
        return getFullyQualifiedName();
    }
}
