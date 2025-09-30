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

package org.apache.flink.connector.cassandra.sink.planner.core.resolution;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Defines how CQL statements handle value bindings and field resolution.
 *
 * <p>This enum controls whether field positions and values are resolved at runtime (DYNAMIC) or
 * pre-computed (STATIC). It affects both query generation and value binding in Cassandra sink
 * operations.
 */
@PublicEvolving
public enum ResolutionMode {

    /** Indicates configuration hasn't been set yet. */
    UNSET,

    /**
     * Dynamic resolution mode - field positions and values are resolved at runtime.
     *
     * <p>In this mode:
     *
     * <ul>
     *   <li>CQL statements use bind markers (?) for all fields
     *   <li>Field positions are computed for each record during binding
     * </ul>
     *
     * <p>Example:
     *
     * <pre>
     * INSERT INTO users (id, name, age) VALUES (?, ?, ?)
     * // Field positions resolved at runtime for each record
     * </pre>
     */
    DYNAMIC,

    /**
     * Static resolution mode - field positions and values are pre-computed.
     *
     * <p>In this mode:
     *
     * <ul>
     *   <li>CQL statements may have literal values for key fields
     *   <li>Field positions are pre-computed during query parsing
     * </ul>
     *
     * <p>Example:
     *
     * <pre>
     * UPDATE users SET name = ?, age = ? WHERE id = 'user123'
     * // Key field 'id' has literal value, positions pre-computed
     * </pre>
     */
    STATIC
}
