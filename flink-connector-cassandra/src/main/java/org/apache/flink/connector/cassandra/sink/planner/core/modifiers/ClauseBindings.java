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

package org.apache.flink.connector.cassandra.sink.planner.core.modifiers;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * Holds bind values for CQL clause modifiers (USING and IF clauses).
 *
 * <p>This class separates USING clause values (TTL, TIMESTAMP) from IF clause values to ensure
 * correct bind order in prepared statements.
 */
@PublicEvolving
public final class ClauseBindings implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final ClauseBindings EMPTY = new ClauseBindings(new Object[0], new Object[0]);

    private final Object[] usingValues; // TTL, TIMESTAMP values
    private final Object[] ifValues; // IF condition values

    public ClauseBindings(Object[] usingValues, Object[] ifValues) {
        this.usingValues = usingValues != null ? usingValues : new Object[0];
        this.ifValues = ifValues != null ? ifValues : new Object[0];
    }

    public Object[] getUsingValues() {
        return usingValues;
    }

    public Object[] getIfValues() {
        return ifValues;
    }

    public static ClauseBindings empty() {
        return EMPTY;
    }
}
