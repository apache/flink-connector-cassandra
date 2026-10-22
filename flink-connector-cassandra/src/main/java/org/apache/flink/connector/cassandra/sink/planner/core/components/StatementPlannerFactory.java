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

package org.apache.flink.connector.cassandra.sink.planner.core.components;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.cassandra.sink.assembler.DynamicPlannerAssembler;
import org.apache.flink.connector.cassandra.sink.assembler.StaticPlannerAssembler;
import org.apache.flink.connector.cassandra.sink.config.CqlSinkConfig;
import org.apache.flink.connector.cassandra.sink.planner.core.resolution.ResolutionMode;

/**
 * Factory for creating StatementPlanner's with appropriate strategies based on sink configuration.
 */
@Internal
public final class StatementPlannerFactory {

    /**
     * Creates a StatementPlanner for the given configuration.
     *
     * @param config the sink configuration
     * @param <INPUT> the record type
     * @return a configured StatementPlanner
     */
    public static <INPUT> StatementPlanner<INPUT> create(CqlSinkConfig<INPUT> config) {
        ResolutionMode mode = config.getResolutionMode();
        switch (mode) {
            case UNSET:
                throw new IllegalArgumentException(
                        String.format(
                                "Invalid ResolutionMode %s, Planner cannot be constructed.", mode));
            case DYNAMIC:
                return new DynamicPlannerAssembler<>(config).assemble();
            case STATIC:
                return new StaticPlannerAssembler<>(config).assemble();
        }
        return null;
    }
}
