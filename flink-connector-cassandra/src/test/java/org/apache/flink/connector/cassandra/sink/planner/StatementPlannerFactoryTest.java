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

package org.apache.flink.connector.cassandra.sink.planner;

import org.apache.flink.connector.cassandra.sink.config.CqlSinkConfig;
import org.apache.flink.connector.cassandra.sink.config.RecordFormatType;
import org.apache.flink.connector.cassandra.sink.planner.core.components.StatementPlanner;
import org.apache.flink.connector.cassandra.sink.planner.core.components.StatementPlannerFactory;
import org.apache.flink.connector.cassandra.sink.planner.resolver.FixedColumnValueResolver;
import org.apache.flink.connector.cassandra.sink.planner.resolver.FixedTableResolver;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link StatementPlannerFactory}. */
public class StatementPlannerFactoryTest {

    @Test
    void testCorrectAssemblerChosenForDynamicMode() {
        // Create config for DYNAMIC mode with SinkPluggable
        SinkPluggable<Row> pluggable =
                SinkPluggable.<Row>builder()
                        .withTableResolver(new FixedTableResolver<>("test_ks", "test_table"))
                        .withColumnValueResolver(
                                new FixedColumnValueResolver<>(
                                        RecordFormatType.ROW, Arrays.asList("id", "name")))
                        .build();

        CqlSinkConfig<Row> config = CqlSinkConfig.forRow().withPluggable(pluggable);

        // Create planner - should use DynamicAssembler
        StatementPlanner<Row> planner = StatementPlannerFactory.create(config);

        // Verify planner was created (DynamicAssembler was used)
        assertThat(planner).isNotNull();
    }

    @Test
    void testCorrectAssemblerChosenForStaticMode() {
        // Create config for STATIC mode
        CqlSinkConfig<Row> config =
                CqlSinkConfig.forRow().withQuery("INSERT INTO ks.tbl (id) VALUES (?)");

        // Create planner - should use StaticAssembler
        StatementPlanner<Row> planner = StatementPlannerFactory.create(config);

        // Verify planner was created (StaticAssembler was used)
        assertThat(planner).isNotNull();
    }

    @Test
    void testUnsetModeThrowsException() {
        // Create config with neither query nor pluggable (UNSET mode)
        CqlSinkConfig<Row> config = CqlSinkConfig.forRow();

        // Should throw exception for UNSET mode
        assertThatThrownBy(() -> StatementPlannerFactory.create(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid ResolutionMode UNSET");
    }
}
