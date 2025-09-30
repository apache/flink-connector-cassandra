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

package org.apache.flink.connector.cassandra.sink.assembler;

import org.apache.flink.connector.cassandra.sink.config.CqlSinkConfig;
import org.apache.flink.connector.cassandra.sink.planner.SinkPluggable;
import org.apache.flink.connector.cassandra.sink.planner.core.components.StatementPlanner;
import org.apache.flink.connector.cassandra.sink.planner.core.customization.StatementCustomizer;
import org.apache.flink.connector.cassandra.sink.planner.core.modifiers.CqlClauseResolver;
import org.apache.flink.connector.cassandra.sink.planner.resolver.ColumnValueResolver;
import org.apache.flink.connector.cassandra.sink.planner.resolver.TableResolver;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

/** Unit tests for {@link DynamicPlannerAssembler}. */
public class DynamicPlannerAssemblerTest {

    @Mock private CqlSinkConfig<String> config;
    @Mock private TableResolver<String> tableResolver;
    @Mock private ColumnValueResolver<String> columnValueResolver;
    @Mock private CqlClauseResolver<String> clauseResolver;
    @Mock private StatementCustomizer<String> customizer;

    private DynamicPlannerAssembler<String> assembler;
    private SinkPluggable<String> pluggable;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        assembler = new DynamicPlannerAssembler<>(config);
    }

    @Test
    void testAssembleWithInsertStrategy() {
        // Setup
        pluggable =
                SinkPluggable.<String>builder()
                        .withTableResolver(tableResolver)
                        .withColumnValueResolver(columnValueResolver)
                        .withCqlClauseResolver(clauseResolver)
                        .withStatementCustomizer(customizer)
                        .build();

        when(config.getPluggable()).thenReturn(pluggable);
        when(columnValueResolver.kind()).thenReturn(ColumnValueResolver.Kind.INSERT);

        // Execute
        StatementPlanner<String> planner = assembler.assemble();

        // Verify
        assertThat(planner).isNotNull();
    }

    @Test
    void testAssembleWithUpdateStrategy() {
        // Setup
        pluggable =
                SinkPluggable.<String>builder()
                        .withTableResolver(tableResolver)
                        .withColumnValueResolver(columnValueResolver)
                        .withCqlClauseResolver(clauseResolver)
                        .withStatementCustomizer(customizer)
                        .build();

        when(config.getPluggable()).thenReturn(pluggable);
        when(columnValueResolver.kind()).thenReturn(ColumnValueResolver.Kind.UPDATE);

        // Execute
        StatementPlanner<String> planner = assembler.assemble();

        // Verify
        assertThat(planner).isNotNull();
    }

    @Test
    void testAssembleValidationAndEdgeCases() {
        // Test 1: Null optional components (clauseResolver and customizer) are allowed
        pluggable =
                SinkPluggable.<String>builder()
                        .withTableResolver(tableResolver)
                        .withColumnValueResolver(columnValueResolver)
                        .withCqlClauseResolver(null) // Optional
                        .withStatementCustomizer(null) // Optional
                        .build();

        when(config.getPluggable()).thenReturn(pluggable);
        when(columnValueResolver.kind()).thenReturn(ColumnValueResolver.Kind.INSERT);

        StatementPlanner<String> planner = assembler.assemble();
        assertThat(planner).isNotNull();

        // Test 2: Null SinkPluggable throws exception
        when(config.getPluggable()).thenReturn(null);
        assertThatThrownBy(() -> assembler.assemble())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SinkPluggable cannot be null in DYNAMIC mode");

        // Test 3: Null TableResolver throws exception when building pluggable
        assertThatThrownBy(
                        () ->
                                SinkPluggable.<String>builder()
                                        .withTableResolver(null)
                                        .withColumnValueResolver(columnValueResolver)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("TableResolver cannot be null");

        // Test 4: Null ColumnValueResolver throws exception when building pluggable
        assertThatThrownBy(
                        () ->
                                SinkPluggable.<String>builder()
                                        .withTableResolver(tableResolver)
                                        .withColumnValueResolver(null)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ColumnValueResolver cannot be null");

        // Test 5: Null resolver kind throws NullPointerException
        pluggable =
                SinkPluggable.<String>builder()
                        .withTableResolver(tableResolver)
                        .withColumnValueResolver(columnValueResolver)
                        .build();

        when(config.getPluggable()).thenReturn(pluggable);
        when(columnValueResolver.kind()).thenReturn(null); // Simulate unknown kind

        assertThatThrownBy(() -> assembler.assemble())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("resolverKind");

        // Test 6: Null config in constructor is allowed but fails on assemble
        assertThatThrownBy(() -> new DynamicPlannerAssembler<>(null).assemble())
                .isInstanceOf(NullPointerException.class);
    }
}
