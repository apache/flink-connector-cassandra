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
import org.apache.flink.connector.cassandra.sink.planner.core.components.StatementPlanner;
import org.apache.flink.connector.cassandra.sink.planner.core.customization.StatementCustomizer;
import org.apache.flink.connector.cassandra.sink.planner.core.modifiers.ClauseBindings;
import org.apache.flink.connector.cassandra.sink.planner.core.modifiers.CqlClauseResolver;
import org.apache.flink.connector.cassandra.sink.planner.core.strategy.PlannerStrategy;
import org.apache.flink.connector.cassandra.sink.planner.resolver.ColumnValueResolver;
import org.apache.flink.connector.cassandra.sink.planner.resolver.ResolvedWrite;
import org.apache.flink.connector.cassandra.sink.planner.resolver.TableRef;
import org.apache.flink.connector.cassandra.sink.planner.resolver.TableResolver;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Update;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Unit tests for {@link StatementPlanner}. */
public class StatementPlannerTest {

    @Mock private Session session;
    @Mock private PreparedStatement preparedStatement;
    @Mock private BoundStatement boundStatement;
    @Mock private TableResolver<String> tableResolver;
    @Mock private ColumnValueResolver<String> columnValueResolver;
    @Mock private PlannerStrategy<String> strategy;
    @Mock private StatementCustomizer<String> customizer;
    @Mock private CqlClauseResolver<String> clauseResolver;
    @Mock private CqlSinkConfig<String> config;
    // PreparedStatementCache is now internal to StatementPlanner

    private StatementPlanner<String> planner;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testDynamicInsertHappyPath() {
        // Setup
        String record = "test-record";
        TableRef tableRef = new TableRef("keyspace", "table");
        ResolvedWrite resolvedWrite =
                ResolvedWrite.insert(Arrays.asList("id", "name"), new Object[] {1, "Alice"});
        PlannerStrategy.QueryWithBindings queryWithBindings =
                new PlannerStrategy.QueryWithBindings(
                        "INSERT INTO keyspace.table (id, name) VALUES (?, ?)",
                        ClauseBindings.empty());

        when(tableResolver.resolve(record)).thenReturn(tableRef);
        when(columnValueResolver.resolve(record)).thenReturn(resolvedWrite);
        when(strategy.getQueryWithBindings(tableRef, config, clauseResolver, resolvedWrite, record))
                .thenReturn(queryWithBindings);
        when(strategy.getOrderedBindValues(resolvedWrite, ClauseBindings.empty()))
                .thenReturn(new Object[] {1, "Alice"});
        when(session.prepare(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.bind(any())).thenReturn(boundStatement);
        planner =
                new StatementPlanner<>(
                        tableResolver, columnValueResolver, strategy, clauseResolver, customizer);
        Statement result = planner.plan(record, session, config);
        // Verify
        assertThat(result).isSameAs(boundStatement);
        verify(tableResolver).resolve(record);
        verify(columnValueResolver).resolve(record);
        verify(strategy)
                .getQueryWithBindings(tableRef, config, clauseResolver, resolvedWrite, record);
        verify(session).prepare(anyString());
        verify(preparedStatement).bind(new Object[] {1, "Alice"}); // Values in correct order
        verify(customizer).apply(boundStatement, record);
    }

    @Test
    void testDynamicUpdateWithClauseModifiers() {
        // Setup
        String record = "test-record";
        TableRef tableRef = new TableRef("keyspace", "table");
        ResolvedWrite resolvedWrite =
                ResolvedWrite.update(
                        Arrays.asList("name", "email"),
                        new Object[] {"Bob", "bob@example.com"},
                        Collections.singletonList("id"),
                        new Object[] {123});
        ClauseBindings clauseBindings =
                new ClauseBindings(new Object[] {3600L}, new Object[0]); // TTL value
        PlannerStrategy.QueryWithBindings queryWithBindings =
                new PlannerStrategy.QueryWithBindings(
                        "UPDATE keyspace.table SET name = ?, email = ? WHERE id = ? USING TTL ?",
                        clauseBindings);

        when(tableResolver.resolve(record)).thenReturn(tableRef);
        when(columnValueResolver.resolve(record)).thenReturn(resolvedWrite);
        // Mock both methods since we don't know which will be called
        when(clauseResolver.applyTo(any(Insert.class), eq(record))).thenReturn(clauseBindings);
        when(clauseResolver.applyTo(any(Update.class), eq(record))).thenReturn(clauseBindings);
        when(strategy.getQueryWithBindings(tableRef, config, clauseResolver, resolvedWrite, record))
                .thenReturn(queryWithBindings);
        when(strategy.getOrderedBindValues(resolvedWrite, clauseBindings))
                .thenReturn(new Object[] {"Bob", "bob@example.com", 123, 3600L});
        when(session.prepare(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.bind(any())).thenReturn(boundStatement);
        // customizer now returns void

        planner =
                new StatementPlanner<>(
                        tableResolver, columnValueResolver, strategy, clauseResolver, customizer);

        // Execute
        Statement result = planner.plan(record, session, config);

        // Verify bind order
        verify(preparedStatement).bind(new Object[] {"Bob", "bob@example.com", 123, 3600L});
        verify(customizer).apply(boundStatement, record);
    }

    @Test
    void testCacheReuseOnSecondCall() {
        // Setup
        String record = "test-record";
        TableRef tableRef = new TableRef("keyspace", "table");
        ResolvedWrite resolvedWrite =
                ResolvedWrite.insert(Collections.singletonList("id"), new Object[] {1});
        PlannerStrategy.QueryWithBindings queryWithBindings =
                new PlannerStrategy.QueryWithBindings(
                        "INSERT INTO keyspace.table (id) VALUES (?)", ClauseBindings.empty());

        when(tableResolver.resolve(record)).thenReturn(tableRef);
        when(columnValueResolver.resolve(record)).thenReturn(resolvedWrite);
        when(strategy.getQueryWithBindings(tableRef, config, clauseResolver, resolvedWrite, record))
                .thenReturn(queryWithBindings);
        when(strategy.getOrderedBindValues(resolvedWrite, ClauseBindings.empty()))
                .thenReturn(new Object[] {1});
        when(session.prepare(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.bind(any())).thenReturn(boundStatement);
        // customizer now returns void

        planner =
                new StatementPlanner<>(
                        tableResolver, columnValueResolver, strategy, clauseResolver, customizer);

        // Execute twice
        planner.plan(record, session, config);
        planner.plan(record, session, config);

        // Verify cache is used (prepare should only be called once due to internal caching)
        verify(session, times(1)).prepare(anyString());
        verify(strategy, times(2))
                .getQueryWithBindings(tableRef, config, clauseResolver, resolvedWrite, record);
    }

    @Test
    void testStaticModeIgnoresClauseResolver() {
        // Setup for static mode
        String record = "test-record";
        TableRef tableRef = new TableRef("keyspace", "table");
        ResolvedWrite resolvedWrite =
                ResolvedWrite.insert(Arrays.asList("id", "name"), new Object[] {1, "Alice"});
        PlannerStrategy.QueryWithBindings queryWithBindings =
                new PlannerStrategy.QueryWithBindings(
                        "INSERT INTO keyspace.table (id, name) VALUES (?, ?)",
                        ClauseBindings.empty());

        when(tableResolver.resolve(record)).thenReturn(tableRef);
        when(columnValueResolver.resolve(record)).thenReturn(resolvedWrite);
        // Note: null passed for clauseResolver in static mode
        when(strategy.getQueryWithBindings(
                        eq(tableRef), eq(config), any(), eq(resolvedWrite), eq(record)))
                .thenReturn(queryWithBindings);
        when(strategy.getOrderedBindValues(resolvedWrite, ClauseBindings.empty()))
                .thenReturn(new Object[] {1, "Alice"});
        when(session.prepare(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.bind(any())).thenReturn(boundStatement);
        // customizer now returns void

        // Use null clauseResolver for static mode
        planner =
                new StatementPlanner<>(
                        tableResolver, columnValueResolver, strategy, null, customizer);

        // Execute
        Statement result = planner.plan(record, session, config);

        // Verify clauseResolver was not used (any() matcher allows NoOpClauseResolver)
        verify(strategy)
                .getQueryWithBindings(
                        eq(tableRef), eq(config), any(), eq(resolvedWrite), eq(record));
        verifyNoInteractions(clauseResolver);
        assertThat(result).isSameAs(boundStatement);
    }

    @Test
    void testExceptionPropagationFromComponents() {
        // Test 1: TableResolver exception propagates
        RuntimeException tableError = new RuntimeException("Table resolution failed");
        when(tableResolver.resolve(any())).thenThrow(tableError);

        planner =
                new StatementPlanner<>(
                        tableResolver, columnValueResolver, strategy, clauseResolver, customizer);

        assertThatThrownBy(() -> planner.plan("record", session, config)).isSameAs(tableError);

        // Reset mocks for next test
        MockitoAnnotations.openMocks(this);

        // Test 2: ColumnValueResolver exception propagates
        when(tableResolver.resolve(any())).thenReturn(new TableRef("ks", "tbl"));
        RuntimeException columnError = new RuntimeException("Column resolution failed");
        when(columnValueResolver.resolve(any())).thenThrow(columnError);

        planner =
                new StatementPlanner<>(
                        tableResolver, columnValueResolver, strategy, clauseResolver, customizer);

        assertThatThrownBy(() -> planner.plan("record", session, config)).isSameAs(columnError);

        // Reset mocks for next test
        MockitoAnnotations.openMocks(this);

        // Test 3: Strategy exception propagates
        when(tableResolver.resolve(any())).thenReturn(new TableRef("ks", "tbl"));
        when(columnValueResolver.resolve(any()))
                .thenReturn(
                        ResolvedWrite.insert(Collections.singletonList("id"), new Object[] {1}));
        IllegalArgumentException strategyError =
                new IllegalArgumentException("Wrong operation kind");
        when(strategy.getQueryWithBindings(any(), any(), any(), any(), any()))
                .thenThrow(strategyError);

        planner =
                new StatementPlanner<>(
                        tableResolver, columnValueResolver, strategy, clauseResolver, customizer);

        assertThatThrownBy(() -> planner.plan("record", session, config)).isSameAs(strategyError);
    }

    @Test
    void testSessionPrepareFailurePropagates() {
        // Setup
        String record = "test-record";
        TableRef tableRef = new TableRef("keyspace", "table");
        ResolvedWrite resolvedWrite =
                ResolvedWrite.insert(Collections.singletonList("id"), new Object[] {1});
        PlannerStrategy.QueryWithBindings queryWithBindings =
                new PlannerStrategy.QueryWithBindings(
                        "INSERT INTO keyspace.table (id) VALUES (?)", ClauseBindings.empty());

        when(tableResolver.resolve(record)).thenReturn(tableRef);
        when(columnValueResolver.resolve(record)).thenReturn(resolvedWrite);
        when(strategy.getQueryWithBindings(tableRef, config, clauseResolver, resolvedWrite, record))
                .thenReturn(queryWithBindings);

        RuntimeException prepareError = new RuntimeException("Session prepare failed");
        when(session.prepare(anyString())).thenThrow(prepareError);

        planner =
                new StatementPlanner<>(
                        tableResolver, columnValueResolver, strategy, clauseResolver, customizer);

        assertThatThrownBy(() -> planner.plan(record, session, config))
                .isInstanceOf(com.google.common.util.concurrent.UncheckedExecutionException.class)
                .hasCauseInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("Session prepare failed");
    }

    @Test
    void testNullCustomizerAllowed() {
        // Setup with null customizer
        String record = "test-record";
        TableRef tableRef = new TableRef("keyspace", "table");
        ResolvedWrite resolvedWrite =
                ResolvedWrite.insert(Collections.singletonList("id"), new Object[] {1});
        PlannerStrategy.QueryWithBindings queryWithBindings =
                new PlannerStrategy.QueryWithBindings(
                        "INSERT INTO keyspace.table (id) VALUES (?)", ClauseBindings.empty());

        when(tableResolver.resolve(record)).thenReturn(tableRef);
        when(columnValueResolver.resolve(record)).thenReturn(resolvedWrite);
        when(strategy.getQueryWithBindings(tableRef, config, clauseResolver, resolvedWrite, record))
                .thenReturn(queryWithBindings);
        when(strategy.getOrderedBindValues(resolvedWrite, ClauseBindings.empty()))
                .thenReturn(new Object[] {1});
        when(session.prepare(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.bind(any())).thenReturn(boundStatement);

        planner =
                new StatementPlanner<>(
                        tableResolver, columnValueResolver, strategy, clauseResolver, null);

        // Execute
        Statement result = planner.plan(record, session, config);

        // Verify customizer not used
        assertThat(result).isSameAs(boundStatement);
        verifyNoInteractions(customizer);
    }
}
