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

package org.apache.flink.connector.cassandra.sink.planner.core.strategy;

import org.apache.flink.connector.cassandra.sink.config.CqlSinkConfig;
import org.apache.flink.connector.cassandra.sink.planner.core.modifiers.CqlClauseResolver;
import org.apache.flink.connector.cassandra.sink.planner.resolver.ResolvedWrite;
import org.apache.flink.connector.cassandra.sink.planner.resolver.TableRef;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

/** Unit tests for {@link StaticInsertStrategy}. */
class StaticInsertStrategyTest {

    @Mock private CqlSinkConfig<Row> config;
    @Mock private CqlClauseResolver<Row> clauseResolver;

    private StaticInsertStrategy<Row> strategy;
    private TableRef table;
    private Row record;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        strategy = new StaticInsertStrategy<>();
        table = new TableRef("test_keyspace", "test_table");
        record = Row.of(1, "Alice", "alice@example.com");
    }

    @Test
    void testStaticInsertQuery() {
        String staticQuery =
                "INSERT INTO test_keyspace.test_table (id, name, email) VALUES (?, ?, ?)";
        when(config.getQuery()).thenReturn(staticQuery);

        ResolvedWrite write =
                ResolvedWrite.insert(
                        Arrays.asList("id", "name", "email"),
                        new Object[] {1, "Alice", "alice@example.com"});

        PlannerStrategy.QueryWithBindings result =
                strategy.getQueryWithBindings(table, config, clauseResolver, write, record);

        assertThat(result.query).isEqualTo(staticQuery);
        assertThat(result.clauseBindings).isNotNull();
        assertThat(result.clauseBindings.getUsingValues()).isEmpty();
        assertThat(result.clauseBindings.getIfValues()).isEmpty();

        Object[] bindValues = strategy.getOrderedBindValues(write, null);
        assertThat(bindValues).containsExactly(1, "Alice", "alice@example.com");
    }

    @Test
    void testStaticInsertWithTTL() {
        String staticQuery =
                "INSERT INTO test_keyspace.test_table (id, name) VALUES (?, ?) USING TTL 3600";
        when(config.getQuery()).thenReturn(staticQuery);

        ResolvedWrite write =
                ResolvedWrite.insert(Arrays.asList("id", "name"), new Object[] {1, "Alice"});

        PlannerStrategy.QueryWithBindings result =
                strategy.getQueryWithBindings(table, config, clauseResolver, write, record);

        assertThat(result.query).isEqualTo(staticQuery);
        assertThat(result.clauseBindings).isNotNull();
        assertThat(result.clauseBindings.getUsingValues()).isEmpty();
        assertThat(result.clauseBindings.getIfValues()).isEmpty();

        Object[] bindValues = strategy.getOrderedBindValues(write, null);
        assertThat(bindValues).containsExactly(1, "Alice");
    }

    @Test
    void testStaticInsertWithIfNotExists() {
        String staticQuery =
                "INSERT INTO test_keyspace.test_table (id, name) VALUES (?, ?) IF NOT EXISTS";
        when(config.getQuery()).thenReturn(staticQuery);

        ResolvedWrite write =
                ResolvedWrite.insert(Arrays.asList("id", "name"), new Object[] {1, "Alice"});

        PlannerStrategy.QueryWithBindings result =
                strategy.getQueryWithBindings(table, config, clauseResolver, write, record);

        assertThat(result.query).isEqualTo(staticQuery);
        assertThat(result.clauseBindings).isNotNull();
        assertThat(result.clauseBindings.getUsingValues()).isEmpty();
        assertThat(result.clauseBindings.getIfValues()).isEmpty();

        Object[] bindValues = strategy.getOrderedBindValues(write, null);
        assertThat(bindValues).containsExactly(1, "Alice");
    }

    @Test
    void testStaticInsertFailsForUpdateOperation() {
        String staticQuery = "INSERT INTO test_keyspace.test_table (id, name) VALUES (?, ?)";
        when(config.getQuery()).thenReturn(staticQuery);

        ResolvedWrite write =
                ResolvedWrite.update(
                        Collections.singletonList("name"),
                        new Object[] {"Bob"},
                        Collections.singletonList("id"),
                        new Object[] {1});

        assertThatThrownBy(
                        () ->
                                strategy.getQueryWithBindings(
                                        table, config, clauseResolver, write, record))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("StaticInsertStrategy got UPDATE");
    }

    @Test
    void testStaticInsertWithComplexQuery() {
        String staticQuery =
                "INSERT INTO test_keyspace.test_table (id, data) VALUES (?, ?) "
                        + "USING TTL 7200 AND TIMESTAMP 1234567890";
        when(config.getQuery()).thenReturn(staticQuery);

        ResolvedWrite write =
                ResolvedWrite.insert(Arrays.asList("id", "data"), new Object[] {1, "complex_data"});

        PlannerStrategy.QueryWithBindings result =
                strategy.getQueryWithBindings(table, config, clauseResolver, write, record);

        assertThat(result.query).isEqualTo(staticQuery);
        assertThat(result.clauseBindings).isNotNull();
        assertThat(result.clauseBindings.getUsingValues()).isEmpty();
        assertThat(result.clauseBindings.getIfValues()).isEmpty();

        Object[] bindValues = strategy.getOrderedBindValues(write, null);
        assertThat(bindValues).containsExactly(1, "complex_data");
    }
}
