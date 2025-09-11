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
import org.apache.flink.connector.cassandra.sink.planner.core.modifiers.ClauseBindings;
import org.apache.flink.connector.cassandra.sink.planner.core.modifiers.CqlClauseResolver;
import org.apache.flink.connector.cassandra.sink.planner.resolver.ResolvedWrite;
import org.apache.flink.connector.cassandra.sink.planner.resolver.TableRef;
import org.apache.flink.types.Row;

import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/** Unit tests for {@link InsertStrategy}. */
class InsertStrategyTest {

    @Mock private CqlSinkConfig<Row> config;
    @Mock private CqlClauseResolver<Row> clauseResolver;

    private InsertStrategy<Row> strategy;
    private TableRef table;
    private Row record;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        strategy = new InsertStrategy<>();
        table = new TableRef("test_keyspace", "test_table");
        record = Row.of(1, "Alice", "alice@example.com");
    }

    @Test
    void testSimpleInsertWithoutClauses() {
        ResolvedWrite write =
                ResolvedWrite.insert(
                        Arrays.asList("id", "name", "email"),
                        new Object[] {1, "Alice", "alice@example.com"});

        when(clauseResolver.applyTo(any(Insert.class), eq(record)))
                .thenReturn(ClauseBindings.empty());

        PlannerStrategy.QueryWithBindings result =
                strategy.getQueryWithBindings(table, config, clauseResolver, write, record);

        assertThat(result.query)
                .startsWith("INSERT INTO test_keyspace.test_table")
                .contains("(id,name,email) VALUES (?,?,?)");
        assertThat(result.clauseBindings.getUsingValues()).isEmpty();
        assertThat(result.clauseBindings.getIfValues()).isEmpty();

        Object[] bindValues = strategy.getOrderedBindValues(write, result.clauseBindings);
        assertThat(bindValues).containsExactly(1, "Alice", "alice@example.com");
    }

    @Test
    void testInsertWithTTLClause() {
        ResolvedWrite write =
                ResolvedWrite.insert(Arrays.asList("id", "name"), new Object[] {1, "Alice"});

        when(clauseResolver.applyTo(any(Insert.class), eq(record)))
                .thenAnswer(
                        invocation -> {
                            Insert insert = invocation.getArgument(0);
                            insert.using(QueryBuilder.ttl(QueryBuilder.bindMarker()));
                            return new ClauseBindings(new Object[] {3600}, null);
                        });

        PlannerStrategy.QueryWithBindings result =
                strategy.getQueryWithBindings(table, config, clauseResolver, write, record);

        assertThat(result.query)
                .contains("INSERT INTO test_keyspace.test_table")
                .contains("VALUES (?,?)")
                .contains("USING TTL ?");
        assertThat(result.clauseBindings.getUsingValues()).containsExactly(3600);
        assertThat(result.clauseBindings.getIfValues()).isEmpty();

        Object[] bindValues = strategy.getOrderedBindValues(write, result.clauseBindings);
        assertThat(bindValues).containsExactly(1, "Alice", 3600);
    }

    @Test
    void testInsertWithMultipleClauses() {
        ResolvedWrite write =
                ResolvedWrite.insert(Collections.singletonList("id"), new Object[] {1});

        when(clauseResolver.applyTo(any(Insert.class), eq(record)))
                .thenAnswer(
                        invocation -> {
                            Insert insert = invocation.getArgument(0);
                            insert.using(QueryBuilder.ttl(QueryBuilder.bindMarker()))
                                    .and(QueryBuilder.timestamp(QueryBuilder.bindMarker()));
                            return new ClauseBindings(new Object[] {7200, 1234567890L}, null);
                        });

        PlannerStrategy.QueryWithBindings result =
                strategy.getQueryWithBindings(table, config, clauseResolver, write, record);

        assertThat(result.query).contains("USING TTL ?").contains("AND TIMESTAMP ?");
        assertThat(result.clauseBindings.getUsingValues()).containsExactly(7200, 1234567890L);
        assertThat(result.clauseBindings.getIfValues()).isEmpty();

        Object[] bindValues = strategy.getOrderedBindValues(write, result.clauseBindings);
        assertThat(bindValues).containsExactly(1, 7200, 1234567890L);
    }

    @Test
    void testInsertWithIfNotExists() {
        ResolvedWrite write =
                ResolvedWrite.insert(Arrays.asList("id", "name"), new Object[] {1, "Alice"});

        when(clauseResolver.applyTo(any(Insert.class), eq(record)))
                .thenAnswer(
                        invocation -> {
                            Insert insert = invocation.getArgument(0);
                            insert.ifNotExists();
                            return ClauseBindings.empty();
                        });

        PlannerStrategy.QueryWithBindings result =
                strategy.getQueryWithBindings(table, config, clauseResolver, write, record);

        assertThat(result.query).contains("IF NOT EXISTS");
        assertThat(result.clauseBindings.getUsingValues()).isEmpty();
        assertThat(result.clauseBindings.getIfValues()).isEmpty();

        Object[] bindValues = strategy.getOrderedBindValues(write, result.clauseBindings);
        assertThat(bindValues).containsExactly(1, "Alice");
    }

    @Test
    void testInsertFailsForUpdateOperation() {
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
                .hasMessageContaining("InsertStrategy received UPDATE");
    }

    @Test
    void testGetOrderedBindValuesWithEmptyClauseBindings() {
        ResolvedWrite write =
                ResolvedWrite.insert(Arrays.asList("id", "name"), new Object[] {1, "Alice"});

        Object[] bindValues = strategy.getOrderedBindValues(write, ClauseBindings.empty());
        assertThat(bindValues).containsExactly(1, "Alice");
    }

    @Test
    void testGetOrderedBindValuesWithClauseBindings() {
        ResolvedWrite write =
                ResolvedWrite.insert(Arrays.asList("id", "name"), new Object[] {1, "Alice"});

        ClauseBindings clauseBindings = new ClauseBindings(new Object[] {3600, 1234567890L}, null);
        Object[] bindValues = strategy.getOrderedBindValues(write, clauseBindings);
        assertThat(bindValues).containsExactly(1, "Alice", 3600, 1234567890L);
    }
}
