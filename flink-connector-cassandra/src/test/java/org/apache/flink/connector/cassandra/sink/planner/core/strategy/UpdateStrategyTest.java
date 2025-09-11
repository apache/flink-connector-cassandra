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

import com.datastax.driver.core.querybuilder.QueryBuilder;
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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/** Unit tests for {@link UpdateStrategy}. */
class UpdateStrategyTest {

    @Mock private CqlSinkConfig<Row> config;
    @Mock private CqlClauseResolver<Row> clauseResolver;

    private UpdateStrategy<Row> strategy;
    private TableRef table;
    private Row record;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        strategy = new UpdateStrategy<>();
        table = new TableRef("test_keyspace", "test_table");
        record = Row.of("Bob", 25, 1);
    }

    @Test
    void testSimpleUpdateWithoutClauses() {
        ResolvedWrite write =
                ResolvedWrite.update(
                        Arrays.asList("name", "age"),
                        new Object[] {"Bob", 25},
                        Collections.singletonList("id"),
                        new Object[] {1});

        when(clauseResolver.applyTo(any(Update.class), eq(record)))
                .thenReturn(ClauseBindings.empty());

        PlannerStrategy.QueryWithBindings result =
                strategy.getQueryWithBindings(table, config, clauseResolver, write, record);

        assertThat(result.query)
                .isEqualTo("UPDATE test_keyspace.test_table SET name=?,age=? WHERE id=?;");
        assertThat(result.clauseBindings.getUsingValues()).isEmpty();
        assertThat(result.clauseBindings.getIfValues()).isEmpty();

        Object[] bindValues = strategy.getOrderedBindValues(write, result.clauseBindings);
        assertThat(bindValues).containsExactly("Bob", 25, 1);
    }

    @Test
    void testUpdateWithUsingTTL() {
        ResolvedWrite write =
                ResolvedWrite.update(
                        Collections.singletonList("name"),
                        new Object[] {"Bob"},
                        Collections.singletonList("id"),
                        new Object[] {1});

        when(clauseResolver.applyTo(any(Update.class), eq(record)))
                .thenAnswer(
                        invocation -> {
                            Update update = invocation.getArgument(0);
                            update.using(QueryBuilder.ttl(QueryBuilder.bindMarker()));
                            return new ClauseBindings(new Object[] {3600}, null);
                        });

        PlannerStrategy.QueryWithBindings result =
                strategy.getQueryWithBindings(table, config, clauseResolver, write, record);

        assertThat(result.query)
                .contains("UPDATE test_keyspace.test_table")
                .contains("USING TTL ?")
                .contains("SET name=?")
                .contains("WHERE id=?");
        assertThat(result.clauseBindings.getUsingValues()).containsExactly(3600);
        assertThat(result.clauseBindings.getIfValues()).isEmpty();

        // For UPDATE with USING, the order is: USING values, SET values, WHERE values
        Object[] bindValues = strategy.getOrderedBindValues(write, result.clauseBindings);
        assertThat(bindValues).containsExactly(3600, "Bob", 1);
    }

    @Test
    void testUpdateWithUsingTimestamp() {
        ResolvedWrite write =
                ResolvedWrite.update(
                        Collections.singletonList("status"),
                        new Object[] {"active"},
                        Collections.singletonList("id"),
                        new Object[] {123});

        when(clauseResolver.applyTo(any(Update.class), eq(record)))
                .thenAnswer(
                        invocation -> {
                            Update update = invocation.getArgument(0);
                            update.using(QueryBuilder.timestamp(QueryBuilder.bindMarker()));
                            return new ClauseBindings(new Object[] {1234567890L}, null);
                        });

        PlannerStrategy.QueryWithBindings result =
                strategy.getQueryWithBindings(table, config, clauseResolver, write, record);

        assertThat(result.query).contains("USING TIMESTAMP ?");
        assertThat(result.clauseBindings.getUsingValues()).containsExactly(1234567890L);
        assertThat(result.clauseBindings.getIfValues()).isEmpty();

        Object[] bindValues = strategy.getOrderedBindValues(write, result.clauseBindings);
        assertThat(bindValues).containsExactly(1234567890L, "active", 123);
    }

    @Test
    void testUpdateWithIfCondition() {
        ResolvedWrite write =
                ResolvedWrite.update(
                        Collections.singletonList("status"),
                        new Object[] {"completed"},
                        Collections.singletonList("id"),
                        new Object[] {456});

        when(clauseResolver.applyTo(any(Update.class), eq(record)))
                .thenAnswer(
                        invocation -> {
                            Update update = invocation.getArgument(0);
                            update.onlyIf(QueryBuilder.eq("status", QueryBuilder.bindMarker()));
                            return new ClauseBindings(null, new Object[] {"pending"});
                        });

        PlannerStrategy.QueryWithBindings result =
                strategy.getQueryWithBindings(table, config, clauseResolver, write, record);

        assertThat(result.query)
                .contains("UPDATE test_keyspace.test_table")
                .contains("SET status=?")
                .contains("WHERE id=?")
                .contains("IF status=?");
        assertThat(result.clauseBindings.getUsingValues()).isEmpty();
        assertThat(result.clauseBindings.getIfValues()).containsExactly("pending");

        // For UPDATE with IF, the order is: SET values, WHERE values, IF values
        Object[] bindValues = strategy.getOrderedBindValues(write, result.clauseBindings);
        assertThat(bindValues).containsExactly("completed", 456, "pending");
    }

    @Test
    void testUpdateWithMultipleWhereColumns() {
        ResolvedWrite write =
                ResolvedWrite.update(
                        Collections.singletonList("status"),
                        new Object[] {"active"},
                        Arrays.asList("partition_key", "clustering_key"),
                        new Object[] {"partition1", "cluster1"});

        when(clauseResolver.applyTo(any(Update.class), eq(record)))
                .thenReturn(ClauseBindings.empty());

        PlannerStrategy.QueryWithBindings result =
                strategy.getQueryWithBindings(table, config, clauseResolver, write, record);

        assertThat(result.query)
                .isEqualTo(
                        "UPDATE test_keyspace.test_table SET status=? WHERE partition_key=? AND clustering_key=?;");

        Object[] bindValues = strategy.getOrderedBindValues(write, result.clauseBindings);
        assertThat(bindValues).containsExactly("active", "partition1", "cluster1");
    }

    @Test
    void testUpdateWithNullValues() {
        ResolvedWrite write =
                ResolvedWrite.update(
                        Arrays.asList("name", "email"),
                        new Object[] {"Bob", null},
                        Collections.singletonList("id"),
                        new Object[] {1});

        when(clauseResolver.applyTo(any(Update.class), eq(record)))
                .thenReturn(ClauseBindings.empty());

        PlannerStrategy.QueryWithBindings result =
                strategy.getQueryWithBindings(table, config, clauseResolver, write, record);

        Object[] bindValues = strategy.getOrderedBindValues(write, result.clauseBindings);
        assertThat(bindValues).containsExactly("Bob", null, 1);
    }

    @Test
    void testUpdateFailsForInsertOperation() {
        ResolvedWrite write =
                ResolvedWrite.insert(Arrays.asList("id", "name"), new Object[] {1, "Alice"});

        assertThatThrownBy(
                        () ->
                                strategy.getQueryWithBindings(
                                        table, config, clauseResolver, write, record))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("UpdateStrategy received INSERT");
    }

    @Test
    void testGetOrderedBindValuesWithUsingClause() {
        ResolvedWrite write =
                ResolvedWrite.update(
                        Arrays.asList("name", "age"),
                        new Object[] {"Bob", 25},
                        Collections.singletonList("id"),
                        new Object[] {1});

        // Simulate USING TTL and TIMESTAMP
        ClauseBindings clauseBindings = new ClauseBindings(new Object[] {3600, 1234567890L}, null);
        Object[] bindValues = strategy.getOrderedBindValues(write, clauseBindings);

        // USING values come first for UPDATE
        assertThat(bindValues).containsExactly(3600, 1234567890L, "Bob", 25, 1);
    }

    @Test
    void testGetOrderedBindValuesWithIfClause() {
        ResolvedWrite write =
                ResolvedWrite.update(
                        Collections.singletonList("status"),
                        new Object[] {"completed"},
                        Collections.singletonList("id"),
                        new Object[] {456});

        // IF clause values
        ClauseBindings clauseBindings = new ClauseBindings(null, new Object[] {"pending"});
        Object[] bindValues = strategy.getOrderedBindValues(write, clauseBindings);

        // IF values come last for UPDATE
        assertThat(bindValues).containsExactly("completed", 456, "pending");
    }

    @Test
    void testComplexUpdateWithMultipleClauses() {
        ResolvedWrite write =
                ResolvedWrite.update(
                        Arrays.asList("name", "age", "status"),
                        new Object[] {"Bob", 25, "active"},
                        Arrays.asList("partition_key", "clustering_key"),
                        new Object[] {"pk1", "ck1"});

        when(clauseResolver.applyTo(any(Update.class), eq(record)))
                .thenAnswer(
                        invocation -> {
                            Update update = invocation.getArgument(0);
                            update.using(QueryBuilder.ttl(QueryBuilder.bindMarker()))
                                    .and(QueryBuilder.timestamp(QueryBuilder.bindMarker()))
                                    .onlyIf(QueryBuilder.eq("version", QueryBuilder.bindMarker()));
                            return new ClauseBindings(
                                    new Object[] {7200, 9876543210L}, new Object[] {5});
                        });

        PlannerStrategy.QueryWithBindings result =
                strategy.getQueryWithBindings(table, config, clauseResolver, write, record);

        assertThat(result.query)
                .contains("USING TTL ?")
                .contains("AND TIMESTAMP ?")
                .contains("IF version=?");
        assertThat(result.clauseBindings.getUsingValues()).containsExactly(7200, 9876543210L);
        assertThat(result.clauseBindings.getIfValues()).containsExactly(5);

        Object[] bindValues = strategy.getOrderedBindValues(write, result.clauseBindings);
        // USING values first, then SET, then WHERE, then IF
        assertThat(bindValues)
                .containsExactly(7200, 9876543210L, "Bob", 25, "active", "pk1", "ck1", 5);
    }
}
