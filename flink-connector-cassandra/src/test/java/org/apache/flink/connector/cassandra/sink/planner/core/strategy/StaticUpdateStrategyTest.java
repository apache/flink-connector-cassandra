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

/** Unit tests for {@link StaticUpdateStrategy}. */
class StaticUpdateStrategyTest {

    @Mock private CqlSinkConfig<Row> config;
    @Mock private CqlClauseResolver<Row> clauseResolver;

    private StaticUpdateStrategy<Row> strategy;
    private TableRef table;
    private Row record;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        strategy = new StaticUpdateStrategy<>();
        table = new TableRef("test_keyspace", "test_table");
        record = Row.of("Bob", 25, 1);
    }

    @Test
    void testStaticUpdateQuery() {
        String staticQuery = "UPDATE test_keyspace.test_table SET name=?, age=? WHERE id=?";
        when(config.getQuery()).thenReturn(staticQuery);

        ResolvedWrite write =
                ResolvedWrite.update(
                        Arrays.asList("name", "age"),
                        new Object[] {"Bob", 25},
                        Collections.singletonList("id"),
                        new Object[] {1});

        PlannerStrategy.QueryWithBindings result =
                strategy.getQueryWithBindings(table, config, clauseResolver, write, record);

        assertThat(result.query).isEqualTo(staticQuery);
        assertThat(result.clauseBindings).isNotNull();
        assertThat(result.clauseBindings.getUsingValues()).isEmpty();
        assertThat(result.clauseBindings.getIfValues()).isEmpty();

        Object[] bindValues = strategy.getOrderedBindValues(write, null);
        assertThat(bindValues).containsExactly("Bob", 25, 1);
    }

    @Test
    void testStaticUpdateWithTTL() {
        String staticQuery =
                "UPDATE test_keyspace.test_table USING TTL 3600 SET status=? WHERE id=?";
        when(config.getQuery()).thenReturn(staticQuery);

        ResolvedWrite write =
                ResolvedWrite.update(
                        Collections.singletonList("status"),
                        new Object[] {"active"},
                        Collections.singletonList("id"),
                        new Object[] {1});

        PlannerStrategy.QueryWithBindings result =
                strategy.getQueryWithBindings(table, config, clauseResolver, write, record);

        assertThat(result.query).isEqualTo(staticQuery);
        assertThat(result.clauseBindings).isNotNull();
        assertThat(result.clauseBindings.getUsingValues()).isEmpty();
        assertThat(result.clauseBindings.getIfValues()).isEmpty();

        Object[] bindValues = strategy.getOrderedBindValues(write, null);
        assertThat(bindValues).containsExactly("active", 1);
    }

    @Test
    void testStaticUpdateWithIfCondition() {
        String staticQuery = "UPDATE test_keyspace.test_table SET status=? WHERE id=? IF status=?";
        when(config.getQuery()).thenReturn(staticQuery);

        ResolvedWrite write =
                ResolvedWrite.update(
                        Collections.singletonList("status"),
                        new Object[] {"completed"},
                        Collections.singletonList("id"),
                        new Object[] {456});

        PlannerStrategy.QueryWithBindings result =
                strategy.getQueryWithBindings(table, config, clauseResolver, write, record);

        assertThat(result.query).isEqualTo(staticQuery);
        assertThat(result.clauseBindings).isNotNull();
        assertThat(result.clauseBindings.getUsingValues()).isEmpty();
        assertThat(result.clauseBindings.getIfValues()).isEmpty();

        Object[] bindValues = strategy.getOrderedBindValues(write, null);
        assertThat(bindValues).containsExactly("completed", 456);
    }

    @Test
    void testStaticUpdateFailsForInsertOperation() {
        String staticQuery = "UPDATE test_keyspace.test_table SET name=? WHERE id=?";
        when(config.getQuery()).thenReturn(staticQuery);

        ResolvedWrite write =
                ResolvedWrite.insert(Arrays.asList("id", "name"), new Object[] {1, "Alice"});

        assertThatThrownBy(
                        () ->
                                strategy.getQueryWithBindings(
                                        table, config, clauseResolver, write, record))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("StaticUpdateStrategy got INSERT");
    }

    @Test
    void testStaticUpdateWithMultipleWhereColumns() {
        String staticQuery =
                "UPDATE test_keyspace.test_table SET status=? WHERE partition_key=? AND clustering_key=?";
        when(config.getQuery()).thenReturn(staticQuery);

        ResolvedWrite write =
                ResolvedWrite.update(
                        Collections.singletonList("status"),
                        new Object[] {"active"},
                        Arrays.asList("partition_key", "clustering_key"),
                        new Object[] {"partition1", "cluster1"});

        PlannerStrategy.QueryWithBindings result =
                strategy.getQueryWithBindings(table, config, clauseResolver, write, record);

        assertThat(result.query).isEqualTo(staticQuery);
        assertThat(result.clauseBindings).isNotNull();
        assertThat(result.clauseBindings.getUsingValues()).isEmpty();
        assertThat(result.clauseBindings.getIfValues()).isEmpty();

        Object[] bindValues = strategy.getOrderedBindValues(write, null);
        assertThat(bindValues).containsExactly("active", "partition1", "cluster1");
    }

    @Test
    void testStaticUpdateWithComplexQuery() {
        String staticQuery =
                "UPDATE test_keyspace.test_table USING TTL 7200 AND TIMESTAMP 1234567890 "
                        + "SET data=? WHERE id=? IF EXISTS";
        when(config.getQuery()).thenReturn(staticQuery);

        ResolvedWrite write =
                ResolvedWrite.update(
                        Collections.singletonList("data"),
                        new Object[] {"complex_data"},
                        Collections.singletonList("id"),
                        new Object[] {1});

        PlannerStrategy.QueryWithBindings result =
                strategy.getQueryWithBindings(table, config, clauseResolver, write, record);

        assertThat(result.query).isEqualTo(staticQuery);
        assertThat(result.clauseBindings).isNotNull();
        assertThat(result.clauseBindings.getUsingValues()).isEmpty();
        assertThat(result.clauseBindings.getIfValues()).isEmpty();

        Object[] bindValues = strategy.getOrderedBindValues(write, null);
        assertThat(bindValues).containsExactly("complex_data", 1);
    }
}
