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

package org.apache.flink.connector.cassandra.source;

import org.apache.flink.connector.cassandra.CassandraTestEnvironment;
import org.apache.flink.connector.cassandra.source.enumerator.CassandraEnumeratorState;
import org.apache.flink.connector.cassandra.source.reader.CassandraSplitReader;
import org.apache.flink.connector.cassandra.source.split.CassandraSplit;
import org.apache.flink.connector.cassandra.source.split.SplitsGenerator;
import org.apache.flink.connector.cassandra.source.utils.QueryValidator;
import org.apache.flink.connector.testframe.environment.ClusterControllable;
import org.apache.flink.connector.testframe.environment.MiniClusterTestEnvironment;
import org.apache.flink.connector.testframe.environment.TestEnvironment;
import org.apache.flink.connector.testframe.external.source.DataStreamSourceExternalContext;
import org.apache.flink.connector.testframe.junit.annotations.TestContext;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.connector.testframe.junit.annotations.TestExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestSemantics;
import org.apache.flink.connector.testframe.testsuites.SourceTestSuiteBase;
import org.apache.flink.connector.testframe.utils.CollectIteratorAssertions;
import org.apache.flink.connectors.cassandra.utils.Pojo;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;

import static java.util.concurrent.CompletableFuture.runAsync;
import static org.apache.flink.connector.cassandra.CassandraTestEnvironment.KEYSPACE;
import static org.apache.flink.connector.cassandra.CassandraTestEnvironment.SPLITS_TABLE;
import static org.apache.flink.connector.cassandra.source.CassandraTestContext.CassandraTestContextFactory;
import static org.apache.flink.connector.cassandra.source.split.SplitsGenerator.CassandraPartitioner.MURMUR3PARTITIONER;
import static org.apache.flink.connector.cassandra.source.split.SplitsGenerator.CassandraPartitioner.RANDOMPARTITIONER;
import static org.apache.flink.connector.testframe.utils.ConnectorTestConstants.DEFAULT_COLLECT_DATA_TIMEOUT;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for the Cassandra source. */
class CassandraSourceITCase extends SourceTestSuiteBase<Pojo> {

    private static final long EXPECTED_TABLE_SIZE = 51200L;
    @TestEnv MiniClusterTestEnvironment flinkTestEnvironment = new MiniClusterTestEnvironment();

    @TestExternalSystem
    CassandraTestEnvironment cassandraTestEnvironment = new CassandraTestEnvironment(true);

    @TestSemantics
    CheckpointingMode[] semantics = new CheckpointingMode[] {CheckpointingMode.EXACTLY_ONCE};

    @TestContext
    CassandraTestContextFactory contextFactory =
            new CassandraTestContextFactory(cassandraTestEnvironment);

    @TestTemplate
    @DisplayName("Test basic splitting with MURMUR3PARTITIONER (default Cassandra partitioner)")
    public void testGenerateSplitsMurMur3Partitioner(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<Pojo> externalContext,
            CheckpointingMode semantic) {
        final int parallelism = 2;
        SplitsGenerator generator =
                new SplitsGenerator(
                        MURMUR3PARTITIONER,
                        cassandraTestEnvironment.getSession(),
                        KEYSPACE,
                        SPLITS_TABLE,
                        parallelism,
                        CassandraSource.MAX_SPLIT_MEMORY_SIZE_DEFAULT);
        final CassandraEnumeratorState state = generator.prepareSplits();

        // no maxSplitMemorySize specified falling back number of splits = parallelism
        assertThat(state.getNumSplitsLeftToGenerate()).isEqualTo(parallelism);

        final CassandraSplit split1 = state.getNextSplit();
        checkNotNull(split1, "No splits left to generate in CassandraEnumeratorState");
        assertThat(split1.splitId()).isEqualTo("(-9223372036854775808,0)");

        final CassandraSplit split2 = state.getNextSplit();
        checkNotNull(split2, "No splits left to generate in CassandraEnumeratorState");
        assertThat(split2.splitId()).isEqualTo("(0,9223372036854775807)");
    }

    @TestTemplate
    @DisplayName("Test basic splitting with RANDOMPARTITIONER")
    public void testGenerateSplitsRandomPartitioner(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<Pojo> externalContext,
            CheckpointingMode semantic) {
        final int parallelism = 2;
        final SplitsGenerator generator =
                new SplitsGenerator(
                        RANDOMPARTITIONER,
                        cassandraTestEnvironment.getSession(),
                        KEYSPACE,
                        SPLITS_TABLE,
                        parallelism,
                        CassandraSource.MAX_SPLIT_MEMORY_SIZE_DEFAULT);
        final CassandraEnumeratorState state = generator.prepareSplits();

        // no maxSplitMemorySize specified falling back number of splits = parallelism
        assertThat(state.getNumSplitsLeftToGenerate()).isEqualTo(parallelism);

        final CassandraSplit split1 = state.getNextSplit();
        checkNotNull(split1, "No splits left to generate in CassandraEnumeratorState");
        assertThat(split1.splitId()).isEqualTo("(0,85070591730234615865843651857942052864)");

        final CassandraSplit split2 = state.getNextSplit();
        checkNotNull(split2, "No splits left to generate in CassandraEnumeratorState");
        assertThat(split2.splitId())
                .isEqualTo(
                        "(85070591730234615865843651857942052864,170141183460469231731687303715884105727)");
    }

    @TestTemplate
    @DisplayName("Test splitting with a correct split size set")
    public void testGenerateSplitsWithCorrectSize(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<Pojo> externalContext,
            CheckpointingMode semantic)
            throws Exception {
        final int parallelism = 2;
        final long maxSplitMemorySize = 10000L;
        final SplitsGenerator generator =
                new SplitsGenerator(
                        MURMUR3PARTITIONER,
                        cassandraTestEnvironment.getSession(),
                        KEYSPACE,
                        SPLITS_TABLE,
                        parallelism,
                        maxSplitMemorySize);
        final long tableSize = generator.estimateTableSize();
        // sanity check to ensure that the size estimates were updated in the Cassandra cluster
        assertThat(tableSize).isEqualTo(EXPECTED_TABLE_SIZE);
        final CassandraEnumeratorState cassandraEnumeratorState = generator.prepareSplits();
        assertThat(cassandraEnumeratorState.getNumSplitsLeftToGenerate())
                // regular case
                .isEqualTo(tableSize / maxSplitMemorySize);
    }

    @TestTemplate
    @DisplayName("Test splitting with a too big split size set")
    public void testGenerateSplitsWithTooHighMaximumSplitSize(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<Pojo> externalContext,
            CheckpointingMode semantic)
            throws Exception {
        final int parallelism = 20;
        final SplitsGenerator generator =
                new SplitsGenerator(
                        MURMUR3PARTITIONER,
                        cassandraTestEnvironment.getSession(),
                        KEYSPACE,
                        SPLITS_TABLE,
                        parallelism,
                        100_000_000L);
        // sanity check to ensure that the size estimates were updated in the Cassandra cluster
        assertThat(generator.estimateTableSize()).isEqualTo(EXPECTED_TABLE_SIZE);
        final CassandraEnumeratorState cassandraEnumeratorState = generator.prepareSplits();
        // maxSplitMemorySize is too high compared to table size. Falling back to parallelism splits
        // too low maxSplitMemorySize is guarded by an assertion > min at source creation
        assertThat(cassandraEnumeratorState.getNumSplitsLeftToGenerate()).isEqualTo(parallelism);
    }

    @Test
    public void testKeySpaceTableExtractionRegexp() {
        Arrays.asList(
                        "select field FROM keyspace.table where field = value;",
                        "select * FROM keyspace.table;",
                        "select field1, field2 from keyspace.table;",
                        "select field1, field2 from keyspace.table LIMIT(1000);",
                        "select field1 from keyspace.table ;",
                        "select field1 from keyspace.table where field1=1;")
                .forEach(this::assertQueryFormatCorrect);

        Arrays.asList(
                        "select field1 from table;", // missing keyspace
                        "select field1 from .table", // undefined keyspace var in a script
                        "select field1 from keyspace.;", // undefined table var in a script
                        "select field1 from keyspace.table" // missing ";"
                        )
                .forEach(this::assertQueryFormatIncorrect);
    }

    @Test
    public void testProhibitedClauses() {
        Arrays.asList(
                        "SELECT COUNT(*) from keyspace.table;",
                        "SELECT AVG(*) from keyspace.table;",
                        "SELECT MIN(*) from keyspace.table;",
                        "SELECT MAX(*) from keyspace.table;",
                        "SELECT SUM(*) from keyspace.table;",
                        "SELECT field1, field2 from keyspace.table ORDER BY field1;",
                        "SELECT field1, field2 from keyspace.table GROUP BY field1;")
                .forEach(this::assertProhibitedClauseRejected);
        Arrays.asList(
                        "select * from keyspace.table where field1_with_minimum_word=1;",
                        "select field1_with_minimum_word from keyspace.table;",
                        "select * from keyspace.table_with_minimum_word;")
                .forEach(this::assertQueryFormatCorrect);
    }

    @Test
    public void testGenerateRangeQuery() {
        String query;
        String outputQuery;

        // query with where clause
        query = "SELECT field FROM keyspace.table WHERE field = value;";
        outputQuery = CassandraSplitReader.generateRangeQuery(query, "field");
        assertThat(outputQuery)
                .isEqualTo(
                        "SELECT field FROM keyspace.table WHERE (token(field) >= ?) AND (token(field) < ?) AND field = value;");

        // query without where clause
        query = "SELECT * FROM keyspace.table;";
        outputQuery = CassandraSplitReader.generateRangeQuery(query, "field");
        assertThat(outputQuery)
                .isEqualTo(
                        "SELECT * FROM keyspace.table WHERE (token(field) >= ?) AND (token(field) < ?);");

        // query without where clause but with another trailing clause
        query = "SELECT field FROM keyspace.table LIMIT(1000);";
        outputQuery = CassandraSplitReader.generateRangeQuery(query, "field");
        assertThat(outputQuery)
                .isEqualTo(
                        "SELECT field FROM keyspace.table WHERE (token(field) >= ?) AND (token(field) < ?) LIMIT(1000);");

        // query with where clause and another trailing clause
        query = "SELECT field FROM keyspace.table WHERE field = value LIMIT(1000);";
        outputQuery = CassandraSplitReader.generateRangeQuery(query, "field");
        assertThat(outputQuery)
                .isEqualTo(
                        "SELECT field FROM keyspace.table WHERE (token(field) >= ?) AND (token(field) < ?) AND field = value LIMIT(1000);");
    }

    @Test
    public void testExtractFilteringColumns() {
        final QueryValidator queryValidator = cassandraTestEnvironment.getQueryValidator();
        final String query1 = "SELECT * FROM keyspace.table WHERE field = value;";
        assertThat(queryValidator.extractFilteringColumns(query1))
                .containsAll(Collections.singletonList("field"));
        final String query2 =
                "SELECT * FROM keyspace.table WHERE field1 = value AND field2 = value;";
        assertThat(queryValidator.extractFilteringColumns(query2))
                .containsAll(Arrays.asList("field1", "field2"));
        final String query3 = "SELECT * FROM keyspace.table;";
        assertThat(queryValidator.extractFilteringColumns(query3)).isEmpty();
    }

    @Test
    public void testFilterOnPartitionKey() {
        final QueryValidator queryValidator = cassandraTestEnvironment.getQueryValidator();
        // filter on all the columns of the partition key
        final String goodQuery =
                String.format(
                        "SELECT id FROM %s.%s WHERE col1 = %d AND col2 =%d;",
                        KEYSPACE, SPLITS_TABLE, 1, 1);
        assertThat(
                        queryValidator.filtersOnPartitionKey(
                                queryValidator.extractFilteringColumns(goodQuery),
                                KEYSPACE,
                                SPLITS_TABLE))
                .isTrue();

        // filter on only one of the columns of the partition key
        final String badQuery =
                String.format("SELECT id FROM %s.%s WHERE col1 = %d;", KEYSPACE, SPLITS_TABLE, 1);
        assertThat(
                        queryValidator.filtersOnPartitionKey(
                                queryValidator.extractFilteringColumns(badQuery),
                                KEYSPACE,
                                SPLITS_TABLE))
                .isFalse();
    }

    @Test
    public void testIsPrimaryKeyColumn() {
        final QueryValidator queryValidator = cassandraTestEnvironment.getQueryValidator();
        assertThat(queryValidator.isPrimaryKeyColumn("col1", KEYSPACE, SPLITS_TABLE)).isTrue();
        assertThat(queryValidator.isPrimaryKeyColumn("col2", KEYSPACE, SPLITS_TABLE)).isTrue();
        assertThat(queryValidator.isPrimaryKeyColumn("col3", KEYSPACE, SPLITS_TABLE)).isTrue();
        assertThat(queryValidator.isPrimaryKeyColumn("col4", KEYSPACE, SPLITS_TABLE)).isFalse();
    }

    @Test
    public void testIsIndexedColumn() {
        // The was no index set on the table
        final QueryValidator queryValidator = cassandraTestEnvironment.getQueryValidator();
        assertThat(queryValidator.isIndexedColumn("col1", KEYSPACE, SPLITS_TABLE)).isFalse();
        assertThat(queryValidator.isIndexedColumn("col2", KEYSPACE, SPLITS_TABLE)).isFalse();
        assertThat(queryValidator.isIndexedColumn("col3", KEYSPACE, SPLITS_TABLE)).isFalse();
        assertThat(queryValidator.isIndexedColumn("col4", KEYSPACE, SPLITS_TABLE)).isTrue();
    }

    private void assertQueryFormatIncorrect(String query) {
        assertThatThrownBy(
                        () ->
                                cassandraTestEnvironment
                                        .getQueryValidator()
                                        .checkQueryValidity(query))
                .hasMessageContaining(
                        "Query must be of the form select ... from keyspace.table ...;");
    }

    private void assertQueryFormatCorrect(String query) {
        Matcher matcher = CassandraSource.SELECT_REGEXP.matcher(query);
        assertThat(matcher.matches()).isTrue();
        assertThat(matcher.group(1)).matches(".*"); // keyspace
        assertThat(matcher.group(2)).matches(".*"); // table
    }

    private void assertProhibitedClauseRejected(String query) {
        assertThatThrownBy(
                        () ->
                                cassandraTestEnvironment
                                        .getQueryValidator()
                                        .checkQueryValidity(query))
                .hasMessageContaining(
                        "Aggregations/OrderBy are not supported because the query is executed on subsets/partitions of the input table");
    }

    // overridden to use unordered checks
    @Override
    protected void checkResultWithSemantic(
            CloseableIterator<Pojo> resultIterator,
            List<List<Pojo>> testData,
            CheckpointingMode semantic,
            Integer limit) {
        if (limit != null) {
            Runnable runnable =
                    () ->
                            CollectIteratorAssertions.assertUnordered(resultIterator)
                                    .withNumRecordsLimit(limit)
                                    .matchesRecordsFromSource(testData, semantic);

            assertThat(runAsync(runnable)).succeedsWithin(DEFAULT_COLLECT_DATA_TIMEOUT);
        } else {
            CollectIteratorAssertions.assertUnordered(resultIterator)
                    .matchesRecordsFromSource(testData, semantic);
        }
    }

    @Disabled("Not a unbounded source")
    @Override
    public void testSourceMetrics(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<Pojo> externalContext,
            CheckpointingMode semantic)
            throws Exception {}

    @Disabled("Not a unbounded source")
    @Override
    public void testSavepoint(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<Pojo> externalContext,
            CheckpointingMode semantic) {}

    @Disabled("Not a unbounded source")
    @Override
    public void testScaleUp(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<Pojo> externalContext,
            CheckpointingMode semantic) {}

    @Disabled("Not a unbounded source")
    @Override
    public void testScaleDown(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<Pojo> externalContext,
            CheckpointingMode semantic) {}

    @Disabled("Not a unbounded source")
    @Override
    public void testTaskManagerFailure(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<Pojo> externalContext,
            ClusterControllable controller,
            CheckpointingMode semantic) {}
}
