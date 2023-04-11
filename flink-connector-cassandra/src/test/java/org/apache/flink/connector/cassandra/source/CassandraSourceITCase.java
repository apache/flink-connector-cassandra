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
import org.apache.flink.connector.cassandra.source.split.CassandraSplit;
import org.apache.flink.connector.cassandra.source.split.SplitsGenerator;
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
import org.junit.jupiter.api.TestTemplate;

import java.util.List;

import static java.util.concurrent.CompletableFuture.runAsync;
import static org.apache.flink.connector.cassandra.source.CassandraTestContext.CassandraTestContextFactory;
import static org.apache.flink.connector.cassandra.source.split.SplitsGenerator.CassandraPartitioner.MURMUR3PARTITIONER;
import static org.apache.flink.connector.cassandra.source.split.SplitsGenerator.CassandraPartitioner.RANDOMPARTITIONER;
import static org.apache.flink.connector.testframe.utils.ConnectorTestConstants.DEFAULT_COLLECT_DATA_TIMEOUT;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for the Cassandra source. */
class CassandraSourceITCase extends SourceTestSuiteBase<Pojo> {

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
                        CassandraTestEnvironment.KEYSPACE,
                        CassandraTestEnvironment.SPLITS_TABLE,
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
                        CassandraTestEnvironment.KEYSPACE,
                        CassandraTestEnvironment.SPLITS_TABLE,
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
                        CassandraTestEnvironment.KEYSPACE,
                        CassandraTestEnvironment.SPLITS_TABLE,
                        parallelism,
                        maxSplitMemorySize);
        final long tableSize = generator.estimateTableSize();
        // sanity check to ensure that the size estimates were updated in the Cassandra cluster
        assertThat(tableSize).isEqualTo(35840L);
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
                        CassandraTestEnvironment.KEYSPACE,
                        CassandraTestEnvironment.SPLITS_TABLE,
                        parallelism,
                        100_000_000L);
        // sanity check to ensure that the size estimates were updated in the Cassandra cluster
        assertThat(generator.estimateTableSize()).isEqualTo(35840L);
        final CassandraEnumeratorState cassandraEnumeratorState = generator.prepareSplits();
        // maxSplitMemorySize is too high compared to table size. Falling back to parallelism splits
        // too low maxSplitMemorySize is guarded by an assertion > min at source creation
        assertThat(cassandraEnumeratorState.getNumSplitsLeftToGenerate()).isEqualTo(parallelism);
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
