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

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.mapping.Mapper;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link CassandraSourceBuilder}. */
class CassandraSourceBuilderTest {

    private static final ClusterBuilder TEST_CLUSTER_BUILDER =
            new ClusterBuilder() {
                @Override
                protected Cluster buildCluster(Cluster.Builder builder) {
                    return builder.addContactPointsWithPorts(
                                    new InetSocketAddress("localhost", 9042))
                            .build();
                }
            };

    private static final String TEST_QUERY = "SELECT id, name FROM test_keyspace.test_table;";

    @Test
    void testPojoSourceBuilder() {
        // Test POJO source builder
        CassandraSource<TestPojo> source =
                CassandraSource.builder()
                        .setClusterBuilder(TEST_CLUSTER_BUILDER)
                        .setQuery(TEST_QUERY)
                        .setMapperOptions(
                                () -> new Mapper.Option[] {Mapper.Option.saveNullFields(true)})
                        .setMaxSplitMemorySize(MemorySize.ofMebiBytes(32))
                        .forPojo(TestPojo.class);

        assertThat(source).isNotNull();
        assertThat(source.getBoundedness())
                .isEqualTo(org.apache.flink.api.connector.source.Boundedness.BOUNDED);
    }

    @Test
    void testRowDataSourceBuilder() {
        // Test RowData source builder
        RowType rowType =
                RowType.of(
                        new org.apache.flink.table.types.logical.LogicalType[] {
                            new IntType(), VarCharType.STRING_TYPE
                        },
                        new String[] {"id", "name"});

        CassandraSource<RowData> source =
                CassandraSource.builder()
                        .setClusterBuilder(TEST_CLUSTER_BUILDER)
                        .setQuery(TEST_QUERY)
                        .forRowData(rowType);

        assertThat(source).isNotNull();
        assertThat(source.getBoundedness())
                .isEqualTo(org.apache.flink.api.connector.source.Boundedness.BOUNDED);
    }

    @Test
    void testBuilderValidation() {
        // Test missing cluster builder
        assertThatThrownBy(
                        () ->
                                CassandraSource.builder()
                                        .setQuery(TEST_QUERY)
                                        .forPojo(TestPojo.class))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("ClusterBuilder is required");

        // Test missing query
        assertThatThrownBy(
                        () ->
                                CassandraSource.builder()
                                        .setClusterBuilder(TEST_CLUSTER_BUILDER)
                                        .forPojo(TestPojo.class))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Query is required");

        // Test conflicting parameters (both POJO and RowData)
        RowType rowType =
                RowType.of(
                        new org.apache.flink.table.types.logical.LogicalType[] {new IntType()},
                        new String[] {"id"});

        // This test is no longer relevant since the API prevents this scenario
        // by having separate build methods

        // Test invalid memory size for POJO
        assertThatThrownBy(
                        () ->
                                CassandraSource.builder()
                                        .setClusterBuilder(TEST_CLUSTER_BUILDER)
                                        .setQuery(TEST_QUERY)
                                        .setMaxSplitMemorySize(MemorySize.parse("5b"))
                                        .forPojo(TestPojo.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be at least");

        // Test invalid memory size for RowData
        assertThatThrownBy(
                        () ->
                                CassandraSource.builder()
                                        .setClusterBuilder(TEST_CLUSTER_BUILDER)
                                        .setQuery(TEST_QUERY)
                                        .setMaxSplitMemorySize(MemorySize.parse("5b"))
                                        .forRowData(rowType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be at least");
    }

    @Test
    void testBuilderDefaults() {
        // Test that default mapper options work
        CassandraSource<TestPojo> source =
                CassandraSource.builder()
                        .setClusterBuilder(TEST_CLUSTER_BUILDER)
                        .setQuery(TEST_QUERY)
                        .forPojo(TestPojo.class);

        assertThat(source).isNotNull();
    }

    @Test
    void testBuilderWithValidMemorySize() {
        // Test valid memory sizes
        CassandraSource<TestPojo> source1 =
                CassandraSource.builder()
                        .setClusterBuilder(TEST_CLUSTER_BUILDER)
                        .setQuery(TEST_QUERY)
                        .setMaxSplitMemorySize(MemorySize.ofMebiBytes(64)) // Default
                        .forPojo(TestPojo.class);
        assertThat(source1).isNotNull();

        CassandraSource<TestPojo> source2 =
                CassandraSource.builder()
                        .setClusterBuilder(TEST_CLUSTER_BUILDER)
                        .setQuery(TEST_QUERY)
                        .setMaxSplitMemorySize(MemorySize.ofMebiBytes(128))
                        .forPojo(TestPojo.class);
        assertThat(source2).isNotNull();
    }

    @Test
    void testBuilderWithNullClusterBuilder() {
        assertThatThrownBy(
                        () ->
                                CassandraSource.builder()
                                        .setClusterBuilder(null)
                                        .setQuery(TEST_QUERY)
                                        .forPojo(TestPojo.class))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("ClusterBuilder is required");
    }

    @Test
    void testBuilderWithNullQuery() {
        assertThatThrownBy(
                        () ->
                                CassandraSource.builder()
                                        .setClusterBuilder(TEST_CLUSTER_BUILDER)
                                        .setQuery(null)
                                        .forPojo(TestPojo.class))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Query is required");
    }

    @Test
    void testBuilderWithEmptyQuery() {
        assertThatThrownBy(
                        () ->
                                CassandraSource.builder()
                                        .setClusterBuilder(TEST_CLUSTER_BUILDER)
                                        .setQuery("")
                                        .forPojo(TestPojo.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Query must be of the form select");
    }

    @Test
    void testBuilderWithNullMapperOptions() {
        // Null mapper options should be allowed and use defaults
        CassandraSource<TestPojo> source =
                CassandraSource.builder()
                        .setClusterBuilder(TEST_CLUSTER_BUILDER)
                        .setQuery(TEST_QUERY)
                        .setMapperOptions(null)
                        .forPojo(TestPojo.class);
        assertThat(source).isNotNull();
    }

    /** Test POJO class for testing. */
    public static class TestPojo {
        private int id;
        private String name;

        public TestPojo() {}

        public TestPojo(int id, String name) {
            this.id = id;
            this.name = name;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
