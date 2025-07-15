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

package org.apache.flink.connector.cassandra.source.reader.converter;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.cassandra.source.reader.CassandraRow;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.streaming.connectors.cassandra.MapperOptions;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Date;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

/** Unit tests for {@link CassandraRowToPojoConverter}. */
class CassandraRowToPojoConverterTest {

    @Mock private ClusterBuilder clusterBuilder;
    @Mock private Cluster cluster;
    @Mock private Session session;
    @Mock private MapperOptions mapperOptions;
    @Mock private Row mockRow;
    @Mock private ExecutionInfo mockExecutionInfo;
    @Mock private CassandraRow mockCassandraRow;
    @Mock private ColumnDefinitions mockColumnDefinitions;

    private CassandraRowToPojoConverter<TestPojo> converter;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(clusterBuilder.getCluster()).thenReturn(cluster);
        when(cluster.connect()).thenReturn(session);
        when(mapperOptions.getMapperOptions()).thenReturn(new Mapper.Option[0]);
        converter =
                new CassandraRowToPojoConverter<>(TestPojo.class, mapperOptions, clusterBuilder);
    }

    @Test
    void testGetProducedType() {
        TypeInformation<TestPojo> typeInfo = converter.getProducedType();
        assertThat(typeInfo).isNotNull();
        assertThat(typeInfo.getTypeClass()).isEqualTo(TestPojo.class);
    }

    @Test
    void testConvertWithNullRow() {
        assertThatThrownBy(() -> converter.convert(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void testConstructorValidation() {
        assertThatThrownBy(
                        () ->
                                new CassandraRowToPojoConverter<>(
                                        null, mapperOptions, clusterBuilder))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("POJO class cannot be null");

        assertThatThrownBy(
                        () ->
                                new CassandraRowToPojoConverter<>(
                                        TestPojo.class, null, clusterBuilder))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Mapper options cannot be null");

        assertThatThrownBy(
                        () ->
                                new CassandraRowToPojoConverter<>(
                                        TestPojo.class, mapperOptions, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Cluster builder cannot be null");
    }

    @Test
    void testSingleRowResultSetMechanics() throws Exception {
        // Setup mocks
        when(mockCassandraRow.getRow()).thenReturn(mockRow);
        when(mockCassandraRow.getExecutionInfo()).thenReturn(mockExecutionInfo);
        when(mockRow.getColumnDefinitions()).thenReturn(mockColumnDefinitions);

        // Create the converter and use reflection to access SingleRowResultSet
        Class<?> singleRowResultSetClass =
                Class.forName(
                        "org.apache.flink.connector.cassandra.source.reader.converter.CassandraRowToPojoConverter$SingleRowResultSet");

        // Create an instance of SingleRowResultSet
        Object singleRowResultSet =
                singleRowResultSetClass
                        .getDeclaredConstructor(Row.class, ExecutionInfo.class)
                        .newInstance(mockRow, mockExecutionInfo);

        ResultSet resultSet = (ResultSet) singleRowResultSet;

        // Test one() method
        assertThat(resultSet.one()).isEqualTo(mockRow);

        // Test all() method
        assertThat(resultSet.all()).hasSize(1);
        assertThat(resultSet.all().get(0)).isEqualTo(mockRow);

        // Test iterator mechanics
        Iterator<Row> iterator = resultSet.iterator();
        assertThat(iterator).isNotNull();
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next()).isEqualTo(mockRow);

        // Test wasApplied() method
        assertThat(resultSet.wasApplied()).isTrue();

        // Test other ResultSet methods
        assertThat(resultSet.isExhausted()).isTrue();
        assertThat(resultSet.isFullyFetched()).isTrue();
        assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(1);
        assertThat(resultSet.getExecutionInfo()).isEqualTo(mockExecutionInfo);
        assertThat(resultSet.getAllExecutionInfo()).hasSize(1);
        assertThat(resultSet.getAllExecutionInfo().get(0)).isEqualTo(mockExecutionInfo);
        assertThat(resultSet.getColumnDefinitions()).isEqualTo(mockColumnDefinitions);

        // Test fetchMoreResults returns null future
        assertThat(resultSet.fetchMoreResults()).isNotNull();
        assertThat(resultSet.fetchMoreResults().get()).isNull();
    }

    @Test
    void testMapperOptionsConfiguration() {
        // Test that mapper options are properly stored
        Mapper.Option[] options = new Mapper.Option[2];
        when(mapperOptions.getMapperOptions()).thenReturn(options);

        CassandraRowToPojoConverter<TestPojo> converterWithOptions =
                new CassandraRowToPojoConverter<>(TestPojo.class, mapperOptions, clusterBuilder);

        // Verify the converter was created successfully with mapper options
        assertThat(converterWithOptions).isNotNull();

        // When mapper options return null, converter should still work
        when(mapperOptions.getMapperOptions()).thenReturn(null);
        CassandraRowToPojoConverter<TestPojo> converterWithNullOptions =
                new CassandraRowToPojoConverter<>(TestPojo.class, mapperOptions, clusterBuilder);
        assertThat(converterWithNullOptions).isNotNull();
    }

    /** Test POJO class with Cassandra annotations. */
    @Table(keyspace = "test_keyspace", name = "test_table")
    public static class TestPojo {
        @PartitionKey
        @Column(name = "id")
        private String id;

        @Column(name = "name")
        private String name;

        @Column(name = "age")
        private int age;

        @Column(name = "active")
        private boolean active;

        @Column(name = "created_at")
        private Date createdAt;

        // Getters and setters
        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public boolean isActive() {
            return active;
        }

        public void setActive(boolean active) {
            this.active = active;
        }

        public Date getCreatedAt() {
            return createdAt;
        }

        public void setCreatedAt(Date createdAt) {
            this.createdAt = createdAt;
        }
    }
}
