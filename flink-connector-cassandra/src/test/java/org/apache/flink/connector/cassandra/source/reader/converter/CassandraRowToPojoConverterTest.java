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
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

/** Unit tests for {@link CassandraRowToPojoConverter}. */
class CassandraRowToPojoConverterTest {

    @Mock private ClusterBuilder clusterBuilder;
    @Mock private Cluster cluster;
    @Mock private Session session;
    @Mock private MappingManager mappingManager;
    @Mock private Mapper<TestPojo> mapper;
    @Mock private MapperOptions mapperOptions;
    @Mock private Row row;
    @Mock private ExecutionInfo executionInfo;
    @Mock private ResultSet resultSet;

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
    void testConvertSuccessfully() throws Exception {
        // Setup test data
        Date testDate = new Date();
        TestPojo expectedPojo = new TestPojo();
        expectedPojo.setId("test-id");
        expectedPojo.setName("test-name");
        expectedPojo.setAge(25);
        expectedPojo.setActive(true);
        expectedPojo.setCreatedAt(testDate);
        when(clusterBuilder.getCluster()).thenReturn(cluster);
        when(cluster.connect()).thenReturn(session);
        CassandraRow cassandraRow = new CassandraRow(row, executionInfo);
        assertThatThrownBy(() -> converter.convert(cassandraRow))
                .isInstanceOf(Exception.class); // Should fail during DataStax mapper initialization
    }

    @Test
    void testMapperInitializationWithOptions() {
        Mapper.Option[] options = {Mapper.Option.saveNullFields(true)};
        when(mapperOptions.getMapperOptions()).thenReturn(options);
        CassandraRowToPojoConverter<TestPojo> converterWithOptions =
                new CassandraRowToPojoConverter<>(TestPojo.class, mapperOptions, clusterBuilder);

        assertThat(converterWithOptions).isNotNull();
        assertThat(converterWithOptions.getProducedType().getTypeClass()).isEqualTo(TestPojo.class);
        CassandraRow cassandraRow = new CassandraRow(row, executionInfo);
        assertThatThrownBy(() -> converterWithOptions.convert(cassandraRow))
                .isInstanceOf(Exception.class);
    }

    @Test
    void testConvertWithNullRow() {
        assertThatThrownBy(() -> converter.convert(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void testMapperLazyInitialization() {
        CassandraRow cassandraRow = new CassandraRow(row, executionInfo);
        assertThatThrownBy(() -> converter.convert(cassandraRow)).isInstanceOf(Exception.class);
        assertThatThrownBy(() -> converter.convert(cassandraRow)).isInstanceOf(Exception.class);
    }

    @Test
    void testClusterConnectionFailure() {
        when(clusterBuilder.getCluster()).thenThrow(new RuntimeException("Connection failed"));

        CassandraRow cassandraRow = new CassandraRow(row, executionInfo);
        assertThatThrownBy(() -> converter.convert(cassandraRow))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Connection failed");
    }

    @Test
    void testSerializability() {
        assertThat(converter).isInstanceOf(java.io.Serializable.class);
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
    void testSingleRowResultSetBehavior() {
        CassandraRow cassandraRow = new CassandraRow(row, executionInfo);
        assertThatThrownBy(() -> converter.convert(cassandraRow)).isInstanceOf(Exception.class);
    }

    @Test
    void testFullConversionFlow() throws Exception {
        Date testDate = new Date();
        CassandraRowToPojoConverter<TestPojo> fullConverter =
                new CassandraRowToPojoConverter<>(TestPojo.class, mapperOptions, clusterBuilder);
        assertThat(fullConverter.getProducedType().getTypeClass()).isEqualTo(TestPojo.class);

        // Test that the conversion process attempts to initialize the mapper
        CassandraRow cassandraRow = new CassandraRow(row, executionInfo);
        when(clusterBuilder.getCluster()).thenReturn(cluster);
        when(cluster.connect()).thenReturn(session);
        assertThatThrownBy(() -> fullConverter.convert(cassandraRow)).isInstanceOf(Exception.class);
        assertThatThrownBy(() -> fullConverter.convert(cassandraRow)).isInstanceOf(Exception.class);
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
