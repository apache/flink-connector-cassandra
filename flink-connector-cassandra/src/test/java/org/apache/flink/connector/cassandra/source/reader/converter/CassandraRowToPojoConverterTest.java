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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/** Unit tests for {@link CassandraRowToPojoConverter}. */
class CassandraRowToPojoConverterTest {

    @Mock private Row mockRow;
    @Mock private ColumnDefinitions mockColumnDefinitions;

    private CassandraRowToPojoConverter<TestPojo> converter;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new CassandraRowToPojoConverter<>(TestPojo.class);
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
        assertThatThrownBy(() -> new CassandraRowToPojoConverter<>(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("POJO class cannot be null");

        assertThatThrownBy(() -> new CassandraRowToPojoConverter<>(String.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be annotated with");
    }

    @Test
    void testConvert() {
        final Date createdAt = new Date(1700000000000L);
        when(mockRow.getColumnDefinitions()).thenReturn(mockColumnDefinitions);
        when(mockColumnDefinitions.contains(any(CqlIdentifier.class))).thenReturn(true);
        when(mockRow.isNull(any(CqlIdentifier.class))).thenReturn(false);
        when(mockRow.isNull(CqlIdentifier.fromCql("name"))).thenReturn(true);
        when(mockRow.get(CqlIdentifier.fromCql("id"), GenericType.STRING)).thenReturn("id1");
        when(mockRow.get(CqlIdentifier.fromCql("age"), GenericType.INTEGER)).thenReturn(42);
        when(mockRow.get(CqlIdentifier.fromCql("active"), GenericType.BOOLEAN)).thenReturn(true);
        when(mockRow.get(CqlIdentifier.fromCql("created_at"), GenericType.of(Date.class)))
                .thenReturn(createdAt);

        final TestPojo pojo = converter.convert(new CassandraRow(mockRow));

        assertThat(pojo.getId()).isEqualTo("id1");
        assertThat(pojo.getName()).isNull();
        assertThat(pojo.getAge()).isEqualTo(42);
        assertThat(pojo.isActive()).isTrue();
        assertThat(pojo.getCreatedAt()).isEqualTo(createdAt);
    }

    @Test
    void testConvertIgnoresMissingColumns() {
        when(mockRow.getColumnDefinitions()).thenReturn(mockColumnDefinitions);
        when(mockColumnDefinitions.contains(any(CqlIdentifier.class))).thenReturn(false);
        when(mockColumnDefinitions.contains(CqlIdentifier.fromCql("id"))).thenReturn(true);
        when(mockRow.get(CqlIdentifier.fromCql("id"), GenericType.STRING)).thenReturn("id1");

        final TestPojo pojo = converter.convert(new CassandraRow(mockRow));

        assertThat(pojo.getId()).isEqualTo("id1");
        assertThat(pojo.getName()).isNull();
        assertThat(pojo.getAge()).isZero();
        assertThat(pojo.getCreatedAt()).isNull();
    }

    /** Test POJO class with Cassandra annotations. */
    @Entity(defaultKeyspace = "test_keyspace")
    @CqlName("test_table")
    public static class TestPojo {
        @PartitionKey
        @CqlName("id")
        private String id;

        @CqlName("name")
        private String name;

        @CqlName("age")
        private int age;

        @CqlName("active")
        private boolean active;

        // mapped to created_at by the default naming strategy
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
