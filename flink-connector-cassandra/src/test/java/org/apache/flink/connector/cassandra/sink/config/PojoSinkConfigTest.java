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

package org.apache.flink.connector.cassandra.sink.config;

import org.apache.flink.streaming.connectors.cassandra.SimpleMapperOptions;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link PojoSinkConfig}. */
class PojoSinkConfigTest {

    @Table(keyspace = "test_ks", name = "test_table")
    static class TestPojo {
        @PartitionKey
        @Column(name = "id")
        private String id;

        @Column(name = "name")
        private String name;

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
    }

    @Test
    void testPojoConfigurationWithoutMapperOptions() {
        PojoSinkConfig<TestPojo> config =
                new PojoSinkConfig<>(TestPojo.class, "test_keyspace", null);

        assertThat(config.getPojoClass()).isEqualTo(TestPojo.class);
        assertThat(config.getKeyspace()).isEqualTo("test_keyspace");
        assertThat(config.getMapperOptions()).isNull();
        assertThat(config.getRecordFormatType()).isEqualTo(RecordFormatType.POJO);
    }

    @Test
    void testPojoConfigurationWithMapperOptions() {
        SimpleMapperOptions options = new SimpleMapperOptions().ttl(3600);

        PojoSinkConfig<TestPojo> config = new PojoSinkConfig<>(TestPojo.class, "test_ks", options);

        assertThat(config.getPojoClass()).isEqualTo(TestPojo.class);
        assertThat(config.getKeyspace()).isEqualTo("test_ks");
        assertThat(config.getMapperOptions()).isEqualTo(options);
        assertThat(config.getRecordFormatType()).isEqualTo(RecordFormatType.POJO);
    }

    @Test
    void testConstructorValidation() {
        // Null POJO class
        assertThatThrownBy(() -> new PojoSinkConfig<TestPojo>(null, "test_ks", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pojoClass must not be null");

        // Null keyspace
        assertThatThrownBy(() -> new PojoSinkConfig<>(TestPojo.class, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Keyspace cannot be empty");

        // Empty keyspace
        assertThatThrownBy(() -> new PojoSinkConfig<>(TestPojo.class, "", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Keyspace cannot be empty");
    }
}
