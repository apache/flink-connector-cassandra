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

package org.apache.flink.connector.cassandra.sink.planner.resolver;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link TableRef}. */
class TableRefTest {

    @Test
    void testValidConstruction() {
        TableRef tableRef = new TableRef("my_keyspace", "my_table");

        assertThat(tableRef.keyspace()).isEqualTo("my_keyspace");
        assertThat(tableRef.tableName()).isEqualTo("my_table");
        assertThat(tableRef.getFullyQualifiedName()).isEqualTo("my_keyspace.my_table");
        assertThat(tableRef.toString()).isEqualTo("my_keyspace.my_table");
    }

    @Test
    void testInvalidParametersFail() {
        // Null keyspace
        assertThatThrownBy(() -> new TableRef(null, "my_table"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("keyspace cannot be null/empty");

        // Empty keyspace
        assertThatThrownBy(() -> new TableRef("", "my_table"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("keyspace cannot be null/empty");

        // Whitespace keyspace
        assertThatThrownBy(() -> new TableRef("   ", "my_table"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("keyspace cannot be null/empty");

        // Null table name
        assertThatThrownBy(() -> new TableRef("my_keyspace", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tableName cannot be null/empty");

        // Empty table name
        assertThatThrownBy(() -> new TableRef("my_keyspace", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tableName cannot be null/empty");

        // Whitespace table name
        assertThatThrownBy(() -> new TableRef("my_keyspace", "   "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tableName cannot be null/empty");
    }
}
