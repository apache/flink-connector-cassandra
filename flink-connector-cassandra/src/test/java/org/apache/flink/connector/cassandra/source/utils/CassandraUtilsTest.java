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

package org.apache.flink.connector.cassandra.source.utils;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link CassandraUtils}. */
class CassandraUtilsTest {

    private static final String KEYSPACE = "ks";
    private static final String TABLE = "tbl";

    @Test
    void testPartitionKeyKeepsTheOrderOfTheColumns() {
        // the names are not in the alphabetical order
        final Metadata metadata = createMetadata("col_b", "col_a", "col_c");

        assertThat(CassandraUtils.getPartitionKey(KEYSPACE, TABLE, metadata))
                .containsExactly("col_b", "col_a", "col_c");
        assertThat(CassandraUtils.getPartitionKeyString(KEYSPACE, TABLE, metadata))
                .isEqualTo("col_b,col_a,col_c");
    }

    @Test
    void testCaseSensitivePartitionKeyIsQuoted() {
        final Metadata metadata = createMetadata("\"userId\"", "region");

        assertThat(CassandraUtils.getPartitionKey(KEYSPACE, TABLE, metadata))
                .containsExactly("userId", "region");
        assertThat(CassandraUtils.getPartitionKeyString(KEYSPACE, TABLE, metadata))
                .isEqualTo("\"userId\",region");
    }

    @Test
    void testMissingKeyspaceOrTable() {
        final Metadata metadata = createMetadata("id");

        assertThatThrownBy(() -> CassandraUtils.getTable("unknown", TABLE, metadata))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Table unknown.tbl does not exist");
        assertThatThrownBy(() -> CassandraUtils.getPartitionKey(KEYSPACE, "unknown", metadata))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Table ks.unknown does not exist");
    }

    private static Metadata createMetadata(String... partitionKeyColumns) {
        final ColumnMetadata[] columns =
                Arrays.stream(partitionKeyColumns)
                        .map(
                                name -> {
                                    final ColumnMetadata column = mock(ColumnMetadata.class);
                                    when(column.getName()).thenReturn(CqlIdentifier.fromCql(name));
                                    return column;
                                })
                        .toArray(ColumnMetadata[]::new);
        final TableMetadata table = mock(TableMetadata.class);
        when(table.getPartitionKey()).thenReturn(Arrays.asList(columns));
        final KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
        when(keyspace.getTable(TABLE)).thenReturn(Optional.of(table));
        final Metadata metadata = mock(Metadata.class);
        when(metadata.getKeyspace(KEYSPACE)).thenReturn(Optional.of(keyspace));
        return metadata;
    }
}
