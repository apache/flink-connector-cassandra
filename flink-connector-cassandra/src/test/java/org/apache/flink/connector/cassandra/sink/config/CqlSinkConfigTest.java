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

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.connector.cassandra.sink.planner.SinkPluggable;
import org.apache.flink.connector.cassandra.sink.planner.core.resolution.ResolutionMode;
import org.apache.flink.connector.cassandra.sink.planner.resolver.ColumnValueResolver;
import org.apache.flink.connector.cassandra.sink.planner.resolver.TableResolver;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/** Unit tests for {@link CqlSinkConfig}. */
class CqlSinkConfigTest {

    private static SinkPluggable<Row> createValidPluggable() {
        return SinkPluggable.<Row>builder()
                .withTableResolver(mock(TableResolver.class))
                .withColumnValueResolver(mock(ColumnValueResolver.class))
                .build();
    }

    @Test
    void testFactoryMethodsAndDefaults() {
        // Test Row config
        CqlSinkConfig<Row> rowConfig = CqlSinkConfig.forRow();
        assertThat(rowConfig.getRecordFormatType()).isEqualTo(RecordFormatType.ROW);
        assertThat(rowConfig.getQuery()).isNull();
        assertThat(rowConfig.getPluggable()).isNull();
        assertThat(rowConfig.getIgnoreNullFields()).isFalse();
        assertThat(rowConfig.getResolutionMode()).isEqualTo(ResolutionMode.UNSET);

        // Test Tuple config
        CqlSinkConfig<Tuple> tupleConfig = CqlSinkConfig.forTuple();
        assertThat(tupleConfig.getRecordFormatType()).isEqualTo(RecordFormatType.TUPLE);
        assertThat(tupleConfig.getQuery()).isNull();
        assertThat(tupleConfig.getPluggable()).isNull();
        assertThat(tupleConfig.getIgnoreNullFields()).isFalse();
        assertThat(tupleConfig.getResolutionMode()).isEqualTo(ResolutionMode.UNSET);

        // Test Scala Product config
        CqlSinkConfig<scala.Product> scalaConfig = CqlSinkConfig.forScalaProduct();
        assertThat(scalaConfig.getRecordFormatType()).isEqualTo(RecordFormatType.SCALA_PRODUCT);
        assertThat(scalaConfig.getQuery()).isNull();
        assertThat(scalaConfig.getPluggable()).isNull();
        assertThat(scalaConfig.getIgnoreNullFields()).isFalse();
        assertThat(scalaConfig.getResolutionMode()).isEqualTo(ResolutionMode.UNSET);
    }

    @Test
    void testStaticModeWithQuery() {
        CqlSinkConfig<Row> config =
                CqlSinkConfig.forRow()
                        .withQuery("INSERT INTO keyspace.table (id, name) VALUES (?, ?)");

        assertThat(config.getQuery())
                .isEqualTo("INSERT INTO keyspace.table (id, name) VALUES (?, ?)");
        assertThat(config.getResolutionMode()).isEqualTo(ResolutionMode.STATIC);
        assertThat(config.getRecordFormatType()).isEqualTo(RecordFormatType.ROW);
        assertThat(config.getPluggable()).isNull();
    }

    @Test
    void testDynamicModeWithPluggable() {
        SinkPluggable<Row> pluggable = createValidPluggable();
        CqlSinkConfig<Row> config = CqlSinkConfig.forRow().withPluggable(pluggable);

        assertThat(config.getPluggable()).isEqualTo(pluggable);
        assertThat(config.getResolutionMode()).isEqualTo(ResolutionMode.DYNAMIC);
        assertThat(config.getQuery()).isNull();
    }

    @Test
    void testModeConflictsAndValidation() {
        // Test ignoreNullFields fails in DYNAMIC mode
        SinkPluggable<Row> pluggable = createValidPluggable();
        CqlSinkConfig<Row> dynamicConfig = CqlSinkConfig.forRow().withPluggable(pluggable);

        assertThatThrownBy(() -> dynamicConfig.withIgnoreNullFields(true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ignoreNullFields is not supported in DYNAMIC mode")
                .hasMessageContaining("control null/unset behavior via StatementCustomizer");

        // Test query after pluggable fails
        assertThatThrownBy(
                        () -> dynamicConfig.withQuery("INSERT INTO keyspace.table (id) VALUES (?)"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot set query when pluggables are already configured")
                .hasMessageContaining("STATIC and DYNAMIC are mutually exclusive");

        // Test pluggable after query fails
        CqlSinkConfig<Row> staticConfig =
                CqlSinkConfig.forRow().withQuery("INSERT INTO keyspace.table (id) VALUES (?)");

        assertThatThrownBy(() -> staticConfig.withPluggable(pluggable))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot set pluggables when a query is already configured")
                .hasMessageContaining("STATIC and DYNAMIC are mutually exclusive");

        // Test query validation
        CqlSinkConfig<Row> config = CqlSinkConfig.forRow();
        assertThatThrownBy(() -> config.withQuery(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("query must not be blank");

        assertThatThrownBy(() -> config.withQuery(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("query must not be blank");

        assertThatThrownBy(() -> config.withQuery("   "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("query must not be blank");

        // Test pluggable validation
        assertThatThrownBy(() -> CqlSinkConfig.forRow().withPluggable(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pluggable cannot be null");
    }

    @Test
    void testResolutionModeDerivation() {
        // Test with Row and STATIC mode
        CqlSinkConfig<Row> rowConfig = CqlSinkConfig.forRow();
        assertThat(rowConfig.getResolutionMode()).isEqualTo(ResolutionMode.UNSET);

        rowConfig.withQuery("INSERT INTO keyspace.table (id) VALUES (?)");
        assertThat(rowConfig.getResolutionMode()).isEqualTo(ResolutionMode.STATIC);

        // Test with Tuple and DYNAMIC mode
        CqlSinkConfig<Tuple> tupleConfig = CqlSinkConfig.forTuple();
        assertThat(tupleConfig.getResolutionMode()).isEqualTo(ResolutionMode.UNSET);

        @SuppressWarnings("unchecked")
        SinkPluggable<Tuple> tuplePluggable =
                SinkPluggable.<Tuple>builder()
                        .withTableResolver(mock(TableResolver.class))
                        .withColumnValueResolver(mock(ColumnValueResolver.class))
                        .build();

        tupleConfig.withPluggable(tuplePluggable);
        assertThat(tupleConfig.getResolutionMode()).isEqualTo(ResolutionMode.DYNAMIC);
        assertThat(tupleConfig.getPluggable()).isEqualTo(tuplePluggable);
    }

    @Test
    void testAPIChaining() {
        // STATIC mode chaining
        CqlSinkConfig<Row> staticConfig =
                CqlSinkConfig.forRow()
                        .withIgnoreNullFields(true)
                        .withQuery("INSERT INTO keyspace.table (id, name) VALUES (?, ?)");

        assertThat(staticConfig.getIgnoreNullFields()).isTrue();
        assertThat(staticConfig.getQuery())
                .isEqualTo("INSERT INTO keyspace.table (id, name) VALUES (?, ?)");
        assertThat(staticConfig.getResolutionMode()).isEqualTo(ResolutionMode.STATIC);

        // DYNAMIC mode chaining
        SinkPluggable<Row> pluggable = createValidPluggable();
        CqlSinkConfig<Row> dynamicConfig = CqlSinkConfig.forRow().withPluggable(pluggable);

        assertThat(dynamicConfig.getPluggable()).isEqualTo(pluggable);
        assertThat(dynamicConfig.getResolutionMode()).isEqualTo(ResolutionMode.DYNAMIC);
    }

    @Test
    void testSinkPluggableBuilderValidation() {
        // Test that SinkPluggable.builder() requires TableResolver and ColumnValueResolver
        assertThatThrownBy(() -> SinkPluggable.<Row>builder().build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("TableResolver cannot be null");

        // Test with only TableResolver
        assertThatThrownBy(
                        () ->
                                SinkPluggable.<Row>builder()
                                        .withTableResolver(mock(TableResolver.class))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ColumnValueResolver cannot be null");

        // Test with only ColumnValueResolver
        assertThatThrownBy(
                        () ->
                                SinkPluggable.<Row>builder()
                                        .withColumnValueResolver(mock(ColumnValueResolver.class))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("TableResolver cannot be null");

        // Test successful build with both required resolvers
        SinkPluggable<Row> pluggable =
                SinkPluggable.<Row>builder()
                        .withTableResolver(mock(TableResolver.class))
                        .withColumnValueResolver(mock(ColumnValueResolver.class))
                        .build();

        assertThat(pluggable).isNotNull();
        assertThat(pluggable.getTableResolver()).isNotNull();
        assertThat(pluggable.getColumnValueResolver()).isNotNull();
        assertThat(pluggable.getCqlClauseResolver()).isNull();
        assertThat(pluggable.getStatementCustomizer()).isNull();
    }

    @Test
    void testSerializationWithStaticMode() throws Exception {
        CqlSinkConfig<Row> original =
                CqlSinkConfig.forRow()
                        .withIgnoreNullFields(true)
                        .withQuery("INSERT INTO keyspace.table (id, name) VALUES (?, ?)");

        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(original);
        oos.close();

        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        @SuppressWarnings("unchecked")
        CqlSinkConfig<Row> deserialized = (CqlSinkConfig<Row>) ois.readObject();
        ois.close();

        // Verify
        assertThat(deserialized.getQuery()).isEqualTo(original.getQuery());
        assertThat(deserialized.getIgnoreNullFields()).isEqualTo(original.getIgnoreNullFields());
        assertThat(deserialized.getResolutionMode()).isEqualTo(ResolutionMode.STATIC);
        assertThat(deserialized.getRecordFormatType()).isEqualTo(RecordFormatType.ROW);
    }

    @Test
    void testSerializationWithDynamicMode() throws Exception {
        SinkPluggable<Row> pluggable = createValidPluggable();

        CqlSinkConfig<Row> original = CqlSinkConfig.forRow().withPluggable(pluggable);

        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(original);
        oos.close();

        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        @SuppressWarnings("unchecked")
        CqlSinkConfig<Row> deserialized = (CqlSinkConfig<Row>) ois.readObject();
        ois.close();

        // Verify
        assertThat(deserialized.getPluggable()).isNotNull();
        assertThat(deserialized.getIgnoreNullFields()).isFalse();
        assertThat(deserialized.getResolutionMode()).isEqualTo(ResolutionMode.DYNAMIC);
        assertThat(deserialized.getRecordFormatType()).isEqualTo(RecordFormatType.ROW);
    }
}
