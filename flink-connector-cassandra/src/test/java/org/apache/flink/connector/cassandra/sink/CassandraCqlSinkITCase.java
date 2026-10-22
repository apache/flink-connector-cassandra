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

package org.apache.flink.connector.cassandra.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.cassandra.CassandraTestEnvironment;
import org.apache.flink.connector.cassandra.sink.config.CqlSinkConfig;
import org.apache.flink.connector.cassandra.sink.config.write.RequestConfiguration;
import org.apache.flink.connector.cassandra.sink.planner.SinkPluggable;
import org.apache.flink.connector.cassandra.sink.planner.core.customization.StatementCustomizer;
import org.apache.flink.connector.cassandra.sink.planner.core.modifiers.ClauseBindings;
import org.apache.flink.connector.cassandra.sink.planner.core.modifiers.CqlClauseResolver;
import org.apache.flink.connector.cassandra.sink.planner.resolver.ColumnValueResolver;
import org.apache.flink.connector.cassandra.sink.planner.resolver.ResolvedWrite;
import org.apache.flink.connector.cassandra.sink.planner.resolver.TableRef;
import org.apache.flink.connector.cassandra.sink.planner.resolver.TableResolver;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Update;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.ttl;
import static org.apache.flink.connector.cassandra.CassandraTestEnvironment.KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Comprehensive integration tests for CassandraSink V2 with CQL queries covering Row and Tuple
 * types in both static and dynamic modes.
 */
@Testcontainers
class CassandraCqlSinkITCase {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_EXTENSION = new MiniClusterExtension();

    // Static ClusterBuilder to avoid serialization issues
    public static class TestClusterBuilder extends ClusterBuilder {
        private final String host;
        private final int port;

        public TestClusterBuilder(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        protected Cluster buildCluster(Cluster.Builder builder) {
            return builder.addContactPoint(host).withPort(port).build();
        }
    }

    private static final CassandraTestEnvironment cassandraTestEnvironment =
            new CassandraTestEnvironment(false);

    private static Integer port;
    private static String host;

    // TypeInformation constants for Row-based tests
    private static final TypeInformation<Row> ROW_S_S_I =
            Types.ROW(Types.STRING, Types.STRING, Types.INT);
    private static final TypeInformation<Row> ROW_S_S = Types.ROW(Types.STRING, Types.STRING);
    private static final TypeInformation<Row> ROW_I_S = Types.ROW(Types.INT, Types.STRING);
    private static final TypeInformation<Row> ROW_I_S_S =
            Types.ROW(Types.INT, Types.STRING, Types.STRING);
    private static final TypeInformation<Row> ROW_S_I_S =
            Types.ROW(Types.STRING, Types.INT, Types.STRING);
    private static final TypeInformation<Row> ROW_S = Types.ROW(Types.STRING);

    @BeforeAll
    static void setupCassandra() throws Exception {
        cassandraTestEnvironment.startUp();
        createTestTables();
        port = cassandraTestEnvironment.getPort();
        host = cassandraTestEnvironment.getHost();
    }

    @AfterAll
    static void tearDownCassandra() throws Exception {
        dropTestTables();
        cassandraTestEnvironment.tearDown();
    }

    @BeforeEach
    void clearTables() {
        // Clear all test tables
        clearTable("test_table");
        clearTable("test_table_composite");
        clearTable("test_table_lwt");
        clearTable("test_table_ttl");
        clearTable("test_table_even");
        clearTable("test_table_odd");
        clearTable("\"UserEvents\"");
    }

    private void clearTable(String tableName) {
        try {
            String truncateQuery = String.format("TRUNCATE %s.%s;", KEYSPACE, tableName);
            cassandraTestEnvironment.executeRequestWithTimeout(truncateQuery);
        } catch (Exception ignored) {
            // Ignore if table doesn't exist
        }
    }

    private static void createTestTables() {
        // Simple table for basic tests
        String createSimple =
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s.test_table ("
                                + "id text PRIMARY KEY, "
                                + "name text, "
                                + "age int"
                                + ");",
                        KEYSPACE);
        cassandraTestEnvironment.executeRequestWithTimeout(createSimple);

        // Table with composite key
        String createComposite =
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s.test_table_composite ("
                                + "pk text, "
                                + "ck int, "
                                + "value text, "
                                + "PRIMARY KEY (pk, ck)"
                                + ");",
                        KEYSPACE);
        cassandraTestEnvironment.executeRequestWithTimeout(createComposite);

        // Table for LWT tests
        String createLWT =
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s.test_table_lwt ("
                                + "id text PRIMARY KEY, "
                                + "name text, "
                                + "version int"
                                + ");",
                        KEYSPACE);
        cassandraTestEnvironment.executeRequestWithTimeout(createLWT);

        // Table for TTL tests
        String createTTL =
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s.test_table_ttl ("
                                + "id text PRIMARY KEY, "
                                + "value text"
                                + ");",
                        KEYSPACE);
        cassandraTestEnvironment.executeRequestWithTimeout(createTTL);

        // Tables for routing tests
        String createEven =
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s.test_table_even ("
                                + "id int PRIMARY KEY, "
                                + "data text"
                                + ");",
                        KEYSPACE);
        cassandraTestEnvironment.executeRequestWithTimeout(createEven);

        String createOdd =
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s.test_table_odd ("
                                + "id int PRIMARY KEY, "
                                + "data text"
                                + ");",
                        KEYSPACE);
        cassandraTestEnvironment.executeRequestWithTimeout(createOdd);

        // Table for quoted identifier tests - using case-sensitive identifiers
        String createQuoted =
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s.\"UserEvents\" ("
                                + "\"UserId\" text PRIMARY KEY, "
                                + "\"EventName\" text, "
                                + "\"Value\" int"
                                + ");",
                        KEYSPACE);
        cassandraTestEnvironment.executeRequestWithTimeout(createQuoted);
    }

    private static void dropTestTables() {
        String[] tables = {
            "test_table", "test_table_composite", "test_table_lwt",
            "test_table_ttl", "test_table_even", "test_table_odd",
            "\"UserEvents\""
        };

        for (String table : tables) {
            try {
                String dropQuery = String.format("DROP TABLE IF EXISTS %s.%s;", KEYSPACE, table);
                cassandraTestEnvironment.executeRequestWithTimeout(dropQuery);
            } catch (Exception ignored) {
                // Ignore errors
            }
        }
    }

    @Test
    void testStaticInsertBasicRow() throws Exception {
        // Tests basic INSERT operation with Row type in static mode
        List<Row> rows = new ArrayList<>();
        rows.add(Row.of("row1", "Alice", 25));
        rows.add(Row.of("row2", "Bob", 30));
        rows.add(Row.of("row3", "Charlie", 35));

        String insertQuery =
                String.format(
                        "INSERT INTO %s.test_table (id, name, age) VALUES (?, ?, ?)", KEYSPACE);

        CqlSinkConfig<Row> config = CqlSinkConfig.forRow().withQuery(insertQuery);

        TypeInformation<Row> rowTypeInfo = Types.ROW(Types.STRING, Types.STRING, Types.INT);
        executeSinkAndWait(rows, config, rowTypeInfo);

        // Verify data was inserted
        verifyRowExists("test_table", "id", "row1", "name", "Alice", "age", 25);
        verifyRowExists("test_table", "id", "row2", "name", "Bob", "age", 30);
        verifyRowExists("test_table", "id", "row3", "name", "Charlie", "age", 35);
    }

    @Test
    void testStaticInsertWithCaseSensitiveIdentifiersRow() throws Exception {
        // Tests INSERT with case-sensitive quoted table and column identifiers
        // Test with case-sensitive column names (quoted in table creation)
        List<Row> rows = Collections.singletonList(Row.of("q1", "quoted-test", 42));

        String insertQuery =
                String.format(
                        "INSERT INTO %s.\"UserEvents\" (\"UserId\", \"EventName\", \"Value\") VALUES (?, ?, ?)",
                        KEYSPACE);

        CqlSinkConfig<Row> config = CqlSinkConfig.forRow().withQuery(insertQuery);

        executeSinkAndWait(rows, config, ROW_S_S_I);

        // Verify with quoted columns
        String selectQuery =
                String.format(
                        "SELECT \"UserId\", \"EventName\", \"Value\" FROM %s.\"UserEvents\" WHERE \"UserId\" = 'q1'",
                        KEYSPACE);
        ResultSet rs = cassandraTestEnvironment.executeRequestWithTimeout(selectQuery);
        com.datastax.driver.core.Row row = rs.one();
        assertThat(row).isNotNull();
        assertThat(row.getString("UserId")).isEqualTo("q1");
        assertThat(row.getString("EventName")).isEqualTo("quoted-test");
        assertThat(row.getInt("Value")).isEqualTo(42);
    }

    @Test
    void testStaticInsertWithTTLRow() throws Exception {
        // Tests INSERT with static TTL clause that expires records after specified seconds
        List<Row> rows = Collections.singletonList(Row.of("ttl1", "will-expire"));

        String insertQuery =
                String.format(
                        "INSERT INTO %s.test_table_ttl (id, value) VALUES (?, ?) USING TTL 5",
                        KEYSPACE);

        CqlSinkConfig<Row> config = CqlSinkConfig.forRow().withQuery(insertQuery);

        executeSinkAndWait(rows, config, ROW_S_S);

        // Give some time for write to complete
        Thread.sleep(500);

        // Verify row exists initially
        verifyRowExists("test_table_ttl", "id", "ttl1", "value", "will-expire");

        // Wait for TTL to expire
        Thread.sleep(6000);

        // Verify row is gone
        String selectQuery =
                String.format("SELECT * FROM %s.test_table_ttl WHERE id = 'ttl1'", KEYSPACE);
        ResultSet rs = cassandraTestEnvironment.executeRequestWithTimeout(selectQuery);
        assertThat(rs.one()).isNull();
    }

    @Test
    void testStaticInsertWithTimestampRow() throws Exception {
        // Tests INSERT with custom TIMESTAMP clause for setting write time
        List<Row> rows = Collections.singletonList(Row.of("ts1", "with-timestamp", 100));

        long timestamp = System.currentTimeMillis() * 1000; // microseconds
        String insertQuery =
                String.format(
                        "INSERT INTO %s.test_table (id, name, age) VALUES (?, ?, ?) USING TIMESTAMP %d",
                        KEYSPACE, timestamp);

        CqlSinkConfig<Row> config = CqlSinkConfig.forRow().withQuery(insertQuery);

        executeSinkAndWait(rows, config, ROW_S_S_I);

        // Verify writetime
        String selectQuery =
                String.format(
                        "SELECT writetime(name) FROM %s.test_table WHERE id = 'ts1'", KEYSPACE);
        ResultSet rs = cassandraTestEnvironment.executeRequestWithTimeout(selectQuery);
        com.datastax.driver.core.Row row = rs.one();
        assertThat(row.getLong(0)).isEqualTo(timestamp);
    }

    @Test
    void testStaticInsertIfNotExistsRow() throws Exception {
        // Tests INSERT with IF NOT EXISTS clause for conditional inserts
        // First insert should succeed
        List<Row> rows1 = Collections.singletonList(Row.of("lwt1", "first", 1));

        String insertQuery =
                String.format(
                        "INSERT INTO %s.test_table_lwt (id, name, version) VALUES (?, ?, ?) IF NOT EXISTS",
                        KEYSPACE);

        CqlSinkConfig<Row> config = CqlSinkConfig.forRow().withQuery(insertQuery);

        executeSinkAndWait(rows1, config, ROW_S_S_I);

        verifyRowExists("test_table_lwt", "id", "lwt1", "name", "first", "version", 1);

        // Second insert with same key should be ignored
        List<Row> rows2 = Collections.singletonList(Row.of("lwt1", "second", 2));
        executeSinkAndWait(rows2, config, ROW_S_S_I);

        // Verify original data remains
        verifyRowExists("test_table_lwt", "id", "lwt1", "name", "first", "version", 1);
    }

    @Test
    void testStaticInsertWithIgnoreNullFieldsRow() throws Exception {
        // Tests INSERT with ignoreNullFields=true to prevent tombstone creation
        // Define TypeInformation for Row to avoid Kryo serialization
        TypeInformation<Row> rowTypeInfo = Types.ROW(Types.STRING, Types.STRING, Types.INT);

        // First insert complete row
        List<Row> rows1 = Collections.singletonList(Row.of("null1", "initial", 50));

        String insertQuery =
                String.format(
                        "INSERT INTO %s.test_table (id, name, age) VALUES (?, ?, ?)", KEYSPACE);

        CqlSinkConfig<Row> config =
                CqlSinkConfig.forRow().withQuery(insertQuery).withIgnoreNullFields(true);

        executeSinkAndWait(rows1, config, rowTypeInfo);

        // Insert with null field - should not create tombstone
        List<Row> rows2 = Collections.singletonList(Row.of("null1", null, 60));
        executeSinkAndWait(rows2, config, rowTypeInfo);

        // Verify name is still present (not tombstoned)
        verifyRowExists("test_table", "id", "null1", "name", "initial", "age", 60);
    }

    // ============= Static INSERT Tests for Tuple =============

    @Test
    void testStaticInsertBasicTuple() throws Exception {
        // Tests basic INSERT operation with Tuple type in static mode
        List<Tuple3<String, String, Integer>> tuples =
                Arrays.asList(Tuple3.of("tuple1", "Alice", 25), Tuple3.of("tuple2", "Bob", 30));

        String insertQuery =
                String.format(
                        "INSERT INTO %s.test_table (id, name, age) VALUES (?, ?, ?)", KEYSPACE);

        CqlSinkConfig<Tuple> config = CqlSinkConfig.forTuple().withQuery(insertQuery);

        executeSinkAndWait((List<Tuple>) (List<?>) tuples, config);

        verifyRowExists("test_table", "id", "tuple1", "name", "Alice", "age", 25);
        verifyRowExists("test_table", "id", "tuple2", "name", "Bob", "age", 30);
    }

    @Test
    void testStaticInsertWithTTLTuple() throws Exception {
        // Tests INSERT with TTL using Tuple type that expires after specified seconds
        List<Tuple2<String, String>> tuples =
                Collections.singletonList(Tuple2.of("ttl_tuple", "expires"));

        String insertQuery =
                String.format(
                        "INSERT INTO %s.test_table_ttl (id, value) VALUES (?, ?) USING TTL 5",
                        KEYSPACE);

        CqlSinkConfig<Tuple> config = CqlSinkConfig.forTuple().withQuery(insertQuery);

        executeSinkAndWait((List<Tuple>) (List<?>) tuples, config);

        // Give some time for write to complete
        Thread.sleep(500);

        verifyRowExists("test_table_ttl", "id", "ttl_tuple", "value", "expires");

        Thread.sleep(6000);

        String selectQuery =
                String.format("SELECT * FROM %s.test_table_ttl WHERE id = 'ttl_tuple'", KEYSPACE);
        ResultSet rs = cassandraTestEnvironment.executeRequestWithTimeout(selectQuery);
        assertThat(rs.one()).isNull();
    }

    // ============= Static UPDATE Tests for Row =============

    @Test
    void testStaticUpdateBasicRow() throws Exception {
        // Tests basic UPDATE operation with Row type in static mode
        // First insert some data
        String setupQuery =
                String.format(
                        "INSERT INTO %s.test_table (id, name, age) VALUES ('update1', 'Old', 20)",
                        KEYSPACE);
        cassandraTestEnvironment.executeRequestWithTimeout(setupQuery);

        // Update the data
        List<Row> rows = Collections.singletonList(Row.of("New", 25, "update1"));

        String updateQuery =
                String.format("UPDATE %s.test_table SET name = ?, age = ? WHERE id = ?", KEYSPACE);

        CqlSinkConfig<Row> config = CqlSinkConfig.forRow().withQuery(updateQuery);

        executeSinkAndWait(rows, config, ROW_S_I_S);

        verifyRowExists("test_table", "id", "update1", "name", "New", "age", 25);
    }

    @Test
    void testStaticUpdateMultipleColumnsRow() throws Exception {
        // Tests UPDATE operations targeting different columns separately
        // Setup
        String setupQuery =
                String.format(
                        "INSERT INTO %s.test_table (id, name, age) VALUES ('multi1', 'Initial', 30)",
                        KEYSPACE);
        cassandraTestEnvironment.executeRequestWithTimeout(setupQuery);

        // Update only name
        List<Row> rows1 = Arrays.asList(Row.of("Updated", "multi1"));
        String updateQuery1 =
                String.format("UPDATE %s.test_table SET name = ? WHERE id = ?", KEYSPACE);
        CqlSinkConfig<Row> config1 = CqlSinkConfig.forRow().withQuery(updateQuery1);
        executeSinkAndWait(rows1, config1, ROW_S_S);

        // Update only age
        List<Row> rows2 = Arrays.asList(Row.of(35, "multi1"));
        String updateQuery2 =
                String.format("UPDATE %s.test_table SET age = ? WHERE id = ?", KEYSPACE);
        CqlSinkConfig<Row> config2 = CqlSinkConfig.forRow().withQuery(updateQuery2);
        executeSinkAndWait(rows2, config2, ROW_I_S);

        verifyRowExists("test_table", "id", "multi1", "name", "Updated", "age", 35);
    }

    @Test
    void testStaticUpdateWithCompositeKeyRow() throws Exception {
        // Tests UPDATE with composite primary key (partition key + clustering key)
        // Setup
        String setupQuery =
                String.format(
                        "INSERT INTO %s.test_table_composite (pk, ck, value) VALUES ('pk1', 1, 'old')",
                        KEYSPACE);
        cassandraTestEnvironment.executeRequestWithTimeout(setupQuery);

        // Update
        List<Row> rows = Arrays.asList(Row.of("new", "pk1", 1));
        String updateQuery =
                String.format(
                        "UPDATE %s.test_table_composite SET value = ? WHERE pk = ? AND ck = ?",
                        KEYSPACE);
        CqlSinkConfig<Row> config = CqlSinkConfig.forRow().withQuery(updateQuery);
        executeSinkAndWait(rows, config, ROW_S_S_I);

        String selectQuery =
                String.format(
                        "SELECT * FROM %s.test_table_composite WHERE pk = 'pk1' AND ck = 1",
                        KEYSPACE);
        ResultSet rs = cassandraTestEnvironment.executeRequestWithTimeout(selectQuery);
        com.datastax.driver.core.Row row = rs.one();
        assertThat(row.getString("value")).isEqualTo("new");
    }

    @Test
    void testStaticUpdateWithTTLRow() throws Exception {
        // Tests UPDATE with TTL clause that expires updated columns after specified seconds
        // Setup
        String setupQuery =
                String.format(
                        "INSERT INTO %s.test_table_ttl (id, value) VALUES ('ttl_update', 'initial')",
                        KEYSPACE);
        cassandraTestEnvironment.executeRequestWithTimeout(setupQuery);

        // Update with TTL
        List<Row> rows = Arrays.asList(Row.of("updated", "ttl_update"));
        String updateQuery =
                String.format(
                        "UPDATE %s.test_table_ttl USING TTL 5 SET value = ? WHERE id = ?",
                        KEYSPACE);
        CqlSinkConfig<Row> config = CqlSinkConfig.forRow().withQuery(updateQuery);
        executeSinkAndWait(rows, config, ROW_S_S);

        // Give some time for write to complete
        Thread.sleep(500);

        verifyRowExists("test_table_ttl", "id", "ttl_update", "value", "updated");

        Thread.sleep(6000);

        // The value column should expire but row remains
        String selectQuery =
                String.format(
                        "SELECT value FROM %s.test_table_ttl WHERE id = 'ttl_update'", KEYSPACE);
        ResultSet rs = cassandraTestEnvironment.executeRequestWithTimeout(selectQuery);
        com.datastax.driver.core.Row row = rs.one();
        assertThat(row.isNull("value")).isTrue();
    }

    @Test
    void testStaticUpdateBasicTuple() throws Exception {
        // Tests basic UPDATE operation with Tuple type in static mode
        // Setup
        String setupQuery =
                String.format(
                        "INSERT INTO %s.test_table (id, name, age) VALUES ('tuple_up', 'Old', 20)",
                        KEYSPACE);
        cassandraTestEnvironment.executeRequestWithTimeout(setupQuery);

        // Update
        List<Tuple3<String, Integer, String>> tuples =
                Collections.singletonList(Tuple3.of("New", 25, "tuple_up"));

        String updateQuery =
                String.format("UPDATE %s.test_table SET name = ?, age = ? WHERE id = ?", KEYSPACE);

        CqlSinkConfig<Tuple> config = CqlSinkConfig.forTuple().withQuery(updateQuery);

        executeSinkAndWait((List<Tuple>) (List<?>) tuples, config);

        verifyRowExists("test_table", "id", "tuple_up", "name", "New", "age", 25);
    }

    private static class EvenOddTableResolver implements TableResolver<Row> {
        @Override
        public TableRef resolve(Row record) {
            int id = (int) record.getField(0);
            String tableName = (id % 2 == 0) ? "test_table_even" : "test_table_odd";
            return new TableRef(KEYSPACE, tableName);
        }
    }

    private static class InsertColumnResolver implements ColumnValueResolver<Row> {
        @Override
        public Kind kind() {
            return Kind.INSERT;
        }

        @Override
        public ResolvedWrite resolve(Row record) {
            int id = (int) record.getField(0);
            String data = (String) record.getField(1);
            return ResolvedWrite.insert(Arrays.asList("id", "data"), new Object[] {id, data});
        }
    }

    @Test
    void testDynamicInsertWithTableRoutingRow() throws Exception {
        // Tests dynamic mode with TableResolver routing records to different tables based on
        // content
        // Create dynamic pluggable that routes to different tables based on id
        TableResolver<Row> tableResolver = new EvenOddTableResolver();
        ColumnValueResolver<Row> columnResolver = new InsertColumnResolver();

        SinkPluggable<Row> pluggable =
                SinkPluggable.<Row>builder()
                        .withTableResolver(tableResolver)
                        .withColumnValueResolver(columnResolver)
                        .build();

        CqlSinkConfig<Row> config = CqlSinkConfig.forRow().withPluggable(pluggable);

        List<Row> rows =
                Arrays.asList(
                        Row.of(1, "odd-one"),
                        Row.of(2, "even-two"),
                        Row.of(3, "odd-three"),
                        Row.of(4, "even-four"));

        executeSinkAndWait(rows, config, ROW_I_S);

        // Verify routing
        verifyRowExists("test_table_odd", "id", 1, "data", "odd-one");
        verifyRowExists("test_table_even", "id", 2, "data", "even-two");
        verifyRowExists("test_table_odd", "id", 3, "data", "odd-three");
        verifyRowExists("test_table_even", "id", 4, "data", "even-four");
    }

    private static class CompositeTableResolver implements TableResolver<Row> {
        @Override
        public TableRef resolve(Row record) {
            return new TableRef(KEYSPACE, "test_table_composite");
        }
    }

    private static class UpdateColumnResolver implements ColumnValueResolver<Row> {
        @Override
        public Kind kind() {
            return Kind.UPDATE;
        }

        @Override
        public ResolvedWrite resolve(Row record) {
            String pk = (String) record.getField(0);
            int ck = (int) record.getField(1);
            String value = (String) record.getField(2);
            return ResolvedWrite.update(
                    Collections.singletonList("value"),
                    new Object[] {value},
                    Arrays.asList("pk", "ck"),
                    new Object[] {pk, ck});
        }
    }

    @Test
    void testDynamicUpdateWithCompositeKeysRow() throws Exception {
        // Tests dynamic mode UPDATE with composite keys using ColumnValueResolver
        // Setup data
        String setupQuery1 =
                String.format(
                        "INSERT INTO %s.test_table_composite (pk, ck, value) VALUES ('pk1', 1, 'old1')",
                        KEYSPACE);
        String setupQuery2 =
                String.format(
                        "INSERT INTO %s.test_table_composite (pk, ck, value) VALUES ('pk1', 2, 'old2')",
                        KEYSPACE);
        cassandraTestEnvironment.executeRequestWithTimeout(setupQuery1);
        cassandraTestEnvironment.executeRequestWithTimeout(setupQuery2);

        TableResolver<Row> tableResolver = new CompositeTableResolver();
        ColumnValueResolver<Row> columnResolver = new UpdateColumnResolver();

        SinkPluggable<Row> pluggable =
                SinkPluggable.<Row>builder()
                        .withTableResolver(tableResolver)
                        .withColumnValueResolver(columnResolver)
                        .build();

        CqlSinkConfig<Row> config = CqlSinkConfig.forRow().withPluggable(pluggable);

        List<Row> rows = Arrays.asList(Row.of("pk1", 1, "new1"), Row.of("pk1", 2, "new2"));

        executeSinkAndWait(rows, config, ROW_S_I_S);

        // Verify updates
        String selectQuery1 =
                String.format(
                        "SELECT * FROM %s.test_table_composite WHERE pk = 'pk1' AND ck = 1",
                        KEYSPACE);
        ResultSet rs1 = cassandraTestEnvironment.executeRequestWithTimeout(selectQuery1);
        assertThat(rs1.one().getString("value")).isEqualTo("new1");

        String selectQuery2 =
                String.format(
                        "SELECT * FROM %s.test_table_composite WHERE pk = 'pk1' AND ck = 2",
                        KEYSPACE);
        ResultSet rs2 = cassandraTestEnvironment.executeRequestWithTimeout(selectQuery2);
        assertThat(rs2.one().getString("value")).isEqualTo("new2");
    }

    private static class SimpleTableResolver implements TableResolver<Row> {
        @Override
        public TableRef resolve(Row record) {
            return new TableRef(KEYSPACE, "test_table");
        }
    }

    private static class SimpleInsertColumnResolver implements ColumnValueResolver<Row> {
        @Override
        public Kind kind() {
            return Kind.INSERT;
        }

        @Override
        public ResolvedWrite resolve(Row record) {
            String id = (String) record.getField(0);
            String name = (String) record.getField(1);
            int age = (int) record.getField(2);
            return ResolvedWrite.insert(
                    Arrays.asList("id", "name", "age"), new Object[] {id, name, age});
        }
    }

    @Test
    void testDynamicWithTableAndColumnsRow() throws Exception {
        // Tests basic dynamic mode with TableResolver and ColumnValueResolver without
        // CqlClauseResolver
        // Simple test for dynamic mode without CqlClauseResolver
        TableResolver<Row> tableResolver = new SimpleTableResolver();
        ColumnValueResolver<Row> columnResolver = new SimpleInsertColumnResolver();

        SinkPluggable<Row> pluggable =
                SinkPluggable.<Row>builder()
                        .withTableResolver(tableResolver)
                        .withColumnValueResolver(columnResolver)
                        .build();

        CqlSinkConfig<Row> config = CqlSinkConfig.forRow().withPluggable(pluggable);

        List<Row> rows = Arrays.asList(Row.of("dyn1", "dynamic", 100));

        executeSinkAndWait(rows, config, ROW_S_S_I);

        verifyRowExists("test_table", "id", "dyn1", "name", "dynamic", "age", 100);
    }

    // ============= Cross-cutting Tests =============

    @Test
    void testBackpressureWithRequestConfigurationRow() throws Exception {
        // Tests backpressure handling with limited concurrent requests and large batch size
        // Test with limited concurrency to simulate backpressure
        RequestConfiguration requestConfig =
                RequestConfiguration.builder().setMaxConcurrentRequests(1).build();

        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            rows.add(Row.of("bp" + i, "name" + i, i));
        }

        String insertQuery =
                String.format(
                        "INSERT INTO %s.test_table (id, name, age) VALUES (?, ?, ?)", KEYSPACE);

        CqlSinkConfig<Row> config = CqlSinkConfig.forRow().withQuery(insertQuery);

        executeSinkAndWaitWithRequestConfig(rows, config, requestConfig, ROW_S_S_I);

        // Verify all records were inserted despite backpressure
        for (int i = 0; i < 100; i++) {
            verifyRowExists("test_table", "id", "bp" + i, "name", "name" + i, "age", i);
        }
    }

    @Test
    void testFatalErrorPropagationRow() throws Exception {
        // Tests proper error propagation when sink configuration is invalid (wrong parameter count)
        List<Row> rows = Collections.singletonList(Row.of("error1", "test", 25));

        // Invalid query with wrong number of parameters
        String invalidQuery =
                String.format("INSERT INTO %s.test_table (id, name, age) VALUES (?, ?)", KEYSPACE);

        CqlSinkConfig<Row> config = CqlSinkConfig.forRow().withQuery(invalidQuery);

        assertThatThrownBy(() -> executeSinkAndWait(rows, config, ROW_S_S_I))
                .isInstanceOf(org.apache.flink.runtime.client.JobExecutionException.class)
                .hasStackTraceContaining("Column count")
                .hasStackTraceContaining("must match value placeholder count");
    }

    @Test
    void testLargeBatchRow() throws Exception {
        // Tests processing large batches (1000 records) for performance and stability
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            rows.add(Row.of("batch" + i, "name" + i, i % 100));
        }

        String insertQuery =
                String.format(
                        "INSERT INTO %s.test_table (id, name, age) VALUES (?, ?, ?)", KEYSPACE);

        CqlSinkConfig<Row> config = CqlSinkConfig.forRow().withQuery(insertQuery);

        executeSinkAndWait(rows, config, ROW_S_S_I);

        // Spot check some records
        verifyRowExists("test_table", "id", "batch0", "name", "name0", "age", 0);
        verifyRowExists("test_table", "id", "batch500", "name", "name500", "age", 0);
        verifyRowExists("test_table", "id", "batch999", "name", "name999", "age", 99);
    }

    // ============= Static Mode Requirement Tests =============

    @Test
    void testStaticModeRequiresFQN() throws Exception {
        // Tests that static mode requires fully qualified keyspace.table names
        List<Row> rows = Collections.singletonList(Row.of("fqn1", "test", 25));

        // Query without keyspace - should fail
        String invalidQuery = "INSERT INTO test_table (id, name, age) VALUES (?, ?, ?)";

        CqlSinkConfig<Row> config = CqlSinkConfig.forRow().withQuery(invalidQuery);

        assertThatThrownBy(() -> executeSinkAndWait(rows, config, ROW_S_S_I))
                .isInstanceOf(JobExecutionException.class)
                .hasStackTraceContaining("Static mode requires fully qualified keyspace.table");
    }

    @Test
    void testBindMarkerParameterCountValidation() throws Exception {
        // Tests bind marker parameter count validation and error handling
        // Test parameter count validation and error handling with bind markers

        // Setup - insert some test data
        for (int i = 1; i <= 3; i++) {
            String query =
                    String.format(
                            "INSERT INTO %s.test_table (id, name, age) VALUES ('in%d', 'name%d', %d)",
                            KEYSPACE, i, i, i * 10);
            cassandraTestEnvironment.executeRequestWithTimeout(query);
        }

        // Test simple UPDATE without WHERE IN first
        List<Row> simpleRows = Collections.singletonList(Row.of("updated", "in1"));
        String simpleQuery =
                String.format("UPDATE %s.test_table SET name = ? WHERE id = ?", KEYSPACE);
        CqlSinkConfig<Row> simpleConfig = CqlSinkConfig.forRow().withQuery(simpleQuery);
        executeSinkAndWait(simpleRows, simpleConfig, ROW_S_S);

        // Verify simple update worked
        verifyRowExists("test_table", "id", "in1", "name", "updated");

        // Test failure case - wrong number of values
        List<Row> wrongRows = Collections.singletonList(Row.of("failed"));

        assertThatThrownBy(() -> executeSinkAndWait(wrongRows, simpleConfig, ROW_S))
                .isInstanceOf(JobExecutionException.class)
                .hasStackTraceContaining("UPDATE parameter count mismatch");
    }

    @Test
    void testWhereInClauseWithBindMarkers() throws Exception {
        // Tests WHERE IN clause with bind markers mixed with literal values
        // Test WHERE IN clause with bind markers (static mode requires at least one ?)

        // Setup - insert test data
        for (int i = 1; i <= 5; i++) {
            String query =
                    String.format(
                            "INSERT INTO %s.test_table (id, name, age) VALUES ('batch%d', 'name%d', %d)",
                            KEYSPACE, i, i, i * 10);
            cassandraTestEnvironment.executeRequestWithTimeout(query);
        }

        // Test UPDATE with WHERE id IN (mix of literal and bind marker)
        // Note: Static mode requires at least one bind marker in WHERE clause
        List<Row> rows = new ArrayList<>();
        rows.add(Row.of("batch_updated", "batch1"));

        String updateQuery =
                String.format(
                        "UPDATE %s.test_table SET name = ? WHERE id IN (?, 'batch3')", KEYSPACE);

        CqlSinkConfig<Row> config = CqlSinkConfig.forRow().withQuery(updateQuery);
        TypeInformation<Row> rowTypeInfo = Types.ROW(Types.STRING, Types.STRING);
        executeSinkAndWait(rows, config, rowTypeInfo);

        // Verify updates applied to specified records
        verifyRowExists("test_table", "id", "batch1", "name", "batch_updated");
        verifyRowExists("test_table", "id", "batch3", "name", "batch_updated");

        // Verify other records unchanged
        verifyRowExists("test_table", "id", "batch2", "name", "name2");
        verifyRowExists("test_table", "id", "batch4", "name", "name4");
        verifyRowExists("test_table", "id", "batch5", "name", "name5");
    }

    @Test
    void testSetColumnsOrdering() throws Exception {
        // Tests UPDATE/INSERT with column ordering different from Row field order
        // Setup - insert initial data
        String setupQuery =
                String.format(
                        "INSERT INTO %s.test_table (id, name, age) VALUES ('order1', 'initial', 10)",
                        KEYSPACE);
        cassandraTestEnvironment.executeRequestWithTimeout(setupQuery);

        // Test UPDATE with column order different from record order
        // Query expects: age, name, id
        // Row provides: age, name, id (matching the query order)
        List<Row> rows = Collections.singletonList(Row.of(30, "reordered", "order1"));

        String updateQuery =
                String.format("UPDATE %s.test_table SET age = ?, name = ? WHERE id = ?", KEYSPACE);

        CqlSinkConfig<Row> config = CqlSinkConfig.forRow().withQuery(updateQuery);
        executeSinkAndWait(rows, config, ROW_I_S_S);

        // Verify the update with correct column mapping
        verifyRowExists("test_table", "id", "order1", "name", "reordered", "age", 30);

        // Test INSERT with different column ordering
        List<Row> insertRows = Collections.singletonList(Row.of(40, "order2", "inserted"));

        String insertQuery =
                String.format(
                        "INSERT INTO %s.test_table (age, id, name) VALUES (?, ?, ?)", KEYSPACE);

        CqlSinkConfig<Row> insertConfig = CqlSinkConfig.forRow().withQuery(insertQuery);
        executeSinkAndWait(insertRows, insertConfig, ROW_I_S_S);

        // Verify insert worked with correct mapping
        verifyRowExists("test_table", "id", "order2", "name", "inserted", "age", 40);
    }

    @Test
    void testQuotedIdentifiersInsertAndUpdate() throws Exception {
        // Tests INSERT and UPDATE with case-sensitive quoted column identifiers
        // Test INSERT with case-sensitive quoted identifiers
        List<Row> insertRows = Collections.singletonList(Row.of("user123", "login", 42));

        String insertQuery =
                String.format(
                        "INSERT INTO %s.\"UserEvents\" (\"UserId\", \"EventName\", \"Value\") VALUES (?, ?, ?)",
                        KEYSPACE);

        CqlSinkConfig<Row> insertConfig = CqlSinkConfig.forRow().withQuery(insertQuery);
        executeSinkAndWait(insertRows, insertConfig, ROW_S_S_I);

        // Verify insert with quoted columns - note case sensitivity
        String selectQuery =
                String.format(
                        "SELECT \"UserId\", \"EventName\", \"Value\" FROM %s.\"UserEvents\" WHERE \"UserId\" = 'user123'",
                        KEYSPACE);
        ResultSet rs = cassandraTestEnvironment.executeRequestWithTimeout(selectQuery);
        com.datastax.driver.core.Row row = rs.one();
        assertThat(row.getString("UserId")).isEqualTo("user123");
        assertThat(row.getString("EventName")).isEqualTo("login");
        assertThat(row.getInt("Value")).isEqualTo(42);

        // Test UPDATE with quoted identifiers
        List<Row> updateRows = Collections.singletonList(Row.of("logout", 100, "user123"));

        String updateQuery =
                String.format(
                        "UPDATE %s.\"UserEvents\" SET \"EventName\" = ?, \"Value\" = ? WHERE \"UserId\" = ?",
                        KEYSPACE);

        CqlSinkConfig<Row> updateConfig = CqlSinkConfig.forRow().withQuery(updateQuery);
        executeSinkAndWait(updateRows, updateConfig, ROW_S_I_S);

        // Verify update
        rs = cassandraTestEnvironment.executeRequestWithTimeout(selectQuery);
        row = rs.one();
        assertThat(row.getString("EventName")).isEqualTo("logout");
        assertThat(row.getInt("Value")).isEqualTo(100);
    }

    private <T> void executeSinkAndWait(List<T> records, CqlSinkConfig<T> config) throws Exception {
        executeSinkAndWait(records, config, null);
    }

    private <T> void executeSinkAndWait(
            List<T> records, CqlSinkConfig<T> config, TypeInformation<T> typeInfo)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<T> stream;
        if (typeInfo != null) {
            stream = env.fromCollection(records, typeInfo);
        } else {
            stream = env.fromCollection(records);
        }

        CassandraSink<T> sink =
                CassandraSinkBuilder.newBuilder(config)
                        .setClusterBuilder(new TestClusterBuilder(host, port))
                        .build();

        stream.sinkTo(sink);
        env.execute();
    }

    private <T> void executeSinkAndWaitWithRequestConfig(
            List<T> records, CqlSinkConfig<T> config, RequestConfiguration requestConfig)
            throws Exception {
        executeSinkAndWaitWithRequestConfig(records, config, requestConfig, null);
    }

    private <T> void executeSinkAndWaitWithRequestConfig(
            List<T> records,
            CqlSinkConfig<T> config,
            RequestConfiguration requestConfig,
            TypeInformation<T> typeInfo)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<T> stream;
        if (typeInfo != null) {
            stream = env.fromCollection(records, typeInfo);
        } else {
            stream = env.fromCollection(records);
        }

        CassandraSink<T> sink =
                CassandraSinkBuilder.newBuilder(config)
                        .setClusterBuilder(new TestClusterBuilder(host, port))
                        .setRequestConfiguration(requestConfig)
                        .build();

        stream.sinkTo(sink);
        env.execute();
    }

    private void verifyRowExists(String table, Object... columnValuePairs) {
        assertThat(columnValuePairs.length % 2).isEqualTo(0);

        StringBuilder whereClause = new StringBuilder();
        for (int i = 0; i < columnValuePairs.length; i += 2) {
            String column = (String) columnValuePairs[i];
            if (column.endsWith("id") || column.endsWith("pk") || column.endsWith("ck")) {
                if (whereClause.length() > 0) {
                    whereClause.append(" AND ");
                }
                Object value = columnValuePairs[i + 1];
                if (value instanceof String) {
                    whereClause.append(column).append(" = '").append(value).append("'");
                } else {
                    whereClause.append(column).append(" = ").append(value);
                }
            }
        }

        String selectQuery =
                String.format("SELECT * FROM %s.%s WHERE %s", KEYSPACE, table, whereClause);
        ResultSet rs = cassandraTestEnvironment.executeRequestWithTimeout(selectQuery);
        com.datastax.driver.core.Row row = rs.one();
        assertThat(row).isNotNull();

        for (int i = 0; i < columnValuePairs.length; i += 2) {
            String column = (String) columnValuePairs[i];
            Object expectedValue = columnValuePairs[i + 1];

            if (expectedValue instanceof String) {
                assertThat(row.getString(column)).isEqualTo(expectedValue);
            } else if (expectedValue instanceof Integer) {
                assertThat(row.getInt(column)).isEqualTo(expectedValue);
            }
        }
    }

    @Test
    void testUsingAndIfTogether() throws Exception {
        // Tests combining USING clauses (TTL/TIMESTAMP) with IF clauses in static mode
        // Create a table with version column for LWT
        String createTable =
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s.versioned_table ("
                                + "id text PRIMARY KEY, "
                                + "value text, "
                                + "version int"
                                + ");",
                        KEYSPACE);
        cassandraTestEnvironment.executeRequestWithTimeout(createTable);

        // First insert a row with initial version
        String initialInsert =
                String.format(
                        "INSERT INTO %s.versioned_table (id, value, version) VALUES ('v1', 'initial', 1)",
                        KEYSPACE);
        cassandraTestEnvironment.executeRequestWithTimeout(initialInsert);

        // Test UPDATE with USING TTL (static) and IF condition together
        // Static mode only supports literal USING/IF clauses
        List<Row> updateRows = Collections.singletonList(Row.of("updated", "v1"));

        String updateQuery =
                String.format(
                        "UPDATE %s.versioned_table USING TTL 5 SET value = ? WHERE id = ? IF version = 1",
                        KEYSPACE);

        CqlSinkConfig<Row> updateConfig = CqlSinkConfig.forRow().withQuery(updateQuery);
        executeSinkAndWait(updateRows, updateConfig, ROW_S_S);

        // Verify the update and TTL
        Thread.sleep(1000); // Wait a bit for TTL to be visible

        String selectQuery =
                String.format(
                        "SELECT id, value, version, TTL(value) as ttl_value FROM %s.versioned_table WHERE id = 'v1'",
                        KEYSPACE);
        ResultSet rs = cassandraTestEnvironment.executeRequestWithTimeout(selectQuery);
        com.datastax.driver.core.Row row = rs.one();
        assertThat(row).isNotNull();
        assertThat(row.getString("value")).isEqualTo("updated");
        int ttl = row.getInt("ttl_value");
        assertThat(ttl).isLessThanOrEqualTo(5).isGreaterThan(0);

        // Test static INSERT with USING TTL and TIMESTAMP together (static values)
        List<Row> insertRows = Collections.singletonList(Row.of("v2", "value2"));
        long timestamp = System.currentTimeMillis() * 1000; // microseconds

        String insertQuery =
                String.format(
                        "INSERT INTO %s.versioned_table (id, value) VALUES (?, ?) USING TTL 5 AND TIMESTAMP %d",
                        KEYSPACE, timestamp);

        CqlSinkConfig<Row> insertConfig = CqlSinkConfig.forRow().withQuery(insertQuery);
        executeSinkAndWait(insertRows, insertConfig, ROW_S_S);

        // Verify the insert with TTL and TIMESTAMP
        String verifyQuery =
                String.format(
                        "SELECT id, value, TTL(value) as ttl_value, WRITETIME(value) as write_time FROM %s.versioned_table WHERE id = 'v2'",
                        KEYSPACE);
        ResultSet rs2 = cassandraTestEnvironment.executeRequestWithTimeout(verifyQuery);
        com.datastax.driver.core.Row row2 = rs2.one();
        assertThat(row2).isNotNull();
        assertThat(row2.getString("value")).isEqualTo("value2");

        // TTL should be around 5 seconds
        int ttl2 = row2.getInt("ttl_value");
        assertThat(ttl2).isLessThanOrEqualTo(5).isGreaterThan(0);

        // Timestamp should match what we set
        assertThat(row2.getLong("write_time")).isEqualTo(timestamp);
    }

    // Static inner classes to avoid serialization issues with anonymous classes
    private static class DynamicTableResolver implements TableResolver<Row> {
        private final String keyspace;

        public DynamicTableResolver(String keyspace) {
            this.keyspace = keyspace;
        }

        @Override
        public TableRef resolve(Row record) {
            int id = (int) record.getField(0);
            String tableName = (id % 2 == 0) ? "dynamic_even" : "dynamic_odd";
            return new TableRef(keyspace, tableName);
        }
    }

    private static class DynamicColumnResolver implements ColumnValueResolver<Row> {
        @Override
        public ColumnValueResolver.Kind kind() {
            return ColumnValueResolver.Kind.INSERT;
        }

        @Override
        public ResolvedWrite resolve(Row record) {
            return ResolvedWrite.insert(
                    Arrays.asList("id", "name", "category"),
                    new Object[] {record.getField(0), record.getField(1), record.getField(2)});
        }
    }

    private static class SelectiveColumnResolver implements ColumnValueResolver<Row> {
        @Override
        public ColumnValueResolver.Kind kind() {
            return ColumnValueResolver.Kind.INSERT;
        }

        @Override
        public ResolvedWrite resolve(Row record) {
            int id = (int) record.getField(0);
            String requiredField = (String) record.getField(1);
            boolean includeOptional = (boolean) record.getField(2);

            List<String> columns = new ArrayList<>();
            List<Object> values = new ArrayList<>();

            // Always include required fields
            columns.add("id");
            values.add(id);
            columns.add("required_field");
            values.add(requiredField);

            // Conditionally include optional field
            if (includeOptional) {
                columns.add("optional_field");
                values.add("optional_" + id);
            }

            // Always add computed field
            columns.add("computed_field");
            values.add("computed_" + id);

            return ResolvedWrite.insert(columns, values.toArray());
        }
    }

    private static class FixedTableResolver implements TableResolver<Row> {
        private final String keyspace;
        private final String table;

        public FixedTableResolver(String keyspace, String table) {
            this.keyspace = keyspace;
            this.table = table;
        }

        @Override
        public TableRef resolve(Row record) {
            return new TableRef(keyspace, table);
        }
    }

    private static class InsertAllColumnsResolver implements ColumnValueResolver<Row> {
        @Override
        public ColumnValueResolver.Kind kind() {
            return ColumnValueResolver.Kind.INSERT;
        }

        @Override
        public ResolvedWrite resolve(Row record) {
            return ResolvedWrite.insert(
                    Arrays.asList("id", "data", "priority"),
                    new Object[] {record.getField(0), record.getField(1), record.getField(2)});
        }
    }

    private static class ConsistencyLevelCustomizer implements StatementCustomizer<Row> {
        @Override
        public void apply(Statement statement, Row record) {
            // Set consistency level based on priority field
            String priority = (String) record.getField(2);
            if ("high".equals(priority)) {
                statement.setConsistencyLevel(ConsistencyLevel.QUORUM);
            } else {
                statement.setConsistencyLevel(ConsistencyLevel.ONE);
            }
            // Mark all statements as idempotent
            statement.setIdempotent(true);
        }
    }

    @Test
    void testIgnoreNullOnUpdate() throws Exception {
        // Tests ignoreNullFields behavior on UPDATE to prevent tombstone creation
        // Insert initial data
        String initialInsert =
                String.format(
                        "INSERT INTO %s.test_table (id, name, age) VALUES ('nulltest1', 'John', 30)",
                        KEYSPACE);
        cassandraTestEnvironment.executeRequestWithTimeout(initialInsert);

        // Test UPDATE with null values and ignoreNullFields enabled
        // The null value for age should be ignored
        List<Row> updateRows = Collections.singletonList(Row.of("Jane", null, "nulltest1"));

        String updateQuery =
                String.format("UPDATE %s.test_table SET name = ?, age = ? WHERE id = ?", KEYSPACE);

        CqlSinkConfig<Row> updateConfig =
                CqlSinkConfig.forRow().withQuery(updateQuery).withIgnoreNullFields(true);

        TypeInformation<Row> rowTypeInfo = Types.ROW(Types.STRING, Types.INT, Types.STRING);
        executeSinkAndWait(updateRows, updateConfig, rowTypeInfo);

        // Verify that name was updated but age retained its original value
        String selectQuery =
                String.format(
                        "SELECT id, name, age FROM %s.test_table WHERE id = 'nulltest1'", KEYSPACE);
        ResultSet rs = cassandraTestEnvironment.executeRequestWithTimeout(selectQuery);
        com.datastax.driver.core.Row row = rs.one();
        assertThat(row).isNotNull();
        assertThat(row.getString("name")).isEqualTo("Jane"); // Updated
        assertThat(row.getInt("age")).isEqualTo(30); // Retained original value

        // Test UPDATE with all null values except WHERE clause
        List<Row> allNullUpdateRows = Collections.singletonList(Row.of(null, null, "nulltest1"));

        executeSinkAndWait(allNullUpdateRows, updateConfig, rowTypeInfo);

        // Verify nothing was changed
        rs = cassandraTestEnvironment.executeRequestWithTimeout(selectQuery);
        row = rs.one();
        assertThat(row).isNotNull();
        assertThat(row.getString("name")).isEqualTo("Jane"); // Still Jane
        assertThat(row.getInt("age")).isEqualTo(30); // Still 30

        // Test UPDATE without ignoreNullFields (default behavior)
        List<Row> updateWithNullRows = Collections.singletonList(Row.of("Bob", null, "nulltest1"));

        CqlSinkConfig<Row> noIgnoreConfig =
                CqlSinkConfig.forRow().withQuery(updateQuery).withIgnoreNullFields(false);

        executeSinkAndWait(updateWithNullRows, noIgnoreConfig, rowTypeInfo);

        // Verify that null was actually set (age should be null now)
        rs = cassandraTestEnvironment.executeRequestWithTimeout(selectQuery);
        row = rs.one();
        assertThat(row).isNotNull();
        assertThat(row.getString("name")).isEqualTo("Bob"); // Updated
        assertThat(row.isNull("age")).isTrue(); // Set to null
    }

    @Test
    void testBadQueryTypesFailFast() {
        // Tests that non-INSERT/UPDATE queries (SELECT, DELETE, TRUNCATE) are rejected in static
        // mode
        // Test that SELECT queries are rejected
        List<Row> rows = Collections.singletonList(Row.of("test1"));

        String selectQuery = String.format("SELECT * FROM %s.test_table WHERE id = ?", KEYSPACE);
        CqlSinkConfig<Row> selectConfig = CqlSinkConfig.forRow().withQuery(selectQuery);

        assertThatThrownBy(() -> executeSinkAndWait(rows, selectConfig, ROW_S))
                .isInstanceOf(org.apache.flink.runtime.client.JobExecutionException.class)
                .hasStackTraceContaining("Static mode only supports INSERT and UPDATE queries");

        // Test that DELETE queries are rejected
        String deleteQuery = String.format("DELETE FROM %s.test_table WHERE id = ?", KEYSPACE);
        CqlSinkConfig<Row> deleteConfig = CqlSinkConfig.forRow().withQuery(deleteQuery);

        assertThatThrownBy(() -> executeSinkAndWait(rows, deleteConfig, ROW_S))
                .isInstanceOf(org.apache.flink.runtime.client.JobExecutionException.class)
                .hasStackTraceContaining("Static mode only supports INSERT and UPDATE queries");

        // Test that TRUNCATE queries are rejected
        String truncateQuery = String.format("TRUNCATE %s.test_table", KEYSPACE);
        CqlSinkConfig<Row> truncateConfig = CqlSinkConfig.forRow().withQuery(truncateQuery);

        // Use a dummy row since empty collections are not allowed
        assertThatThrownBy(() -> executeSinkAndWait(rows, truncateConfig, ROW_S))
                .isInstanceOf(org.apache.flink.runtime.client.JobExecutionException.class)
                .hasStackTraceContaining("Static mode only supports INSERT and UPDATE queries");
    }

    @Test
    void testDynamicModeWithTableResolver() throws Exception {
        // Tests dynamic mode routing records to different tables based on id parity
        // Create two tables for dynamic routing
        String createEvenTable =
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s.dynamic_even ("
                                + "id int PRIMARY KEY, "
                                + "name text, "
                                + "category text"
                                + ");",
                        KEYSPACE);
        cassandraTestEnvironment.executeRequestWithTimeout(createEvenTable);

        String createOddTable =
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s.dynamic_odd ("
                                + "id int PRIMARY KEY, "
                                + "name text, "
                                + "category text"
                                + ");",
                        KEYSPACE);
        cassandraTestEnvironment.executeRequestWithTimeout(createOddTable);

        // Create dynamic TableResolver that routes based on id
        TableResolver<Row> tableResolver = new DynamicTableResolver(KEYSPACE);

        // Create ColumnValueResolver for INSERT
        ColumnValueResolver<Row> columnResolver = new DynamicColumnResolver();

        // Create SinkPluggable
        SinkPluggable<Row> pluggable =
                SinkPluggable.<Row>builder()
                        .withTableResolver(tableResolver)
                        .withColumnValueResolver(columnResolver)
                        .build();

        // Test data with mixed even/odd ids
        List<Row> rows = new ArrayList<>();
        rows.add(Row.of(1, "odd1", "cat1"));
        rows.add(Row.of(2, "even1", "cat2"));
        rows.add(Row.of(3, "odd2", "cat3"));
        rows.add(Row.of(4, "even2", "cat4"));

        CqlSinkConfig<Row> config = CqlSinkConfig.forRow().withPluggable(pluggable);

        TypeInformation<Row> rowTypeInfo = Types.ROW(Types.INT, Types.STRING, Types.STRING);
        executeSinkAndWait(rows, config, rowTypeInfo);

        // Verify odd table
        String selectOdd = String.format("SELECT * FROM %s.dynamic_odd", KEYSPACE);
        ResultSet rsOdd = cassandraTestEnvironment.executeRequestWithTimeout(selectOdd);
        List<com.datastax.driver.core.Row> oddRows = rsOdd.all();
        assertThat(oddRows).hasSize(2);
        assertThat(
                        oddRows.stream()
                                .map(r -> r.getInt("id"))
                                .collect(java.util.stream.Collectors.toList()))
                .containsExactlyInAnyOrder(1, 3);

        // Verify even table
        String selectEven = String.format("SELECT * FROM %s.dynamic_even", KEYSPACE);
        ResultSet rsEven = cassandraTestEnvironment.executeRequestWithTimeout(selectEven);
        List<com.datastax.driver.core.Row> evenRows = rsEven.all();
        assertThat(evenRows).hasSize(2);
        assertThat(
                        evenRows.stream()
                                .map(r -> r.getInt("id"))
                                .collect(java.util.stream.Collectors.toList()))
                .containsExactlyInAnyOrder(2, 4);
    }

    @Test
    void testDynamicModeWithColumnValueResolver() throws Exception {
        // Tests dynamic mode with selective column inclusion based on record content
        // Create table for selective column insertion test
        String createTable =
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s.selective_columns ("
                                + "id int PRIMARY KEY, "
                                + "required_field text, "
                                + "optional_field text, "
                                + "computed_field text"
                                + ");",
                        KEYSPACE);
        cassandraTestEnvironment.executeRequestWithTimeout(createTable);

        // Create ColumnValueResolver that selectively includes columns
        ColumnValueResolver<Row> selectiveResolver = new SelectiveColumnResolver();

        // Create TableResolver for fixed table
        TableResolver<Row> fixedTableResolver =
                new FixedTableResolver(KEYSPACE, "selective_columns");

        // Create SinkPluggable with TableResolver and ColumnValueResolver
        SinkPluggable<Row> pluggable =
                SinkPluggable.<Row>builder()
                        .withTableResolver(fixedTableResolver)
                        .withColumnValueResolver(selectiveResolver)
                        .build();

        // Test data: (id, required_field, should_include_optional)
        List<Row> rows = new ArrayList<>();
        rows.add(Row.of(1, "req1", true)); // Include optional field
        rows.add(Row.of(2, "req2", false)); // Skip optional field
        rows.add(Row.of(3, "req3", true)); // Include optional field

        CqlSinkConfig<Row> config = CqlSinkConfig.forRow().withPluggable(pluggable);

        TypeInformation<Row> rowTypeInfo = Types.ROW(Types.INT, Types.STRING, Types.BOOLEAN);
        executeSinkAndWait(rows, config, rowTypeInfo);

        // Verify records were inserted with selective columns
        String selectQuery = String.format("SELECT * FROM %s.selective_columns", KEYSPACE);
        ResultSet rs = cassandraTestEnvironment.executeRequestWithTimeout(selectQuery);
        List<com.datastax.driver.core.Row> results = rs.all();
        assertThat(results).hasSize(3);

        // Check each record
        for (com.datastax.driver.core.Row row : results) {
            int id = row.getInt("id");
            assertThat(row.getString("required_field")).isEqualTo("req" + id);
            assertThat(row.getString("computed_field")).isEqualTo("computed_" + id);

            if (id == 1 || id == 3) {
                // Should have optional field
                assertThat(row.getString("optional_field")).isEqualTo("optional_" + id);
            } else {
                // Should not have optional field (will be null)
                assertThat(row.getString("optional_field")).isNull();
            }
        }
    }

    @Test
    void testStatementCustomizer() throws Exception {
        // Tests StatementCustomizer for setting consistency levels and idempotency based on record
        // content
        // Create table for statement customization test
        String createTable =
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s.customized_statements ("
                                + "id int PRIMARY KEY, "
                                + "data text, "
                                + "priority text"
                                + ");",
                        KEYSPACE);
        cassandraTestEnvironment.executeRequestWithTimeout(createTable);

        // Test with dynamic mode and statement customizer
        List<Row> rows = new ArrayList<>();
        rows.add(Row.of(1, "data1", "high"));
        rows.add(Row.of(2, "data2", "low"));
        rows.add(Row.of(3, "data3", "high"));

        // Create TableResolver for fixed table
        TableResolver<Row> fixedTableResolver =
                new FixedTableResolver(KEYSPACE, "customized_statements");

        // Create ColumnValueResolver for INSERT
        ColumnValueResolver<Row> insertResolver = new InsertAllColumnsResolver();

        // Create SinkPluggable with StatementCustomizer
        SinkPluggable<Row> pluggable =
                SinkPluggable.<Row>builder()
                        .withTableResolver(fixedTableResolver)
                        .withColumnValueResolver(insertResolver)
                        .withStatementCustomizer(new ConsistencyLevelCustomizer())
                        .build();

        CqlSinkConfig<Row> config = CqlSinkConfig.forRow().withPluggable(pluggable);

        TypeInformation<Row> rowTypeInfo = Types.ROW(Types.INT, Types.STRING, Types.STRING);
        executeSinkAndWait(rows, config, rowTypeInfo);

        // Verify records were inserted
        String selectQuery = String.format("SELECT * FROM %s.customized_statements", KEYSPACE);
        ResultSet rs = cassandraTestEnvironment.executeRequestWithTimeout(selectQuery);
        List<com.datastax.driver.core.Row> results = rs.all();
        assertThat(results).hasSize(3);

        // Verify data is correct
        for (com.datastax.driver.core.Row row : results) {
            int id = row.getInt("id");
            assertThat(row.getString("data")).isEqualTo("data" + id);
            String expectedPriority = (id == 2) ? "low" : "high";
            assertThat(row.getString("priority")).isEqualTo(expectedPriority);
        }
    }

    @Test
    void testRequestConfiguration() throws Exception {
        // Tests RequestConfiguration with limited concurrent requests and retry settings
        // Create table for request configuration test
        String createTable =
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s.request_config_test ("
                                + "id int PRIMARY KEY, "
                                + "data text"
                                + ");",
                        KEYSPACE);
        cassandraTestEnvironment.executeRequestWithTimeout(createTable);

        // Create a larger dataset to test concurrent request limiting
        List<Row> rows = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            rows.add(Row.of(i, "data" + i));
        }

        String insertQuery =
                String.format(
                        "INSERT INTO %s.request_config_test (id, data) VALUES (?, ?)", KEYSPACE);

        // Configure with limited concurrent requests and retries
        RequestConfiguration requestConfig =
                RequestConfiguration.builder()
                        .setMaxConcurrentRequests(5) // Limit concurrent requests to 5
                        .setMaxRetries(2) // Retry failed requests up to 2 times
                        .build();

        CqlSinkConfig<Row> config = CqlSinkConfig.forRow().withQuery(insertQuery);

        executeSinkAndWaitWithRequestConfig(rows, config, requestConfig, ROW_I_S);

        // Verify all records were inserted
        String selectQuery = String.format("SELECT * FROM %s.request_config_test", KEYSPACE);
        ResultSet rs = cassandraTestEnvironment.executeRequestWithTimeout(selectQuery);
        List<com.datastax.driver.core.Row> results = rs.all();
        assertThat(results).hasSize(100);

        // Verify data integrity
        for (com.datastax.driver.core.Row row : results) {
            int id = row.getInt("id");
            assertThat(row.getString("data")).isEqualTo("data" + id);
        }
    }

    // Static CqlClauseResolver implementations for testing
    public static class DynamicTTLResolver implements CqlClauseResolver<Row> {
        @Override
        public ClauseBindings applyTo(Insert insert, Row record) {
            insert.using(ttl(bindMarker()));
            return new ClauseBindings(new Object[] {record.getField(2)}, null);
        }
    }

    public static class DynamicUpdateResolver implements CqlClauseResolver<Row> {
        @Override
        public ClauseBindings applyTo(Update update, Row record) {
            update.using(ttl(bindMarker())).onlyIf(eq("version", bindMarker()));
            return new ClauseBindings(
                    new Object[] {record.getField(2)}, // TTL only (no timestamp for LWT)
                    new Object[] {record.getField(3)} // IF version
                    );
        }
    }

    // Simple TableResolver for single table tests
    public static class SingleTableResolver implements TableResolver<Row> {
        private final String tableName;

        public SingleTableResolver(String tableName) {
            this.tableName = tableName;
        }

        @Override
        public TableRef resolve(Row record) {
            return new TableRef(KEYSPACE, tableName);
        }
    }

    // Simple ColumnValueResolver for dynamic TTL tests
    public static class DynamicTTLColumnResolver implements ColumnValueResolver<Row> {
        @Override
        public Kind kind() {
            return Kind.INSERT;
        }

        @Override
        public ResolvedWrite resolve(Row record) {
            String id = (String) record.getField(0);
            String name = (String) record.getField(1);
            return ResolvedWrite.insert(Arrays.asList("id", "name"), new Object[] {id, name});
        }
    }

    public static class DynamicUpdateColumnResolver implements ColumnValueResolver<Row> {
        @Override
        public Kind kind() {
            return Kind.UPDATE;
        }

        @Override
        public ResolvedWrite resolve(Row record) {
            String id = (String) record.getField(0);
            String name = (String) record.getField(1);
            return ResolvedWrite.update(
                    Arrays.asList("name"), new Object[] {name},
                    Arrays.asList("id"), new Object[] {id});
        }
    }

    @Test
    void testCqlClauseResolverDynamicInsertTTL() throws Exception {
        // Tests CqlClauseResolver for dynamic per-record TTL values in INSERT statements
        // Create table for dynamic TTL test
        String createTable =
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s.dynamic_ttl ("
                                + "id text PRIMARY KEY, "
                                + "name text"
                                + ");",
                        KEYSPACE);
        cassandraTestEnvironment.executeRequestWithTimeout(createTable);

        // Test records with different TTL values per record
        List<Row> rows =
                Arrays.asList(
                        Row.of("ttl1", "first", 5), // 5 second TTL
                        Row.of("ttl2", "second", 5) // 5 second TTL
                        );

        // Create dynamic SinkPluggable with CqlClauseResolver
        SinkPluggable<Row> pluggable =
                SinkPluggable.<Row>builder()
                        .withTableResolver(new SingleTableResolver("dynamic_ttl"))
                        .withColumnValueResolver(new DynamicTTLColumnResolver())
                        .withCqlClauseResolver(new DynamicTTLResolver())
                        .build();

        CqlSinkConfig<Row> config = CqlSinkConfig.forRow().withPluggable(pluggable);

        TypeInformation<Row> rowTypeInfo = Types.ROW(Types.STRING, Types.STRING, Types.INT);
        executeSinkAndWait(rows, config, rowTypeInfo);

        // Verify both records were inserted with correct TTL
        String selectQuery =
                String.format(
                        "SELECT id, name, TTL(name) as ttl_value FROM %s.dynamic_ttl", KEYSPACE);
        ResultSet rs = cassandraTestEnvironment.executeRequestWithTimeout(selectQuery);
        List<com.datastax.driver.core.Row> results = rs.all();
        assertThat(results).hasSize(2);

        // Verify TTL values are approximately correct (allowing for execution time)
        for (com.datastax.driver.core.Row row : results) {
            String id = row.getString("id");
            int ttlValue = row.getInt("ttl_value");
            if ("ttl1".equals(id) || "ttl2".equals(id)) {
                assertThat(ttlValue).isLessThanOrEqualTo(5).isGreaterThan(0);
            }
        }
    }

    @Test
    void testCqlClauseResolverDynamicUpdateWithLWT() throws Exception {
        // Tests CqlClauseResolver for dynamic UPDATE with TTL and IF conditions (LWT)
        // Create table with version column for LWT
        String createTable =
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s.versioned_data ("
                                + "id text PRIMARY KEY, "
                                + "name text, "
                                + "version int"
                                + ");",
                        KEYSPACE);
        cassandraTestEnvironment.executeRequestWithTimeout(createTable);

        // Insert initial data
        String initialInsert =
                String.format(
                        "INSERT INTO %s.versioned_data (id, name, version) VALUES ('ver1', 'initial', 1)",
                        KEYSPACE);
        cassandraTestEnvironment.executeRequestWithTimeout(initialInsert);

        // Test record with TTL and version condition (no timestamp for LWT)
        List<Row> rows =
                Collections.singletonList(
                        Row.of("ver1", "updated", 5, 1) // id, name, ttl, expected_version
                        );

        // Create dynamic SinkPluggable with CqlClauseResolver for UPDATE
        SinkPluggable<Row> pluggable =
                SinkPluggable.<Row>builder()
                        .withTableResolver(new SingleTableResolver("versioned_data"))
                        .withColumnValueResolver(new DynamicUpdateColumnResolver())
                        .withCqlClauseResolver(new DynamicUpdateResolver())
                        .build();

        CqlSinkConfig<Row> config = CqlSinkConfig.forRow().withPluggable(pluggable);

        TypeInformation<Row> rowTypeInfo =
                Types.ROW(Types.STRING, Types.STRING, Types.INT, Types.INT);
        executeSinkAndWait(rows, config, rowTypeInfo);

        // Verify the update succeeded with applied=true
        String selectQuery =
                String.format(
                        "SELECT id, name, version, TTL(name) as ttl_value FROM %s.versioned_data WHERE id = 'ver1'",
                        KEYSPACE);
        ResultSet rs = cassandraTestEnvironment.executeRequestWithTimeout(selectQuery);
        com.datastax.driver.core.Row row = rs.one();
        assertThat(row).isNotNull();
        assertThat(row.getString("name")).isEqualTo("updated");
        assertThat(row.getInt("version")).isEqualTo(1);

        // TTL should be approximately 5 seconds
        int ttl = row.getInt("ttl_value");
        assertThat(ttl).isLessThanOrEqualTo(5).isGreaterThan(0);
    }

    @Test
    void testTTLVerificationWithPreciseExpiration() throws Exception {
        // Tests precise TTL functionality with verification of record expiration timing
        // Create table for TTL precision test
        String createTable =
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s.ttl_precision ("
                                + "id text PRIMARY KEY, "
                                + "data text"
                                + ");",
                        KEYSPACE);
        cassandraTestEnvironment.executeRequestWithTimeout(createTable);

        // Insert record with precise TTL
        List<Row> rows = Collections.singletonList(Row.of("ttl-test", "expires-fast"));

        String insertQuery =
                String.format(
                        "INSERT INTO %s.ttl_precision (id, data) VALUES (?, ?) USING TTL 5",
                        KEYSPACE);

        CqlSinkConfig<Row> config = CqlSinkConfig.forRow().withQuery(insertQuery);
        executeSinkAndWait(rows, config, ROW_S_S);

        // Immediately verify TTL is set correctly (should be close to 5 seconds)
        String selectQuery =
                String.format(
                        "SELECT id, data, TTL(data) as ttl_remaining FROM %s.ttl_precision WHERE id = 'ttl-test'",
                        KEYSPACE);
        ResultSet rs = cassandraTestEnvironment.executeRequestWithTimeout(selectQuery);
        com.datastax.driver.core.Row row = rs.one();
        assertThat(row).isNotNull();
        assertThat(row.getString("data")).isEqualTo("expires-fast");

        // TTL should be 5 or slightly less (due to execution time)
        int ttl = row.getInt("ttl_remaining");
        assertThat(ttl).isLessThanOrEqualTo(5).isGreaterThan(0);

        // Wait for expiration (6 seconds to be safe)
        Thread.sleep(6000);

        // Verify record has expired
        rs = cassandraTestEnvironment.executeRequestWithTimeout(selectQuery);
        row = rs.one();
        assertThat(row).isNull(); // Should be null after TTL expiration
    }
}
