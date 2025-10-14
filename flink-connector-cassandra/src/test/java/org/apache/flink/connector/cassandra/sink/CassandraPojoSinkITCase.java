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
import org.apache.flink.connector.cassandra.CassandraTestEnvironment;
import org.apache.flink.connector.cassandra.sink.config.PojoSinkConfig;
import org.apache.flink.connector.cassandra.sink.config.write.RequestConfiguration;
import org.apache.flink.connector.testframe.environment.MiniClusterTestEnvironment;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.streaming.connectors.cassandra.MapperOptions;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.connector.cassandra.CassandraTestEnvironment.KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Integration tests for NEW CassandraSink V2 with POJO types using PojoSinkConfig. */
@Testcontainers
class CassandraPojoSinkITCase {

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

    @TestEnv MiniClusterTestEnvironment flinkTestEnvironment = new MiniClusterTestEnvironment();

    private static final CassandraTestEnvironment cassandraTestEnvironment =
            new CassandraTestEnvironment(false);

    private static Integer port;
    private static String host;

    @BeforeAll
    static void setupCassandra() throws Exception {
        cassandraTestEnvironment.startUp();
        createTestTable();
        port = cassandraTestEnvironment.getPort();
        host = cassandraTestEnvironment.getHost();
    }

    @AfterAll
    static void tearDownCassandra() throws Exception {
        dropTestTable();
        cassandraTestEnvironment.tearDown();
    }

    @BeforeEach
    void clearTable() {
        String truncateQuery = String.format("TRUNCATE %s.test_users;", KEYSPACE);
        try {
            cassandraTestEnvironment.executeRequestWithTimeout(truncateQuery);
        } catch (Exception ignored) {

        }
    }

    private static void createTestTable() {
        String createQuery =
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s.test_users ("
                                + "id text PRIMARY KEY, "
                                + "name text, "
                                + "age int"
                                + ");",
                        KEYSPACE);
        cassandraTestEnvironment.executeRequestWithTimeout(createQuery);
    }

    private static void dropTestTable() {
        String dropQuery = String.format("DROP TABLE IF EXISTS %s.test_users;", KEYSPACE);
        try {
            cassandraTestEnvironment.executeRequestWithTimeout(dropQuery);
        } catch (Exception ignored) {

        }
    }

    @Test
    void testNewPojoSinkWithPojoSinkConfig() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Create test data
        List<TestUser> users = new ArrayList<>();
        users.add(new TestUser("user1", "Alice", 25));
        users.add(new TestUser("user2", "Bob", 30));
        users.add(new TestUser("user3", "Charlie", 35));

        // Create data stream
        DataStream<TestUser> dataStream =
                env.fromCollection(users, TypeInformation.of(TestUser.class));

        // Create PojoSinkConfig for the NEW sink
        PojoSinkConfig<TestUser> config = new PojoSinkConfig<>(TestUser.class, KEYSPACE, null);

        // Build the NEW CassandraSink V2
        CassandraSink<TestUser> sink =
                CassandraSinkBuilder.newBuilder(config)
                        .setClusterBuilder(new TestClusterBuilder(host, port))
                        .build();

        // Add the sink
        dataStream.sinkTo(sink);

        env.execute();

        // Verify data was inserted
        List<Row> rows = readAllRows();
        assertThat(rows).hasSize(3);

        // Verify each user
        for (TestUser user : users) {
            Row row = readUserById(user.getId());
            assertThat(row).isNotNull();
            assertThat(row.getString("name")).isEqualTo(user.getName());
            assertThat(row.getInt("age")).isEqualTo(user.getAge());
        }
    }

    @Test
    void testNewPojoSinkWithNullValues() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Create test data with some nulls
        List<TestUser> users = new ArrayList<>();
        TestUser userWithNullName = new TestUser("user4", null, 40);
        users.add(userWithNullName);

        DataStream<TestUser> dataStream =
                env.fromCollection(users, TypeInformation.of(TestUser.class));

        // Create PojoSinkConfig for the NEW sink
        PojoSinkConfig<TestUser> config = new PojoSinkConfig<>(TestUser.class, KEYSPACE, null);

        // Build the NEW CassandraSink V2
        CassandraSink<TestUser> sink =
                CassandraSinkBuilder.newBuilder(config)
                        .setClusterBuilder(new TestClusterBuilder(host, port))
                        .build();

        dataStream.sinkTo(sink);
        env.execute();

        // Verify data
        Row row = readUserById("user4");
        assertThat(row).isNotNull();
        assertThat(row.isNull("name")).isTrue();
        assertThat(row.getInt("age")).isEqualTo(40);
    }

    @Test
    void testNewPojoSinkWithMapperOptions() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<TestUser> users = new ArrayList<>();
        users.add(new TestUser("user5", "Eve", 28));
        users.add(new TestUser("user6", "Frank", 32));

        DataStream<TestUser> dataStream =
                env.fromCollection(users, TypeInformation.of(TestUser.class));

        // Create MapperOptions with TTL and consistency level
        MapperOptions mapperOptions =
                () ->
                        new Mapper.Option[] {
                            Mapper.Option.ttl(5), // 5 seconds TTL for testing
                            Mapper.Option.consistencyLevel(ConsistencyLevel.QUORUM)
                        };

        // Create PojoSinkConfig with mapper options
        PojoSinkConfig<TestUser> config =
                new PojoSinkConfig<>(TestUser.class, KEYSPACE, mapperOptions);

        // Build the NEW CassandraSink V2
        CassandraSink<TestUser> sink =
                CassandraSinkBuilder.newBuilder(config)
                        .setClusterBuilder(new TestClusterBuilder(host, port))
                        .build();

        dataStream.sinkTo(sink);
        env.execute();

        // Verify exact count of records
        List<Row> rows = readAllRows();
        assertThat(rows).hasSize(2);

        // Verify TTL was applied
        String ttlQuery =
                String.format(
                        "SELECT TTL(name) AS name_ttl, TTL(age) AS age_ttl FROM %s.test_users WHERE id='user5';",
                        KEYSPACE);
        ResultSet ttlRs = cassandraTestEnvironment.executeRequestWithTimeout(ttlQuery);
        Row ttlRow = ttlRs.one();
        assertThat(ttlRow).isNotNull();
        assertThat(ttlRow.getInt("name_ttl"))
                .isBetween(1, 6); // Wider window to prevent CI flakiness
        assertThat(ttlRow.getInt("age_ttl")).isBetween(1, 6);

        Row row = readUserById("user5");
        assertThat(row).isNotNull();
        assertThat(row.getString("name")).isEqualTo("Eve");
    }

    @Test
    void testNewPojoSinkWithLargerBatch() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // Create larger batch of data
        List<TestUser> users = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            users.add(new TestUser("id" + i, "User" + i, 20 + (i % 30)));
        }

        DataStream<TestUser> dataStream =
                env.fromCollection(users, TypeInformation.of(TestUser.class));

        // Create PojoSinkConfig for the NEW sink
        PojoSinkConfig<TestUser> config = new PojoSinkConfig<>(TestUser.class, KEYSPACE, null);

        // Build the NEW CassandraSink V2
        CassandraSink<TestUser> sink =
                CassandraSinkBuilder.newBuilder(config)
                        .setClusterBuilder(new TestClusterBuilder(host, port))
                        .build();

        dataStream.sinkTo(sink);
        env.execute();

        // Verify all records were inserted
        List<Row> rows = readAllRows();
        assertThat(rows).hasSize(50);
    }

    @Test
    void testTimestampMapperOption() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Custom timestamp in microseconds
        long customMicros = System.currentTimeMillis() * 1000L + 123;

        List<TestUser> users = new ArrayList<>();
        users.add(new TestUser("ts_user", "Alice", 25));

        DataStream<TestUser> dataStream =
                env.fromCollection(users, TypeInformation.of(TestUser.class));

        // Create MapperOptions with custom timestamp
        MapperOptions timestampOptions =
                () -> new Mapper.Option[] {Mapper.Option.timestamp(customMicros)};

        PojoSinkConfig<TestUser> config =
                new PojoSinkConfig<>(TestUser.class, KEYSPACE, timestampOptions);

        CassandraSink<TestUser> sink =
                CassandraSinkBuilder.newBuilder(config)
                        .setClusterBuilder(new TestClusterBuilder(host, port))
                        .build();

        dataStream.sinkTo(sink);
        env.execute();

        // Verify the custom timestamp was applied
        String writetimeQuery =
                String.format(
                        "SELECT writetime(name) AS wt FROM %s.test_users WHERE id='ts_user';",
                        KEYSPACE);
        ResultSet rs = cassandraTestEnvironment.executeRequestWithTimeout(writetimeQuery);
        Row row = rs.one();
        assertThat(row).isNotNull();
        assertThat(row.getLong("wt")).isEqualTo(customMicros);
    }

    @Test
    void testConcurrencyAndBackpressure() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4); // High parallelism

        // Create a large batch of records
        List<TestUser> users = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            users.add(new TestUser("concurrent_" + i, "User" + i, 20 + (i % 50)));
        }

        DataStream<TestUser> dataStream =
                env.fromCollection(users, TypeInformation.of(TestUser.class));

        PojoSinkConfig<TestUser> config = new PojoSinkConfig<>(TestUser.class, KEYSPACE, null);

        // Configure with limited concurrency to exercise permit/flush path
        RequestConfiguration requestConfig =
                RequestConfiguration.builder()
                        .setMaxConcurrentRequests(2) // Very low to force queuing
                        .build();

        CassandraSink<TestUser> sink =
                CassandraSinkBuilder.newBuilder(config)
                        .setClusterBuilder(new TestClusterBuilder(host, port))
                        .setRequestConfiguration(requestConfig)
                        .build();

        dataStream.sinkTo(sink);
        env.execute();

        // Verify all records were written despite concurrency limits
        // Count manually since LIKE with COUNT can be problematic
        int count = 0;
        for (int i = 0; i < 500; i++) {
            Row row = readUserById("concurrent_" + i);
            if (row != null) {
                count++;
            }
        }
        assertThat(count).isEqualTo(500).as("All 500 records should be written");
    }

    /** Simple test POJO with Cassandra annotations. */
    @Table(keyspace = KEYSPACE, name = "test_users")
    public static class TestUser implements Serializable {
        @Column(name = "id")
        private String id;

        @Column(name = "name")
        private String name;

        @Column(name = "age")
        private Integer age;

        public TestUser() {
            // Default constructor required for DataStax mapper
        }

        public TestUser(String id, String name, Integer age) {
            this.id = id;
            this.name = name;
            this.age = age;
        }

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

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }
    }

    @Test
    void testTTLExpiry() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<TestUser> users = new ArrayList<>();
        users.add(new TestUser("ttl_user", "Temporary", 100));

        DataStream<TestUser> dataStream =
                env.fromCollection(users, TypeInformation.of(TestUser.class));

        // Create MapperOptions with short TTL (long enough for initial read)
        MapperOptions mapperOptions = () -> new Mapper.Option[] {Mapper.Option.ttl(5)};

        PojoSinkConfig<TestUser> config =
                new PojoSinkConfig<>(TestUser.class, KEYSPACE, mapperOptions);

        CassandraSink<TestUser> sink =
                CassandraSinkBuilder.newBuilder(config)
                        .setClusterBuilder(new TestClusterBuilder(host, port))
                        .build();

        dataStream.sinkTo(sink);
        env.execute();

        // Wait for record to be written with retry mechanism
        Row row = null;
        long startWait = System.currentTimeMillis();
        while (row == null && (System.currentTimeMillis() - startWait) < 2000) { // 2 second timeout
            row = readUserById("ttl_user");
            if (row == null) {
                Thread.sleep(50); // Wait 50ms before retry
            }
        }
        assertThat(row).isNotNull();
        assertThat(row.getString("name")).isEqualTo("Temporary");
        long startTime = System.currentTimeMillis();
        boolean expired = false;
        while (System.currentTimeMillis() - startTime
                < 8000) { // 8 second timeout (longer than 5s TTL)
            Thread.sleep(500);
            Row expiredRow = readUserById("ttl_user");
            if (expiredRow == null) {
                expired = true;
                break;
            }
        }
        assertThat(expired).isTrue().as("Record should have expired after TTL");
    }

    @Test
    void testSaveNullFieldsFalse() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // First, write a complete record
        List<TestUser> users = new ArrayList<>();
        users.add(new TestUser("null_test", "Alice", 25));

        DataStream<TestUser> dataStream =
                env.fromCollection(users, TypeInformation.of(TestUser.class));

        PojoSinkConfig<TestUser> config = new PojoSinkConfig<>(TestUser.class, KEYSPACE, null);

        CassandraSink<TestUser> sink =
                CassandraSinkBuilder.newBuilder(config)
                        .setClusterBuilder(new TestClusterBuilder(host, port))
                        .build();

        dataStream.sinkTo(sink);
        env.execute();

        // Verify initial state
        Row row = readUserById("null_test");
        assertThat(row.getString("name")).isEqualTo("Alice");

        // Now update with null name but saveNullFields(false)
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<TestUser> updateUsers = new ArrayList<>();
        updateUsers.add(new TestUser("null_test", null, 26));

        DataStream<TestUser> updateStream =
                env.fromCollection(updateUsers, TypeInformation.of(TestUser.class));

        MapperOptions noNullsOptions =
                () -> new Mapper.Option[] {Mapper.Option.saveNullFields(false)};

        PojoSinkConfig<TestUser> noNullsConfig =
                new PojoSinkConfig<>(TestUser.class, KEYSPACE, noNullsOptions);

        CassandraSink<TestUser> noNullsSink =
                CassandraSinkBuilder.newBuilder(noNullsConfig)
                        .setClusterBuilder(new TestClusterBuilder(host, port))
                        .build();

        updateStream.sinkTo(noNullsSink);
        env.execute();

        // Verify name was NOT overwritten with null
        Row updatedRow = readUserById("null_test");
        assertThat(updatedRow.getString("name")).isEqualTo("Alice"); // Still Alice
        assertThat(updatedRow.getInt("age")).isEqualTo(26); // Age updated
    }

    @Test
    void testSaveNullFieldsDefault() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // First, write a complete record
        List<TestUser> users = new ArrayList<>();
        users.add(new TestUser("null_default", "Bob", 30));

        DataStream<TestUser> dataStream =
                env.fromCollection(users, TypeInformation.of(TestUser.class));

        PojoSinkConfig<TestUser> config = new PojoSinkConfig<>(TestUser.class, KEYSPACE, null);

        CassandraSink<TestUser> sink =
                CassandraSinkBuilder.newBuilder(config)
                        .setClusterBuilder(new TestClusterBuilder(host, port))
                        .build();

        dataStream.sinkTo(sink);
        env.execute();

        // Verify initial state
        Row row = readUserById("null_default");
        assertThat(row.getString("name")).isEqualTo("Bob");

        // Now update with null name using default behavior
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<TestUser> updateUsers = new ArrayList<>();
        updateUsers.add(new TestUser("null_default", null, 31));

        DataStream<TestUser> updateStream =
                env.fromCollection(updateUsers, TypeInformation.of(TestUser.class));

        PojoSinkConfig<TestUser> updateConfig =
                new PojoSinkConfig<>(TestUser.class, KEYSPACE, null);

        CassandraSink<TestUser> updateSink =
                CassandraSinkBuilder.newBuilder(updateConfig)
                        .setClusterBuilder(new TestClusterBuilder(host, port))
                        .build();

        updateStream.sinkTo(updateSink);
        env.execute();

        // Verify name WAS overwritten with null (tombstone)
        Row updatedRow = readUserById("null_default");
        assertThat(updatedRow.isNull("name")).isTrue(); // Name is null
        assertThat(updatedRow.getInt("age")).isEqualTo(31); // Age updated
    }

    @Test
    void testUpsertSemantics() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Insert initial record
        List<TestUser> users = new ArrayList<>();
        users.add(new TestUser("upsert_test", "Bob", 30));

        DataStream<TestUser> dataStream =
                env.fromCollection(users, TypeInformation.of(TestUser.class));

        PojoSinkConfig<TestUser> config = new PojoSinkConfig<>(TestUser.class, KEYSPACE, null);

        CassandraSink<TestUser> sink =
                CassandraSinkBuilder.newBuilder(config)
                        .setClusterBuilder(new TestClusterBuilder(host, port))
                        .build();

        dataStream.sinkTo(sink);
        env.execute();

        // Verify initial state
        Row row = readUserById("upsert_test");
        assertThat(row.getString("name")).isEqualTo("Bob");
        assertThat(row.getInt("age")).isEqualTo(30);

        // Update same record
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<TestUser> updateUsers = new ArrayList<>();
        updateUsers.add(new TestUser("upsert_test", "Bobby", 31));

        DataStream<TestUser> updateStream =
                env.fromCollection(updateUsers, TypeInformation.of(TestUser.class));

        CassandraSink<TestUser> updateSink =
                CassandraSinkBuilder.newBuilder(config)
                        .setClusterBuilder(new TestClusterBuilder(host, port))
                        .build();

        updateStream.sinkTo(updateSink);
        env.execute();

        // Verify update and check writetime monotonicity
        Row updatedRow = readUserById("upsert_test");
        assertThat(updatedRow.getString("name")).isEqualTo("Bobby");
        assertThat(updatedRow.getInt("age")).isEqualTo(31);

        // Verify writetime increased (proving actual update happened)
        String writetimeQuery =
                String.format(
                        "SELECT writetime(name) AS wt FROM %s.test_users WHERE id='upsert_test';",
                        KEYSPACE);
        ResultSet firstRs = cassandraTestEnvironment.executeRequestWithTimeout(writetimeQuery);
        long firstWritetime = firstRs.one().getLong("wt");

        // Do another update to verify monotonicity
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<TestUser> finalUpdate = new ArrayList<>();
        finalUpdate.add(new TestUser("upsert_test", "Robert", 32));
        DataStream<TestUser> finalStream =
                env.fromCollection(finalUpdate, TypeInformation.of(TestUser.class));
        CassandraSink<TestUser> finalSink =
                CassandraSinkBuilder.newBuilder(config)
                        .setClusterBuilder(new TestClusterBuilder(host, port))
                        .build();
        finalStream.sinkTo(finalSink);
        env.execute();

        ResultSet secondRs = cassandraTestEnvironment.executeRequestWithTimeout(writetimeQuery);
        long secondWritetime = secondRs.one().getLong("wt");
        assertThat(secondWritetime)
                .isGreaterThan(firstWritetime)
                .as("Writetime should increase on update");
    }

    @Test
    void testNullPartitionKeyFailure() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Create a record with null partition key
        List<TestUser> users = new ArrayList<>();
        users.add(new TestUser(null, "Invalid", 99));

        DataStream<TestUser> dataStream =
                env.fromCollection(users, TypeInformation.of(TestUser.class));

        PojoSinkConfig<TestUser> config = new PojoSinkConfig<>(TestUser.class, KEYSPACE, null);

        CassandraSink<TestUser> sink =
                CassandraSinkBuilder.newBuilder(config)
                        .setClusterBuilder(new TestClusterBuilder(host, port))
                        .build();

        dataStream.sinkTo(sink);

        // Execute should throw exception - the null partition key causes InvalidQueryException
        // which is classified as fatal with a specific message
        assertThatThrownBy(() -> env.execute())
                .hasStackTraceContaining("Encountered a non-recoverable InvalidQueryException")
                .hasStackTraceContaining("Invalid null value");
    }

    @Test
    void testMissingTableFailure() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<NonExistentTableUser> users = new ArrayList<>();
        users.add(new NonExistentTableUser("test_missing", "Test", 25));

        DataStream<NonExistentTableUser> dataStream =
                env.fromCollection(users, TypeInformation.of(NonExistentTableUser.class));

        PojoSinkConfig<NonExistentTableUser> config =
                new PojoSinkConfig<>(NonExistentTableUser.class, KEYSPACE, null);

        CassandraSink<NonExistentTableUser> sink =
                CassandraSinkBuilder.newBuilder(config)
                        .setClusterBuilder(new TestClusterBuilder(host, port))
                        .build();

        dataStream.sinkTo(sink);

        // Execute should throw exception for missing table
        assertThatThrownBy(() -> env.execute())
                .hasStackTraceContaining("does not exist in keyspace");
    }

    /** Test POJO with a non-existent table name to test failure scenarios. */
    @Table(keyspace = KEYSPACE, name = "non_existent_table")
    public static class NonExistentTableUser implements Serializable {
        @Column(name = "id")
        private String id;

        @Column(name = "name")
        private String name;

        @Column(name = "age")
        private Integer age;

        public NonExistentTableUser() {
            // Default constructor required for DataStax mapper
        }

        public NonExistentTableUser(String id, String name, Integer age) {
            this.id = id;
            this.name = name;
            this.age = age;
        }

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

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }
    }

    // ============== Helper Methods ==============

    private List<Row> readAllRows() {
        String selectQuery = String.format("SELECT * FROM %s.test_users;", KEYSPACE);
        ResultSet rs = cassandraTestEnvironment.executeRequestWithTimeout(selectQuery);
        return rs.all();
    }

    private Row readUserById(String id) {
        String selectQuery =
                String.format("SELECT * FROM %s.test_users WHERE id = '%s';", KEYSPACE, id);
        ResultSet rs = cassandraTestEnvironment.executeRequestWithTimeout(selectQuery);
        return rs.one();
    }
}
