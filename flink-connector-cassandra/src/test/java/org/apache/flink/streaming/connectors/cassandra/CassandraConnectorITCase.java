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

package org.apache.flink.streaming.connectors.cassandra;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo;
import org.apache.flink.batch.connectors.cassandra.CassandraInputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraPojoInputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraPojoOutputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraRowOutputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraTupleOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.cassandra.CassandraTestEnvironment;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkContextUtil;
import org.apache.flink.streaming.runtime.operators.WriteAheadSinkTestBase;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.testutils.junit.extensions.retry.RetryExtension;
import org.apache.flink.types.Row;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.annotations.Table;
import net.bytebuddy.ByteBuddy;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import scala.collection.JavaConverters;
import scala.collection.Seq;

import static org.apache.flink.connector.cassandra.CassandraTestEnvironment.KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for all cassandra sinks. */
@SuppressWarnings("serial")
@Testcontainers
@ExtendWith(RetryExtension.class)
class CassandraConnectorITCase
        extends WriteAheadSinkTestBase<
                Tuple3<String, Integer, Integer>,
                CassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>>> {

    private static final CassandraTestEnvironment cassandraTestEnvironment =
            new CassandraTestEnvironment(false);

    private static final String TABLE_NAME_PREFIX = "flink_";
    private static final String TABLE_NAME_VARIABLE = "$TABLE";
    private static final String TUPLE_ID_FIELD = "id";
    private static final String TUPLE_COUNTER_FIELD = "counter";
    private static final String TUPLE_BATCHID_FIELD = "batch_id";
    private static final String CREATE_TABLE_QUERY =
            "CREATE TABLE "
                    + KEYSPACE
                    + "."
                    + TABLE_NAME_VARIABLE
                    + " ("
                    + TUPLE_ID_FIELD
                    + " text PRIMARY KEY, "
                    + TUPLE_COUNTER_FIELD
                    + " int, "
                    + TUPLE_BATCHID_FIELD
                    + " int);";
    private static final String INSERT_DATA_QUERY =
            "INSERT INTO "
                    + KEYSPACE
                    + "."
                    + TABLE_NAME_VARIABLE
                    + " ("
                    + TUPLE_ID_FIELD
                    + ", "
                    + TUPLE_COUNTER_FIELD
                    + ", "
                    + TUPLE_BATCHID_FIELD
                    + ") VALUES (?, ?, ?)";
    private static final String SELECT_DATA_QUERY =
            "SELECT * FROM " + KEYSPACE + "." + TABLE_NAME_VARIABLE + ';';

    private static final Random random = new Random();
    private int tableID;

    private static final ArrayList<Tuple3<String, Integer, Integer>> collection =
            new ArrayList<>(20);
    private static final ArrayList<Row> rowCollection = new ArrayList<>(20);

    private static final TypeInformation[] FIELD_TYPES = {
        BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO
    };

    static {
        for (int i = 0; i < 20; i++) {
            collection.add(new Tuple3<>(UUID.randomUUID().toString(), i, 0));
            rowCollection.add(Row.of(UUID.randomUUID().toString(), i, 0));
        }
    }

    private static Class<? extends Pojo> annotatePojoWithTable(String keyspace, String tableName) {
        return new ByteBuddy()
                .redefine(Pojo.class)
                .name("org.apache.flink.streaming.connectors.cassandra.Pojo" + tableName)
                .annotateType(createTableAnnotation(keyspace, tableName))
                .make()
                .load(Pojo.class.getClassLoader())
                .getLoaded();
    }

    @NotNull
    private static Table createTableAnnotation(String keyspace, String tableName) {
        return new Table() {

            @Override
            public String keyspace() {
                return keyspace;
            }

            @Override
            public String name() {
                return tableName;
            }

            @Override
            public boolean caseSensitiveKeyspace() {
                return false;
            }

            @Override
            public boolean caseSensitiveTable() {
                return false;
            }

            @Override
            public String writeConsistency() {
                return "";
            }

            @Override
            public String readConsistency() {
                return "";
            }

            @Override
            public Class<? extends Annotation> annotationType() {
                return Table.class;
            }
        };
    }

    // ------------------------------------------------------------------------
    //  Utility methods
    // ------------------------------------------------------------------------

    private <T> List<T> readPojosWithInputFormat(Class<T> annotatedPojoClass) {
        final CassandraPojoInputFormat<T> source =
                new CassandraPojoInputFormat<>(
                        injectTableName(SELECT_DATA_QUERY),
                        cassandraTestEnvironment.getBuilderForReading(),
                        annotatedPojoClass);
        List<T> result = new ArrayList<>();

        try {
            source.configure(new Configuration());
            source.open(null);
            while (!source.reachedEnd()) {
                T temp = source.nextRecord(null);
                result.add(temp);
            }
        } finally {
            source.close();
        }
        return result;
    }

    private <T> List<T> writePojosWithOutputFormat(Class<T> annotatedPojoClass) throws Exception {
        final CassandraPojoOutputFormat<T> sink =
                new CassandraPojoOutputFormat<>(
                        cassandraTestEnvironment.getBuilderForWriting(),
                        annotatedPojoClass,
                        () -> new Mapper.Option[] {Mapper.Option.saveNullFields(true)});

        final Constructor<T> pojoConstructor = getPojoConstructor(annotatedPojoClass);
        List<T> pojos = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            pojos.add(pojoConstructor.newInstance(UUID.randomUUID().toString(), i, 0));
        }
        try {
            sink.configure(new Configuration());
            sink.open(0, 1);
            for (T pojo : pojos) {
                sink.writeRecord(pojo);
            }
        } finally {
            sink.close();
        }
        return pojos;
    }

    private <T> Constructor<T> getPojoConstructor(Class<T> annotatedPojoClass)
            throws NoSuchMethodException {
        return annotatedPojoClass.getConstructor(String.class, Integer.TYPE, Integer.TYPE);
    }

    private String injectTableName(String target) {
        return target.replace(TABLE_NAME_VARIABLE, TABLE_NAME_PREFIX + tableID);
    }

    // ------------------------------------------------------------------------
    //  Tests initialization
    // ------------------------------------------------------------------------

    @BeforeAll
    static void startUp() throws Exception {
        cassandraTestEnvironment.startUp();
    }

    @BeforeEach
    void createTable() {
        tableID = random.nextInt(Integer.MAX_VALUE);
        cassandraTestEnvironment.executeRequestWithTimeout(injectTableName(CREATE_TABLE_QUERY));
    }

    @AfterAll
    static void tearDown() throws Exception {
        cassandraTestEnvironment.tearDown();
    }

    // ------------------------------------------------------------------------
    //  Technical Tests
    // ------------------------------------------------------------------------

    @Test
    void testAnnotatePojoWithTable() {
        final String tableName = TABLE_NAME_PREFIX + tableID;

        final Class<? extends Pojo> annotatedPojoClass = annotatePojoWithTable(KEYSPACE, tableName);
        final Table pojoTableAnnotation = annotatedPojoClass.getAnnotation(Table.class);
        assertThat(pojoTableAnnotation.name()).contains(tableName);
    }

    // ------------------------------------------------------------------------
    //  Exactly-once Tests
    // ------------------------------------------------------------------------

    @Override
    protected CassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>> createSink()
            throws Exception {
        return new CassandraTupleWriteAheadSink<>(
                injectTableName(INSERT_DATA_QUERY),
                TypeExtractor.getForObject(new Tuple3<>("", 0, 0))
                        .createSerializer(new ExecutionConfig()),
                cassandraTestEnvironment.getBuilderForReading(),
                new CassandraCommitter(cassandraTestEnvironment.getBuilderForReading()));
    }

    @Override
    protected TupleTypeInfo<Tuple3<String, Integer, Integer>> createTypeInfo() {
        return TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class, Integer.class);
    }

    @Override
    protected Tuple3<String, Integer, Integer> generateValue(int counter, int checkpointID) {
        return new Tuple3<>(UUID.randomUUID().toString(), counter, checkpointID);
    }

    @Override
    protected void verifyResultsIdealCircumstances(
            CassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>> sink) {

        ResultSet result =
                cassandraTestEnvironment.executeRequestWithTimeout(
                        injectTableName(SELECT_DATA_QUERY));
        ArrayList<Integer> list = new ArrayList<>();
        for (int x = 1; x <= 60; x++) {
            list.add(x);
        }

        for (com.datastax.driver.core.Row s : result) {
            list.remove(new Integer(s.getInt(TUPLE_COUNTER_FIELD)));
        }
        assertThat(list)
                .as("The following ID's were not found in the ResultSet: " + list)
                .isEmpty();
    }

    @Override
    protected void verifyResultsDataPersistenceUponMissedNotify(
            CassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>> sink) {

        ResultSet result =
                cassandraTestEnvironment.executeRequestWithTimeout(
                        injectTableName(SELECT_DATA_QUERY));
        ArrayList<Integer> list = new ArrayList<>();
        for (int x = 1; x <= 60; x++) {
            list.add(x);
        }

        for (com.datastax.driver.core.Row s : result) {
            list.remove(new Integer(s.getInt(TUPLE_COUNTER_FIELD)));
        }
        assertThat(list)
                .as("The following ID's were not found in the ResultSet: " + list)
                .isEmpty();
    }

    @Override
    protected void verifyResultsDataDiscardingUponRestore(
            CassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>> sink) {

        ResultSet result =
                cassandraTestEnvironment.executeRequestWithTimeout(
                        injectTableName(SELECT_DATA_QUERY));
        ArrayList<Integer> list = new ArrayList<>();
        for (int x = 1; x <= 20; x++) {
            list.add(x);
        }
        for (int x = 41; x <= 60; x++) {
            list.add(x);
        }

        for (com.datastax.driver.core.Row s : result) {
            list.remove(new Integer(s.getInt(TUPLE_COUNTER_FIELD)));
        }
        assertThat(list)
                .as("The following ID's were not found in the ResultSet: " + list)
                .isEmpty();
    }

    @Override
    protected void verifyResultsWhenReScaling(
            CassandraTupleWriteAheadSink<Tuple3<String, Integer, Integer>> sink,
            int startElementCounter,
            int endElementCounter) {

        // IMPORTANT NOTE:
        //
        // for cassandra we always have to start from 1 because
        // all operators will share the same final db

        ArrayList<Integer> expected = new ArrayList<>();
        for (int i = 1; i <= endElementCounter; i++) {
            expected.add(i);
        }

        ArrayList<Integer> actual = new ArrayList<>();
        ResultSet result =
                cassandraTestEnvironment.executeRequestWithTimeout(
                        injectTableName(SELECT_DATA_QUERY));

        for (com.datastax.driver.core.Row s : result) {
            actual.add(s.getInt(TUPLE_COUNTER_FIELD));
        }

        Collections.sort(actual);
        assertThat(actual.toArray()).isEqualTo(expected.toArray());
    }

    @Test
    void testCassandraCommitter() throws Exception {
        String jobID = new JobID().toString();
        CassandraCommitter cc1 =
                new CassandraCommitter(
                        cassandraTestEnvironment.getBuilderForReading(), "flink_auxiliary_cc");
        cc1.setJobId(jobID);
        cc1.setOperatorId("operator");

        CassandraCommitter cc2 =
                new CassandraCommitter(
                        cassandraTestEnvironment.getBuilderForReading(), "flink_auxiliary_cc");
        cc2.setJobId(jobID);
        cc2.setOperatorId("operator");

        CassandraCommitter cc3 =
                new CassandraCommitter(
                        cassandraTestEnvironment.getBuilderForReading(), "flink_auxiliary_cc");
        cc3.setJobId(jobID);
        cc3.setOperatorId("operator1");

        cc1.createResource();

        cc1.open();
        cc2.open();
        cc3.open();

        assertThat(cc1.isCheckpointCommitted(0, 1)).isFalse();
        assertThat(cc2.isCheckpointCommitted(1, 1)).isFalse();
        assertThat(cc3.isCheckpointCommitted(0, 1)).isFalse();

        cc1.commitCheckpoint(0, 1);
        assertThat(cc1.isCheckpointCommitted(0, 1)).isTrue();
        // verify that other sub-tasks aren't affected
        assertThat(cc2.isCheckpointCommitted(1, 1)).isFalse();
        // verify that other tasks aren't affected
        assertThat(cc3.isCheckpointCommitted(0, 1)).isFalse();

        assertThat(cc1.isCheckpointCommitted(0, 2)).isFalse();

        cc1.close();
        cc2.close();
        cc3.close();

        cc1 =
                new CassandraCommitter(
                        cassandraTestEnvironment.getBuilderForReading(), "flink_auxiliary_cc");
        cc1.setJobId(jobID);
        cc1.setOperatorId("operator");

        cc1.open();

        // verify that checkpoint data is not destroyed within open/close and not reliant on
        // internally cached data
        assertThat(cc1.isCheckpointCommitted(0, 1)).isTrue();
        assertThat(cc1.isCheckpointCommitted(0, 2)).isFalse();

        cc1.close();
    }

    // ------------------------------------------------------------------------
    //  At-least-once Tests
    // ------------------------------------------------------------------------

    @Test
    void testCassandraTupleAtLeastOnceSink() throws Exception {
        CassandraTupleSink<Tuple3<String, Integer, Integer>> sink =
                new CassandraTupleSink<>(
                        injectTableName(INSERT_DATA_QUERY),
                        cassandraTestEnvironment.getBuilderForWriting());
        try {
            sink.open(new Configuration());
            for (Tuple3<String, Integer, Integer> value : collection) {
                sink.send(value);
            }
        } finally {
            sink.close();
        }

        ResultSet rs =
                cassandraTestEnvironment.executeRequestWithTimeout(
                        injectTableName(SELECT_DATA_QUERY));
        assertThat(rs.all()).hasSize(20);
    }

    @Test
    void testCassandraRowAtLeastOnceSink() throws Exception {
        CassandraRowSink sink =
                new CassandraRowSink(
                        FIELD_TYPES.length,
                        injectTableName(INSERT_DATA_QUERY),
                        cassandraTestEnvironment.getBuilderForWriting());
        try {
            sink.open(new Configuration());
            for (Row value : rowCollection) {
                sink.send(value);
            }
        } finally {
            sink.close();
        }

        ResultSet rs =
                cassandraTestEnvironment.executeRequestWithTimeout(
                        injectTableName(SELECT_DATA_QUERY));
        assertThat(rs.all()).hasSize(20);
    }

    @Test
    void testCassandraPojoAtLeastOnceSink() throws Exception {
        final Class<? extends Pojo> annotatedPojoClass =
                annotatePojoWithTable(KEYSPACE, TABLE_NAME_PREFIX + tableID);
        writePojos(annotatedPojoClass, null);

        ResultSet rs =
                cassandraTestEnvironment.executeRequestWithTimeout(
                        injectTableName(SELECT_DATA_QUERY));
        assertThat(rs.all()).hasSize(20);
    }

    @Test
    void testCassandraPojoNoAnnotatedKeyspaceAtLeastOnceSink() throws Exception {
        final Class<? extends Pojo> annotatedPojoClass =
                annotatePojoWithTable("", TABLE_NAME_PREFIX + tableID);
        writePojos(annotatedPojoClass, KEYSPACE);
        ResultSet rs =
                cassandraTestEnvironment.executeRequestWithTimeout(
                        injectTableName(SELECT_DATA_QUERY));
        assertThat(rs.all()).hasSize(20);
    }

    private <T> void writePojos(Class<T> annotatedPojoClass, @Nullable String keyspace)
            throws Exception {
        final Constructor<T> pojoConstructor = getPojoConstructor(annotatedPojoClass);
        CassandraPojoSink<T> sink =
                new CassandraPojoSink<>(
                        annotatedPojoClass,
                        cassandraTestEnvironment.getBuilderForWriting(),
                        null,
                        keyspace);
        try {
            sink.open(new Configuration());
            for (int x = 0; x < 20; x++) {
                sink.send(pojoConstructor.newInstance(UUID.randomUUID().toString(), x, 0));
            }
        } finally {
            sink.close();
        }
    }

    @Test
    void testCassandraTableSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<Row> source = env.fromCollection(rowCollection);

        tEnv.createTemporaryView("testFlinkTable", source);
        ((TableEnvironmentInternal) tEnv)
                .registerTableSinkInternal(
                        "cassandraTable",
                        new CassandraAppendTableSink(
                                        cassandraTestEnvironment.getBuilderForWriting(),
                                        injectTableName(INSERT_DATA_QUERY))
                                .configure(
                                        new String[] {"f0", "f1", "f2"},
                                        new TypeInformation[] {
                                            Types.STRING, Types.INT, Types.INT
                                        }));

        tEnv.sqlQuery("select * from testFlinkTable").executeInsert("cassandraTable").await();

        ResultSet rs =
                cassandraTestEnvironment.executeRequestWithTimeout(
                        injectTableName(SELECT_DATA_QUERY));

        // validate that all input was correctly written to Cassandra
        List<Row> input = new ArrayList<>(rowCollection);
        List<com.datastax.driver.core.Row> output = rs.all();
        for (com.datastax.driver.core.Row o : output) {
            Row cmp = new Row(3);
            cmp.setField(0, o.getString(0));
            cmp.setField(1, o.getInt(2));
            cmp.setField(2, o.getInt(1));
            assertThat(input.remove(cmp))
                    .as("Row " + cmp + " was written to Cassandra but not in input.")
                    .isTrue();
        }
        assertThat(input).as("The input data was not completely written to Cassandra").isEmpty();
    }

    private static int retrialsCount = 0;

    @Test
    void testCassandraBatchPojoFormat() throws Exception {

        final Class<? extends Pojo> annotatedPojoClass =
                annotatePojoWithTable(KEYSPACE, TABLE_NAME_PREFIX + tableID);

        final List<? extends Pojo> pojos = writePojosWithOutputFormat(annotatedPojoClass);
        ResultSet rs =
                cassandraTestEnvironment.executeRequestWithTimeout(
                        injectTableName(SELECT_DATA_QUERY));
        assertThat(rs.all()).hasSize(20);

        final List<? extends Pojo> result = readPojosWithInputFormat(annotatedPojoClass);
        assertThat(result)
                .hasSize(20)
                .usingRecursiveComparison(
                        RecursiveComparisonConfiguration.builder()
                                .withIgnoreCollectionOrder(true)
                                .build())
                .isEqualTo(pojos);
    }

    @Test
    void testCassandraBatchTupleFormat() throws Exception {
        OutputFormat<Tuple3<String, Integer, Integer>> sink =
                new CassandraTupleOutputFormat<>(
                        injectTableName(INSERT_DATA_QUERY),
                        cassandraTestEnvironment.getBuilderForWriting());
        try {
            sink.configure(new Configuration());
            sink.open(0, 1);
            for (Tuple3<String, Integer, Integer> value : collection) {
                sink.writeRecord(value);
            }
        } finally {
            sink.close();
        }

        sink =
                new CassandraTupleOutputFormat<>(
                        injectTableName(INSERT_DATA_QUERY),
                        cassandraTestEnvironment.getBuilderForWriting());
        try {
            sink.configure(new Configuration());
            sink.open(0, 1);
            for (Tuple3<String, Integer, Integer> value : collection) {
                sink.writeRecord(value);
            }
        } finally {
            sink.close();
        }

        InputFormat<Tuple3<String, Integer, Integer>, InputSplit> source =
                new CassandraInputFormat<>(
                        injectTableName(SELECT_DATA_QUERY),
                        cassandraTestEnvironment.getBuilderForReading());
        List<Tuple3<String, Integer, Integer>> result = new ArrayList<>();
        try {
            source.configure(new Configuration());
            source.open(null);
            while (!source.reachedEnd()) {
                result.add(source.nextRecord(new Tuple3<String, Integer, Integer>()));
            }
        } finally {
            source.close();
        }

        assertThat(result).hasSize(20);
    }

    @Test
    void testCassandraBatchRowFormat() throws Exception {
        OutputFormat<Row> sink =
                new CassandraRowOutputFormat(
                        injectTableName(INSERT_DATA_QUERY),
                        cassandraTestEnvironment.getBuilderForWriting());
        try {
            sink.configure(new Configuration());
            sink.open(0, 1);
            for (Row value : rowCollection) {
                sink.writeRecord(value);
            }
        } finally {

            sink.close();
        }

        ResultSet rs =
                cassandraTestEnvironment.executeRequestWithTimeout(
                        injectTableName(SELECT_DATA_QUERY));
        List<com.datastax.driver.core.Row> rows = rs.all();
        assertThat(rows).hasSameSizeAs(rowCollection);
    }

    @Test
    void testCassandraScalaTupleAtLeastOnceSinkBuilderDetection() throws Exception {
        Class<scala.Tuple1<String>> c =
                (Class<scala.Tuple1<String>>) new scala.Tuple1<>("hello").getClass();
        Seq<TypeInformation<?>> typeInfos =
                JavaConverters.asScalaBufferConverter(
                                Collections.<TypeInformation<?>>singletonList(
                                        BasicTypeInfo.STRING_TYPE_INFO))
                        .asScala();
        Seq<String> fieldNames =
                JavaConverters.asScalaBufferConverter(Collections.singletonList("_1")).asScala();

        CaseClassTypeInfo<scala.Tuple1<String>> typeInfo =
                new CaseClassTypeInfo<scala.Tuple1<String>>(c, null, typeInfos, fieldNames) {
                    @Override
                    public TypeSerializer<scala.Tuple1<String>> createSerializer(
                            ExecutionConfig config) {
                        return null;
                    }
                };

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<scala.Tuple1<String>> input =
                env.fromElements(new scala.Tuple1<>("hello")).returns(typeInfo);

        CassandraSink.CassandraSinkBuilder<scala.Tuple1<String>> sinkBuilder =
                CassandraSink.addSink(input);
        assertThat(sinkBuilder).isInstanceOf(CassandraSink.CassandraScalaProductSinkBuilder.class);
    }

    @Test
    void testCassandraScalaTupleAtLeastSink() throws Exception {
        CassandraScalaProductSink<scala.Tuple3<String, Integer, Integer>> sink =
                new CassandraScalaProductSink<>(
                        injectTableName(INSERT_DATA_QUERY),
                        cassandraTestEnvironment.getBuilderForWriting());

        List<scala.Tuple3<String, Integer, Integer>> scalaTupleCollection = new ArrayList<>(20);
        for (int i = 0; i < 20; i++) {
            scalaTupleCollection.add(new scala.Tuple3<>(UUID.randomUUID().toString(), i, 0));
        }
        try {
            sink.open(new Configuration());
            for (scala.Tuple3<String, Integer, Integer> value : scalaTupleCollection) {
                sink.invoke(value, SinkContextUtil.forTimestamp(0));
            }
        } finally {
            sink.close();
        }

        ResultSet rs =
                cassandraTestEnvironment.executeRequestWithTimeout(
                        injectTableName(SELECT_DATA_QUERY));
        List<com.datastax.driver.core.Row> rows = rs.all();
        assertThat(rows).hasSameSizeAs(scalaTupleCollection);

        for (com.datastax.driver.core.Row row : rows) {
            scalaTupleCollection.remove(
                    new scala.Tuple3<>(
                            row.getString(TUPLE_ID_FIELD),
                            row.getInt(TUPLE_COUNTER_FIELD),
                            row.getInt(TUPLE_BATCHID_FIELD)));
        }
        assertThat(scalaTupleCollection).isEmpty();
    }

    @Test
    void testCassandraScalaTuplePartialColumnUpdate() throws Exception {
        CassandraSinkBaseConfig config =
                CassandraSinkBaseConfig.newBuilder().setIgnoreNullFields(true).build();
        CassandraScalaProductSink<scala.Tuple3<String, Integer, Integer>> sink =
                new CassandraScalaProductSink<>(
                        injectTableName(INSERT_DATA_QUERY),
                        cassandraTestEnvironment.getBuilderForWriting(),
                        config);

        String id = UUID.randomUUID().toString();
        Integer counter = 1;
        Integer batchId = 0;

        // Send partial records across multiple request
        scala.Tuple3<String, Integer, Integer> scalaTupleRecordFirst =
                new scala.Tuple3<>(id, counter, null);
        scala.Tuple3<String, Integer, Integer> scalaTupleRecordSecond =
                new scala.Tuple3<>(id, null, batchId);

        try {
            sink.open(new Configuration());
            sink.invoke(scalaTupleRecordFirst, SinkContextUtil.forTimestamp(0));
            sink.invoke(scalaTupleRecordSecond, SinkContextUtil.forTimestamp(0));
        } finally {
            sink.close();
        }

        ResultSet rs =
                cassandraTestEnvironment.executeRequestWithTimeout(
                        injectTableName(SELECT_DATA_QUERY));
        List<com.datastax.driver.core.Row> rows = rs.all();
        assertThat(rows).hasSize(1);
        // Since nulls are ignored, we should be reading one complete record
        for (com.datastax.driver.core.Row row : rows) {
            assertThat(
                            new scala.Tuple3<>(
                                    row.getString(TUPLE_ID_FIELD),
                                    row.getInt(TUPLE_COUNTER_FIELD),
                                    row.getInt(TUPLE_BATCHID_FIELD)))
                    .isEqualTo(new scala.Tuple3<>(id, counter, batchId));
        }
    }
}
