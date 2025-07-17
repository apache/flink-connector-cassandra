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

package org.apache.flink.connector.cassandra.table;

import org.apache.flink.connector.cassandra.CassandraTestEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comprehensive integration tests for Cassandra Dynamic Table Source covering all data types,
 * complex scenarios, edge cases, and error conditions. Tests both basic functionality and
 * real-world complex data structures to ensure complete mapper coverage.
 */
@ExtendWith(MiniClusterExtension.class)
class CassandraDynamicTableSourceITCase {

    private CassandraTestEnvironment cassandraTestEnvironment;
    private StreamTableEnvironment tableEnv;

    @BeforeEach
    void setUp() throws Exception {
        cassandraTestEnvironment = new CassandraTestEnvironment(false);
        cassandraTestEnvironment.startUp();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /** Use parallelism 1 for deterministic results */
        tableEnv = StreamTableEnvironment.create(env);

        createAllTestTables();
        insertAllTestData();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (cassandraTestEnvironment != null) {
            cassandraTestEnvironment.tearDown();
        }
    }

    private void createAllTestTables() {
        createUserDefinedTypes();
        createPrimitivesTable();
        createAdditionalTypesTable();
        createCollectionsTable();
        createComplexTable();
        createTupleTypesTable();
        createDeepNestedTable();
        createEdgeCasesTable();
        createMegaComplexTable();
        createNullHandlingTable();
        createNestedCollectionsTable();
    }

    private void createUserDefinedTypes() {
        /** Create address UDT for testing ROW mapping */
        String createAddressUDT =
                "CREATE TYPE IF NOT EXISTS "
                        + CassandraTestEnvironment.KEYSPACE
                        + ".address ("
                        + "street text, "
                        + "city text, "
                        + "zipcode int, "
                        + "country text"
                        + ");";

        /** Create contact UDT for complex nesting scenarios */
        String createContactUDT =
                "CREATE TYPE IF NOT EXISTS "
                        + CassandraTestEnvironment.KEYSPACE
                        + ".contact ("
                        + "email text, "
                        + "phone text, "
                        + "preferred boolean"
                        + ");";

        /** Create company UDT with nested collections */
        String createCompanyUDT =
                "CREATE TYPE IF NOT EXISTS "
                        + CassandraTestEnvironment.KEYSPACE
                        + ".company ("
                        + "name text, "
                        + "employees frozen<list<text>>, "
                        + "departments frozen<set<text>>, "
                        + "budget_by_dept frozen<map<text, decimal>>"
                        + ");";

        /** Create employee UDT with deep nesting */
        String createEmployeeUDT =
                "CREATE TYPE IF NOT EXISTS "
                        + CassandraTestEnvironment.KEYSPACE
                        + ".employee ("
                        + "id int, "
                        + "name text, "
                        + "address frozen<address>, "
                        + "contacts list<frozen<contact>>, "
                        + "company frozen<company>"
                        + ");";

        cassandraTestEnvironment.executeRequestWithTimeout(createAddressUDT);
        cassandraTestEnvironment.executeRequestWithTimeout(createContactUDT);
        cassandraTestEnvironment.executeRequestWithTimeout(createCompanyUDT);
        cassandraTestEnvironment.executeRequestWithTimeout(createEmployeeUDT);
    }

    private void createPrimitivesTable() {
        /** Table with basic primitive types commonly used */
        String createPrimitivesTable =
                "CREATE TABLE IF NOT EXISTS "
                        + CassandraTestEnvironment.KEYSPACE
                        + ".primitives_table ("
                        + "id int PRIMARY KEY, "
                        + "name text, "
                        + "age int, "
                        + "salary double, "
                        + "active boolean, "
                        + "score float, "
                        + "balance decimal, "
                        + "created_at timestamp"
                        + ");";

        cassandraTestEnvironment.executeRequestWithTimeout(createPrimitivesTable);
    }

    private void createAdditionalTypesTable() {
        /** Table with all supported Cassandra primitive types */
        String createAdditionalTypesTable =
                "CREATE TABLE IF NOT EXISTS "
                        + CassandraTestEnvironment.KEYSPACE
                        + ".all_primitives ("
                        + "id int PRIMARY KEY, "
                        + "text_col text, "
                        + "varchar_col varchar, "
                        + "ascii_col ascii, "
                        + "int_col int, "
                        + "bigint_col bigint, "
                        + "smallint_col smallint, "
                        + "tinyint_col tinyint, "
                        + "float_col float, "
                        + "double_col double, "
                        + "decimal_col decimal, "
                        + "varint_col varint, "
                        + "boolean_col boolean, "
                        + "timestamp_col timestamp, "
                        + "date_col date, "
                        + "time_col time, "
                        + "binary_data blob, "
                        + "uuid_col uuid, "
                        + "timeuuid_col timeuuid, "
                        + "inet_col inet"
                        + ");";

        cassandraTestEnvironment.executeRequestWithTimeout(createAdditionalTypesTable);
    }

    private void createCollectionsTable() {
        /** Table with all collection types and combinations */
        String createCollectionsTable =
                "CREATE TABLE IF NOT EXISTS "
                        + CassandraTestEnvironment.KEYSPACE
                        + ".collections_table ("
                        + "id int PRIMARY KEY, "
                        + "list_text list<text>, "
                        + "list_int list<int>, "
                        + "list_double list<double>, "
                        + "list_boolean list<boolean>, "
                        + "set_text set<text>, "
                        + "set_int set<int>, "
                        + "map_text_int map<text, int>, "
                        + "map_int_text map<int, text>, "
                        + "map_text_boolean map<text, boolean>, "
                        + "list_of_list list<frozen<list<text>>>, "
                        + "map_of_list map<text, frozen<list<int>>>, "
                        + "set_of_map set<frozen<map<text, int>>>"
                        + ");";

        cassandraTestEnvironment.executeRequestWithTimeout(createCollectionsTable);
    }

    private void createComplexTable() {
        /** Table with UDT and complex types */
        String createComplexTable =
                "CREATE TABLE IF NOT EXISTS "
                        + CassandraTestEnvironment.KEYSPACE
                        + ".complex_table ("
                        + "id int PRIMARY KEY, "
                        + "user_address frozen<address>, "
                        + "contact_col frozen<contact>, "
                        + "phone_numbers list<text>, "
                        + "preferences map<text, boolean>, "
                        + "list_address list<frozen<address>>, "
                        + "map_text_address map<text, frozen<address>>, "
                        + "set_contact set<frozen<contact>>"
                        + ");";

        cassandraTestEnvironment.executeRequestWithTimeout(createComplexTable);
    }

    private void createTupleTypesTable() {
        /** Table with tuple types for testing ROW mapping */
        String createTupleTypesTable =
                "CREATE TABLE IF NOT EXISTS "
                        + CassandraTestEnvironment.KEYSPACE
                        + ".tuple_types ("
                        + "id int PRIMARY KEY, "
                        + "simple_tuple tuple<text, int>, "
                        + "complex_tuple tuple<text, int, boolean, double>, "
                        + "nested_tuple tuple<text, list<int>, map<text, boolean>>, "
                        + "list_of_tuples list<tuple<text, int>>, "
                        + "map_with_tuple_key map<tuple<text, int>, text>, "
                        + "map_with_tuple_value map<text, tuple<int, boolean>>"
                        + ");";

        cassandraTestEnvironment.executeRequestWithTimeout(createTupleTypesTable);
    }

    private void createDeepNestedTable() {
        /** Table with deeply nested structures */
        String createDeepNestedTable =
                "CREATE TABLE IF NOT EXISTS "
                        + CassandraTestEnvironment.KEYSPACE
                        + ".deep_nested ("
                        + "id int PRIMARY KEY, "
                        + "employee_data frozen<employee>, "
                        + "employee_list list<frozen<employee>>, "
                        + "employee_map map<text, frozen<employee>>"
                        + ");";

        cassandraTestEnvironment.executeRequestWithTimeout(createDeepNestedTable);
    }

    private void createEdgeCasesTable() {
        /** Table for testing edge cases and null handling */
        String createEdgeCasesTable =
                "CREATE TABLE IF NOT EXISTS "
                        + CassandraTestEnvironment.KEYSPACE
                        + ".edge_cases ("
                        + "id int PRIMARY KEY, "
                        + "empty_list list<text>, "
                        + "empty_set set<int>, "
                        + "empty_map map<text, boolean>, "
                        + "list_with_values list<text>, "
                        + "map_with_values map<text, text>, "
                        + "single_list list<double>, "
                        + "single_set set<uuid>, "
                        + "single_map map<int, text>, "
                        + "list_of_empty_lists list<frozen<list<text>>>, "
                        + "map_of_empty_maps map<text, frozen<map<text, int>>>, "
                        + "large_blob blob, "
                        + "tuple_with_nulls tuple<text, int, boolean>, "
                        + "partial_udt frozen<address>"
                        + ");";

        cassandraTestEnvironment.executeRequestWithTimeout(createEdgeCasesTable);
    }

    private void createMegaComplexTable() {
        /** Table with extremely complex nested structures */
        String createMegaComplexTable =
                "CREATE TABLE IF NOT EXISTS "
                        + CassandraTestEnvironment.KEYSPACE
                        + ".mega_complex ("
                        + "id int PRIMARY KEY, "
                        + "ultimate_complex map<frozen<tuple<text, int>>, frozen<list<frozen<map<uuid, frozen<employee>>>>>>, "
                        + "tuple_collection_madness list<frozen<tuple<text, frozen<list<int>>, frozen<map<text, boolean>>, frozen<set<double>>>>>, "
                        + "numeric_soup list<frozen<map<text, frozen<tuple<tinyint, smallint, int, bigint, float, double, decimal, varint>>>>>, "
                        + "temporal_collections map<date, frozen<list<frozen<tuple<timestamp, time>>>>>, "
                        + "binary_complex list<frozen<map<uuid, frozen<tuple<blob, inet, text>>>>>, "
                        + "boolean_matrix list<frozen<list<frozen<map<text, boolean>>>>>"
                        + ");";

        cassandraTestEnvironment.executeRequestWithTimeout(createMegaComplexTable);
    }

    private void createNullHandlingTable() {
        /** Table for comprehensive null handling testing */
        String createNullHandlingTable =
                "CREATE TABLE IF NOT EXISTS "
                        + CassandraTestEnvironment.KEYSPACE
                        + ".null_handling ("
                        + "id int PRIMARY KEY, "
                        + "nullable_udt frozen<address>, "
                        + "partial_udt frozen<address>, "
                        + "optional_list list<text>, "
                        + "optional_set set<int>, "
                        + "optional_map map<text, text>, "
                        + "large_binary_data blob, "
                        + "very_large_binary blob, "
                        + "empty_text text, "
                        + "zero_int int, "
                        + "false_boolean boolean"
                        + ");";

        cassandraTestEnvironment.executeRequestWithTimeout(createNullHandlingTable);
    }

    private void createNestedCollectionsTable() {
        /** Table for testing nested collections */
        String createNestedTable =
                "CREATE TABLE IF NOT EXISTS "
                        + CassandraTestEnvironment.KEYSPACE
                        + ".nested_collections ("
                        + "id int PRIMARY KEY, "
                        + "nested_list list<frozen<list<int>>>, "
                        + "nested_map map<text, frozen<map<text, int>>>"
                        + ");";

        cassandraTestEnvironment.executeRequestWithTimeout(createNestedTable);
    }

    private void insertAllTestData() {
        insertPrimitivesData();
        insertAllPrimitivesData();
        insertCollectionsData();
        insertComplexTypesData();
        insertTupleTypesData();
        insertDeepNestedData();
        insertEdgeCasesData();
        insertMegaComplexData();
        insertNullHandlingData();
        insertNestedCollectionsData();
    }

    private void insertPrimitivesData() {
        /** Insert basic primitive data */
        cassandraTestEnvironment.executeRequestWithTimeout(
                String.format(
                        "INSERT INTO %s.primitives_table (id, name, age, salary, active, score, balance, created_at) "
                                + "VALUES (1, 'Alice', 30, 75000.50, true, 95.5, 1234.56, '2023-01-15 10:30:00');",
                        CassandraTestEnvironment.KEYSPACE));

        cassandraTestEnvironment.executeRequestWithTimeout(
                String.format(
                        "INSERT INTO %s.primitives_table (id, name, age, salary, active, score, balance, created_at) "
                                + "VALUES (2, 'Bob', 25, 65000.75, false, 88.3, 987.65, '2023-02-20 14:45:00');",
                        CassandraTestEnvironment.KEYSPACE));
    }

    private void insertAllPrimitivesData() {
        /** Insert comprehensive primitive type data */
        String insert =
                String.format(
                        "INSERT INTO %s.all_primitives ("
                                + "id, text_col, varchar_col, ascii_col, "
                                + "int_col, bigint_col, smallint_col, tinyint_col, "
                                + "float_col, double_col, decimal_col, varint_col, "
                                + "boolean_col, timestamp_col, date_col, time_col, "
                                + "binary_data, uuid_col, timeuuid_col, inet_col"
                                + ") VALUES ("
                                + "1, "
                                + "'Hello World', "
                                + "'Varchar Text', "
                                + "'ASCII', "
                                + "42, "
                                + "9223372036854775807, "
                                + "32767, "
                                + "127, "
                                + "3.14, "
                                + "2.718281828, "
                                + "123.456, "
                                + "999999999999999999999999999999, "
                                + "true, "
                                + "'2023-12-25 10:30:00+0000', "
                                + "'2023-12-25', "
                                + "'10:30:00', "
                                + "textAsBlob('binary_test'), "
                                + "550e8400-e29b-41d4-a716-446655440000, "
                                + "now(), "
                                + "'192.168.1.1'"
                                + ");",
                        CassandraTestEnvironment.KEYSPACE);

        cassandraTestEnvironment.executeRequestWithTimeout(insert);
    }

    private void insertCollectionsData() {
        /** Insert collection data */
        String insert =
                String.format(
                        "INSERT INTO %s.collections_table ("
                                + "id, list_text, list_int, list_double, list_boolean, "
                                + "set_text, set_int, "
                                + "map_text_int, map_int_text, map_text_boolean, "
                                + "list_of_list, map_of_list, set_of_map"
                                + ") VALUES ("
                                + "1, "
                                + "['apple', 'banana', 'cherry'], "
                                + "[1, 2, 3, 4, 5], "
                                + "[1.1, 2.2, 3.3], "
                                + "[true, false, true], "
                                + "{'red', 'green', 'blue'}, "
                                + "{10, 20, 30}, "
                                + "{'key1': 100, 'key2': 200}, "
                                + "{1: 'one', 2: 'two'}, "
                                + "{'enabled': true, 'visible': false}, "
                                + "[['a', 'b'], ['c', 'd']], "
                                + "{'numbers': [1, 2, 3], 'scores': [90, 95]}, "
                                + "{{'lang': 1, 'skill': 2}, {'java': 5, 'flink': 4}}"
                                + ");",
                        CassandraTestEnvironment.KEYSPACE);

        cassandraTestEnvironment.executeRequestWithTimeout(insert);
    }

    private void insertComplexTypesData() {
        /** Insert complex UDT data */
        String insert =
                String.format(
                        "INSERT INTO %s.complex_table ("
                                + "id, user_address, contact_col, phone_numbers, preferences, "
                                + "list_address, map_text_address, set_contact"
                                + ") VALUES ("
                                + "1, "
                                + "{street: '123 Main St', city: 'New York', zipcode: 10001, country: 'USA'}, "
                                + "{email: 'test@example.com', phone: '555-1234', preferred: true}, "
                                + "['555-1234', '555-5678'], "
                                + "{'email_notifications': true, 'sms_alerts': false}, "
                                + "[{street: '456 Oak Ave', city: 'SF', zipcode: 94102, country: 'USA'}], "
                                + "{'home': {street: '789 Pine St', city: 'LA', zipcode: 90210, country: 'USA'}}, "
                                + "{{email: 'contact1@test.com', phone: '555-0001', preferred: false}}"
                                + ");",
                        CassandraTestEnvironment.KEYSPACE);

        cassandraTestEnvironment.executeRequestWithTimeout(insert);
    }

    private void insertTupleTypesData() {
        /** Insert tuple type data */
        String insert =
                String.format(
                        "INSERT INTO %s.tuple_types ("
                                + "id, simple_tuple, complex_tuple, nested_tuple, "
                                + "list_of_tuples, map_with_tuple_key, map_with_tuple_value"
                                + ") VALUES ("
                                + "1, "
                                + "('Hello', 42), "
                                + "('Complex', 99, true, 3.14), "
                                + "('Nested', [1, 2, 3], {'key': true}), "
                                + "[('first', 1), ('second', 2)], "
                                + "{('key', 100): 'value'}, "
                                + "{'result': (200, false)}"
                                + ");",
                        CassandraTestEnvironment.KEYSPACE);

        cassandraTestEnvironment.executeRequestWithTimeout(insert);
    }

    private void insertDeepNestedData() {
        /** Insert deeply nested UDT data */
        String insert =
                String.format(
                        "INSERT INTO %s.deep_nested ("
                                + "id, employee_data, employee_list, employee_map"
                                + ") VALUES ("
                                + "1, "
                                + "{id: 123, name: 'John Doe', "
                                + " address: {street: '123 Work St', city: 'Work City', zipcode: 12345, country: 'USA'}, "
                                + " contacts: [{email: 'john@work.com', phone: '555-WORK', preferred: true}], "
                                + " company: {name: 'TechCorp', employees: ['Alice', 'Bob'], departments: {'Engineering', 'Sales'}, budget_by_dept: {'Engineering': 1000000.50, 'Sales': 500000.25}}}, "
                                + "[{id: 456, name: 'Jane Smith', address: {street: '456 Home St', city: 'Home City', zipcode: 67890, country: 'USA'}, contacts: [], company: {name: 'StartupInc', employees: ['Jane'], departments: {'All'}, budget_by_dept: {'All': 100000.00}}}], "
                                + "{'manager': {id: 789, name: 'Boss Person', address: {street: '789 Boss Ave', city: 'Boss Town', zipcode: 11111, country: 'USA'}, contacts: [{email: 'boss@company.com', phone: '555-BOSS', preferred: true}], company: {name: 'BigCorp', employees: ['Many'], departments: {'Management'}, budget_by_dept: {'Management': 2000000.00}}}}"
                                + ");",
                        CassandraTestEnvironment.KEYSPACE);

        cassandraTestEnvironment.executeRequestWithTimeout(insert);
    }

    private void insertEdgeCasesData() {
        /** Insert edge case data */
        String insert =
                String.format(
                        "INSERT INTO %s.edge_cases ("
                                + "id, empty_list, empty_set, empty_map, "
                                + "list_with_values, map_with_values, "
                                + "single_list, single_set, single_map, "
                                + "list_of_empty_lists, map_of_empty_maps, "
                                + "large_blob, "
                                + "tuple_with_nulls, partial_udt"
                                + ") VALUES ("
                                + "1, "
                                + "[], "
                                + "{}, "
                                + "{}, "
                                + "['value1', 'value2', 'value3'], "
                                + "{'key1': 'value1', 'key2': 'value2'}, "
                                + "[3.14159], "
                                + "{550e8400-e29b-41d4-a716-446655440000}, "
                                + "{42: 'answer'}, "
                                + "[[], ['item']], "
                                + "{'empty': {}, 'nonempty': {'k': 1}}, "
                                + "textAsBlob('This is a large blob of binary data'), "
                                + "('text_val', 42, true), "
                                + "{street: 'Partial St', city: 'Some City', zipcode: 12345, country: 'Unknown'}"
                                + ");",
                        CassandraTestEnvironment.KEYSPACE);

        cassandraTestEnvironment.executeRequestWithTimeout(insert);
    }

    private void insertMegaComplexData() {
        /** Insert extremely complex data */
        String insert =
                String.format(
                        "INSERT INTO %s.mega_complex ("
                                + "id, ultimate_complex, tuple_collection_madness, "
                                + "numeric_soup, temporal_collections, binary_complex, boolean_matrix"
                                + ") VALUES ("
                                + "1, "
                                + "{('key1', 100): [{550e8400-e29b-41d4-a716-446655440000: {id: 1, name: 'Ultimate Employee', address: {street: 'Ultimate St', city: 'Ultimate City', zipcode: 99999, country: 'Ultimate Land'}, contacts: [], company: {name: 'Ultimate Corp', employees: ['Ultimate'], departments: {'Ultimate'}, budget_by_dept: {'Ultimate': 999999.99}}}}]}, "
                                + "[('chaos', [1, 2, 3], {'flag1': true, 'flag2': false}, {1.1, 2.2, 3.3}), ('madness', [4, 5, 6], {'active': true}, {4.4, 5.5})], "
                                + "[{'numbers': (127, 32767, 2147483647, 9223372036854775807, 3.14, 2.718281828, 123.456, 999999999999999999999999)}], "
                                + "{'2023-12-25': [('2023-12-25 10:30:00+0000', '10:30:00'), ('2023-12-25 15:45:00+0000', '15:45:00')]}, "
                                + "[{550e8400-e29b-41d4-a716-446655440002: (textAsBlob('binary data'), '192.168.1.100', 'text data')}], "
                                + "[[{'row1col1': true, 'row1col2': false}, {'row1col3': true}], [{'row2col1': false, 'row2col2': true}]]"
                                + ");",
                        CassandraTestEnvironment.KEYSPACE);

        cassandraTestEnvironment.executeRequestWithTimeout(insert);
    }

    private void insertNullHandlingData() {
        /** Insert null handling test data */
        String insertNull =
                String.format(
                        "INSERT INTO %s.null_handling ("
                                + "id, nullable_udt, partial_udt, "
                                + "optional_list, optional_set, optional_map, "
                                + "large_binary_data, very_large_binary, "
                                + "empty_text, zero_int, false_boolean"
                                + ") VALUES ("
                                + "1, "
                                + "null, "
                                + "{street: 'Some Street', city: null, zipcode: 12345, country: 'USA'}, "
                                + "null, "
                                + "null, "
                                + "null, "
                                + "textAsBlob('Large binary blob'), "
                                + "textAsBlob('Very large binary blob'), "
                                + "'', "
                                + "0, "
                                + "false"
                                + ");",
                        CassandraTestEnvironment.KEYSPACE);

        String insertPopulated =
                String.format(
                        "INSERT INTO %s.null_handling ("
                                + "id, nullable_udt, partial_udt, "
                                + "optional_list, optional_set, optional_map, "
                                + "large_binary_data, very_large_binary, "
                                + "empty_text, zero_int, false_boolean"
                                + ") VALUES ("
                                + "2, "
                                + "{street: 'Full Street', city: 'Full City', zipcode: 54321, country: 'Canada'}, "
                                + "{street: 'Partial Street', city: 'Partial City', zipcode: 99999, country: 'Mexico'}, "
                                + "['item1', 'item2', 'item3'], "
                                + "{100, 200, 300}, "
                                + "{'key1': 'value1', 'key2': 'value2'}, "
                                + "textAsBlob('Small binary'), "
                                + "textAsBlob('Another small binary'), "
                                + "'populated text', "
                                + "42, "
                                + "true"
                                + ");",
                        CassandraTestEnvironment.KEYSPACE);

        cassandraTestEnvironment.executeRequestWithTimeout(insertNull);
        cassandraTestEnvironment.executeRequestWithTimeout(insertPopulated);
    }

    private void insertNestedCollectionsData() {
        /** Insert nested collections test data */
        cassandraTestEnvironment.executeRequestWithTimeout(
                String.format(
                        "INSERT INTO %s.nested_collections (id, nested_list, nested_map) "
                                + "VALUES (1, [[1, 2], [3, 4]], {'outer1': {'inner1': 10, 'inner2': 20}});",
                        CassandraTestEnvironment.KEYSPACE));
    }

    /**
     * Tests basic primitive data types including INT, STRING, DOUBLE, BOOLEAN, FLOAT, DECIMAL,
     * TIMESTAMP. Validates that common primitive types are correctly mapped from Cassandra to
     * Flink.
     */
    @Test
    void testBasicPrimitiveTypes() throws Exception {
        String createFlinkTable =
                "CREATE TABLE flink_primitives ("
                        + "  id INT,"
                        + "  name STRING,"
                        + "  age INT,"
                        + "  salary DOUBLE,"
                        + "  active BOOLEAN,"
                        + "  score FLOAT,"
                        + "  balance DECIMAL(10,2),"
                        + "  created_at TIMESTAMP(3)"
                        + ") WITH ("
                        + "  'connector' = 'cassandra',"
                        + "  'hosts' = '"
                        + cassandraTestEnvironment.getContactPoint()
                        + "',"
                        + "  'port' = '"
                        + cassandraTestEnvironment.getPort()
                        + "',"
                        + "  'keyspace' = '"
                        + CassandraTestEnvironment.KEYSPACE
                        + "',"
                        + "  'table' = 'primitives_table',"
                        + "  'username' = 'cassandra',"
                        + "  'password' = 'cassandra'"
                        + ")";

        tableEnv.executeSql(createFlinkTable);

        Table result = tableEnv.sqlQuery("SELECT * FROM flink_primitives");

        List<Row> actualRows = collectResults(result, 2);

        assertThat(actualRows).hasSize(2);

        /** Verify Alice's record */
        assertThat(actualRows)
                .anySatisfy(
                        row -> {
                            assertThat(row.getField(0)).isEqualTo(1);
                            assertThat(row.getField(1)).isEqualTo("Alice");
                            assertThat(row.getField(2)).isEqualTo(30);
                            assertThat(row.getField(3)).isEqualTo(75000.50);
                            assertThat(row.getField(4)).isEqualTo(true);
                            assertThat(row.getField(5)).isEqualTo(95.5f);
                            assertThat(row.getField(6)).isEqualTo(new BigDecimal("1234.56"));
                            assertThat(row.getField(7)).isNotNull();
                        });

        /** Verify Bob's record */
        assertThat(actualRows)
                .anySatisfy(
                        row -> {
                            assertThat(row.getField(0)).isEqualTo(2);
                            assertThat(row.getField(1)).isEqualTo("Bob");
                            assertThat(row.getField(2)).isEqualTo(25);
                            assertThat(row.getField(3)).isEqualTo(65000.75);
                            assertThat(row.getField(4)).isEqualTo(false);
                            assertThat(row.getField(5)).isEqualTo(88.3f);
                            assertThat(row.getField(6)).isEqualTo(new BigDecimal("987.65"));
                            assertThat(row.getField(7)).isNotNull();
                        });
    }

    /**
     * Tests all supported Cassandra primitive types including TINYINT, SMALLINT, BIGINT, CHAR,
     * VARCHAR, ASCII, DECIMAL, VARINT, DATE, TIME, BINARY, UUID, TIMEUUID, INET. Validates
     * comprehensive type coverage and correct mapper selection.
     */
    @Test
    void testAllSupportedPrimitiveTypes() throws Exception {
        String createFlinkTable =
                "CREATE TABLE flink_all_primitives ("
                        + "  id INT,"
                        + "  text_col STRING,"
                        + "  varchar_col STRING,"
                        + "  ascii_col STRING,"
                        + "  int_col INT,"
                        + "  bigint_col BIGINT,"
                        + "  smallint_col SMALLINT,"
                        + "  tinyint_col TINYINT,"
                        + "  float_col FLOAT,"
                        + "  double_col DOUBLE,"
                        + "  decimal_col DECIMAL(10,3),"
                        + "  varint_col DECIMAL(38,0),"
                        + "  boolean_col BOOLEAN,"
                        + "  timestamp_col TIMESTAMP(3),"
                        + "  date_col DATE,"
                        + "  time_col TIME,"
                        + "  binary_data VARBINARY,"
                        + "  uuid_col STRING,"
                        + "  timeuuid_col STRING,"
                        + "  inet_col STRING"
                        + ") WITH ("
                        + "  'connector' = 'cassandra',"
                        + "  'hosts' = '"
                        + cassandraTestEnvironment.getContactPoint()
                        + "',"
                        + "  'port' = '"
                        + cassandraTestEnvironment.getPort()
                        + "',"
                        + "  'keyspace' = '"
                        + CassandraTestEnvironment.KEYSPACE
                        + "',"
                        + "  'table' = 'all_primitives',"
                        + "  'username' = 'cassandra',"
                        + "  'password' = 'cassandra'"
                        + ")";

        tableEnv.executeSql(createFlinkTable);

        Table result = tableEnv.sqlQuery("SELECT * FROM flink_all_primitives");

        List<Row> actualRows = collectResults(result, 1);

        assertThat(actualRows).hasSize(1);
        Row row = actualRows.get(0);

        assertThat(row.getField(0)).isEqualTo(1);
        assertThat(row.getField(1)).isEqualTo("Hello World");
        assertThat(row.getField(2)).isEqualTo("Varchar Text");
        assertThat(row.getField(3)).isEqualTo("ASCII");
        assertThat(row.getField(4)).isEqualTo(42);
        assertThat(row.getField(5)).isEqualTo(9223372036854775807L);
        assertThat(row.getField(6)).isEqualTo((short) 32767);
        assertThat(row.getField(7)).isEqualTo((byte) 127);
        assertThat(row.getField(8)).isEqualTo(3.14f);
        assertThat(row.getField(9)).isEqualTo(2.718281828);
        assertThat(row.getField(10)).isNotNull();
        /** DECIMAL */
        assertThat(row.getField(11)).isNotNull();
        /** VARINT as DECIMAL */
        assertThat(row.getField(12)).isEqualTo(true);
        assertThat(row.getField(13)).isNotNull();
        /** TIMESTAMP */
        assertThat(row.getField(14)).isNotNull();
        /** DATE */
        assertThat(row.getField(15)).isNotNull();
        /** TIME */
        assertThat(row.getField(16)).isNotNull();
        /** BINARY */
        assertThat(row.getField(17)).isNotNull();
        /** UUID as STRING */
        assertThat(row.getField(18)).isNotNull();
        /** TIMEUUID as STRING */
        assertThat(row.getField(19)).isEqualTo("192.168.1.1");
        /** INET as STRING */
    }

    /**
     * Tests all collection types including ARRAY (list), MULTISET (set), MAP, and nested
     * collections. Validates that collection mappers correctly handle different element types and
     * nesting.
     */
    @Test
    void testAllCollectionTypes() throws Exception {
        String createFlinkTable =
                "CREATE TABLE flink_collections ("
                        + "  id INT,"
                        + "  list_text ARRAY<STRING>,"
                        + "  list_int ARRAY<INT>,"
                        + "  list_double ARRAY<DOUBLE>,"
                        + "  list_boolean ARRAY<BOOLEAN>,"
                        + "  set_text MULTISET<STRING>,"
                        + "  set_int MULTISET<INT>,"
                        + "  map_text_int MAP<STRING, INT>,"
                        + "  map_int_text MAP<INT, STRING>,"
                        + "  map_text_boolean MAP<STRING, BOOLEAN>,"
                        + "  list_of_list ARRAY<ARRAY<STRING>>,"
                        + "  map_of_list MAP<STRING, ARRAY<INT>>,"
                        + "  set_of_map MULTISET<MAP<STRING, INT>>"
                        + ") WITH ("
                        + "  'connector' = 'cassandra',"
                        + "  'hosts' = '"
                        + cassandraTestEnvironment.getContactPoint()
                        + "',"
                        + "  'port' = '"
                        + cassandraTestEnvironment.getPort()
                        + "',"
                        + "  'keyspace' = '"
                        + CassandraTestEnvironment.KEYSPACE
                        + "',"
                        + "  'table' = 'collections_table',"
                        + "  'username' = 'cassandra',"
                        + "  'password' = 'cassandra'"
                        + ")";

        tableEnv.executeSql(createFlinkTable);

        Table result = tableEnv.sqlQuery("SELECT * FROM flink_collections");

        List<Row> actualRows = collectResults(result, 1);

        assertThat(actualRows).hasSize(1);
        Row row = actualRows.get(0);

        assertThat(row.getField(0)).isEqualTo(1);

        /** Test ARRAY types */
        Object[] listText = (Object[]) row.getField(1);
        assertThat(listText).containsExactly("apple", "banana", "cherry");

        Object[] listInt = (Object[]) row.getField(2);
        assertThat(listInt).containsExactly(1, 2, 3, 4, 5);

        Object[] listDouble = (Object[]) row.getField(3);
        assertThat(listDouble).containsExactly(1.1, 2.2, 3.3);

        Object[] listBoolean = (Object[]) row.getField(4);
        assertThat(listBoolean).containsExactly(true, false, true);

        /** Test MULTISET types (mapped as MAP of element to count) */
        @SuppressWarnings("unchecked")
        Map<String, Integer> setText = (Map<String, Integer>) row.getField(5);
        assertThat(setText.keySet()).containsExactlyInAnyOrder("red", "green", "blue");
        assertThat(setText.values()).allMatch(count -> count == 1);

        @SuppressWarnings("unchecked")
        Map<Integer, Integer> setInt = (Map<Integer, Integer>) row.getField(6);
        assertThat(setInt.keySet()).containsExactlyInAnyOrder(10, 20, 30);
        assertThat(setInt.values()).allMatch(count -> count == 1);

        /** Test MAP types */
        assertThat(row.getField(7)).isNotNull();
        /** map_text_int */
        assertThat(row.getField(8)).isNotNull();
        /** map_int_text */
        assertThat(row.getField(9)).isNotNull();
        /** map_text_boolean */

        /** Test nested collections */
        assertThat(row.getField(10)).isNotNull();
        /** list_of_list */
        assertThat(row.getField(11)).isNotNull();
        /** map_of_list */
        assertThat(row.getField(12)).isNotNull();
        /** set_of_map */
    }

    /**
     * Tests User-Defined Types (UDTs) mapped to ROW types, including simple UDTs, collections of
     * UDTs, and nested UDT structures. Validates that RowMapper correctly handles UDTValue objects.
     */
    @Test
    void testUserDefinedTypesWithROW() throws Exception {
        String createFlinkTable =
                "CREATE TABLE flink_complex ("
                        + "  id INT,"
                        + "  user_address ROW<street STRING, city STRING, zipcode INT, country STRING>,"
                        + "  contact_col ROW<email STRING, phone STRING, preferred BOOLEAN>,"
                        + "  phone_numbers ARRAY<STRING>,"
                        + "  preferences MAP<STRING, BOOLEAN>,"
                        + "  list_address ARRAY<ROW<street STRING, city STRING, zipcode INT, country STRING>>,"
                        + "  map_text_address MAP<STRING, ROW<street STRING, city STRING, zipcode INT, country STRING>>,"
                        + "  set_contact MULTISET<ROW<email STRING, phone STRING, preferred BOOLEAN>>"
                        + ") WITH ("
                        + "  'connector' = 'cassandra',"
                        + "  'hosts' = '"
                        + cassandraTestEnvironment.getContactPoint()
                        + "',"
                        + "  'port' = '"
                        + cassandraTestEnvironment.getPort()
                        + "',"
                        + "  'keyspace' = '"
                        + CassandraTestEnvironment.KEYSPACE
                        + "',"
                        + "  'table' = 'complex_table',"
                        + "  'username' = 'cassandra',"
                        + "  'password' = 'cassandra'"
                        + ")";

        tableEnv.executeSql(createFlinkTable);

        Table result = tableEnv.sqlQuery("SELECT * FROM flink_complex");

        List<Row> actualRows = collectResults(result, 1);

        assertThat(actualRows).hasSize(1);
        Row row = actualRows.get(0);

        assertThat(row.getField(0)).isEqualTo(1);

        /** Test simple UDT as ROW */
        Row address = (Row) row.getField(1);
        assertThat(address.getField(0)).isEqualTo("123 Main St");
        /** street */
        assertThat(address.getField(1)).isEqualTo("New York");
        /** city */
        assertThat(address.getField(2)).isEqualTo(10001);
        /** zipcode */
        assertThat(address.getField(3)).isEqualTo("USA");
        /** country */
        Row contact = (Row) row.getField(2);
        assertThat(contact.getField(0)).isEqualTo("test@example.com");
        /** email */
        assertThat(contact.getField(1)).isEqualTo("555-1234");
        /** phone */
        assertThat(contact.getField(2)).isEqualTo(true);
        /** preferred */

        /** Test collections with primitive elements */
        Object[] phones = (Object[]) row.getField(3);
        assertThat(phones).containsExactly("555-1234", "555-5678");

        assertThat(row.getField(4)).isNotNull();
        /** preferences map */

        /** Test collections of UDTs */
        assertThat(row.getField(5)).isNotNull();
        /** list_address */
        assertThat(row.getField(6)).isNotNull();
        /** map_text_address */
        assertThat(row.getField(7)).isNotNull();
        /** set_contact */
    }

    /**
     * Tests Cassandra tuple types mapped to ROW types, including simple tuples, complex tuples with
     * multiple types, and nested tuples with collections. Validates that RowMapper correctly
     * handles TupleValue objects.
     */
    @Test
    void testTupleTypesWithROW() throws Exception {
        String createFlinkTable =
                "CREATE TABLE flink_tuples ("
                        + "  id INT,"
                        + "  simple_tuple ROW<f0 STRING, f1 INT>,"
                        + "  complex_tuple ROW<f0 STRING, f1 INT, f2 BOOLEAN, f3 DOUBLE>,"
                        + "  nested_tuple ROW<f0 STRING, f1 ARRAY<INT>, f2 MAP<STRING, BOOLEAN>>,"
                        + "  list_of_tuples ARRAY<ROW<f0 STRING, f1 INT>>,"
                        + "  map_with_tuple_key MAP<ROW<f0 STRING, f1 INT>, STRING>,"
                        + "  map_with_tuple_value MAP<STRING, ROW<f0 INT, f1 BOOLEAN>>"
                        + ") WITH ("
                        + "  'connector' = 'cassandra',"
                        + "  'hosts' = '"
                        + cassandraTestEnvironment.getContactPoint()
                        + "',"
                        + "  'port' = '"
                        + cassandraTestEnvironment.getPort()
                        + "',"
                        + "  'keyspace' = '"
                        + CassandraTestEnvironment.KEYSPACE
                        + "',"
                        + "  'table' = 'tuple_types',"
                        + "  'username' = 'cassandra',"
                        + "  'password' = 'cassandra'"
                        + ")";

        tableEnv.executeSql(createFlinkTable);

        Table result = tableEnv.sqlQuery("SELECT * FROM flink_tuples");

        List<Row> actualRows = collectResults(result, 1);

        assertThat(actualRows).hasSize(1);
        Row row = actualRows.get(0);

        assertThat(row.getField(0)).isEqualTo(1);

        /** Test simple tuple */
        Row simpleTuple = (Row) row.getField(1);
        assertThat(simpleTuple.getField(0)).isEqualTo("Hello");
        assertThat(simpleTuple.getField(1)).isEqualTo(42);

        /** Test complex tuple with multiple types */
        Row complexTuple = (Row) row.getField(2);
        assertThat(complexTuple.getField(0)).isEqualTo("Complex");
        assertThat(complexTuple.getField(1)).isEqualTo(99);
        assertThat(complexTuple.getField(2)).isEqualTo(true);
        assertThat(complexTuple.getField(3)).isEqualTo(3.14);

        /** Test nested tuple with collections */
        Row nestedTuple = (Row) row.getField(3);
        assertThat(nestedTuple.getField(0)).isEqualTo("Nested");
        assertThat(nestedTuple.getField(1)).isNotNull();
        /** array */
        assertThat(nestedTuple.getField(2)).isNotNull();
        /** map */

        /** Test collections of tuples */
        assertThat(row.getField(4)).isNotNull();
        /** list_of_tuples */
        assertThat(row.getField(5)).isNotNull();
        /** map_with_tuple_key */
        assertThat(row.getField(6)).isNotNull();
        /** map_with_tuple_value */
    }

    /**
     * Tests deeply nested structures with multiple levels of UDTs, collections, and tuples.
     * Validates that recursive field mapping works correctly for complex real-world schemas.
     */
    @Test
    void testDeeplyNestedStructures() throws Exception {
        String createFlinkTable =
                "CREATE TABLE flink_deep_nested ("
                        + "  id INT,"
                        + "  employee_data ROW<"
                        + "    id INT, "
                        + "    name STRING, "
                        + "    address ROW<street STRING, city STRING, zipcode INT, country STRING>, "
                        + "    contacts ARRAY<ROW<email STRING, phone STRING, preferred BOOLEAN>>, "
                        + "    company ROW<"
                        + "      name STRING, "
                        + "      employees ARRAY<STRING>, "
                        + "      departments MULTISET<STRING>, "
                        + "      budget_by_dept MAP<STRING, DECIMAL>"
                        + "    >"
                        + "  >,"
                        + "  employee_list ARRAY<ROW<"
                        + "    id INT, "
                        + "    name STRING, "
                        + "    address ROW<street STRING, city STRING, zipcode INT, country STRING>, "
                        + "    contacts ARRAY<ROW<email STRING, phone STRING, preferred BOOLEAN>>, "
                        + "    company ROW<"
                        + "      name STRING, "
                        + "      employees ARRAY<STRING>, "
                        + "      departments MULTISET<STRING>, "
                        + "      budget_by_dept MAP<STRING, DECIMAL>"
                        + "    >"
                        + "  >>,"
                        + "  employee_map MAP<STRING, ROW<"
                        + "    id INT, "
                        + "    name STRING, "
                        + "    address ROW<street STRING, city STRING, zipcode INT, country STRING>, "
                        + "    contacts ARRAY<ROW<email STRING, phone STRING, preferred BOOLEAN>>, "
                        + "    company ROW<"
                        + "      name STRING, "
                        + "      employees ARRAY<STRING>, "
                        + "      departments MULTISET<STRING>, "
                        + "      budget_by_dept MAP<STRING, DECIMAL>"
                        + "    >"
                        + "  >>"
                        + ") WITH ("
                        + "  'connector' = 'cassandra',"
                        + "  'hosts' = '"
                        + cassandraTestEnvironment.getContactPoint()
                        + "',"
                        + "  'port' = '"
                        + cassandraTestEnvironment.getPort()
                        + "',"
                        + "  'keyspace' = '"
                        + CassandraTestEnvironment.KEYSPACE
                        + "',"
                        + "  'table' = 'deep_nested',"
                        + "  'username' = 'cassandra',"
                        + "  'password' = 'cassandra'"
                        + ")";

        tableEnv.executeSql(createFlinkTable);

        Table result = tableEnv.sqlQuery("SELECT * FROM flink_deep_nested");

        List<Row> actualRows = collectResults(result, 1);

        assertThat(actualRows).hasSize(1);
        Row row = actualRows.get(0);

        assertThat(row.getField(0)).isEqualTo(1);

        /** Test deeply nested employee data */
        Row employeeData = (Row) row.getField(1);
        assertThat(employeeData.getField(0)).isEqualTo(123);
        /** id */
        assertThat(employeeData.getField(1)).isEqualTo("John Doe");
        /** name */
        Row address = (Row) employeeData.getField(2);
        assertThat(address.getField(0)).isEqualTo("123 Work St");
        /** street */
        assertThat(address.getField(1)).isEqualTo("Work City");
        /** city */
        Object[] contacts = (Object[]) employeeData.getField(3);
        assertThat(contacts).isNotEmpty();

        Row company = (Row) employeeData.getField(4);
        assertThat(company.getField(0)).isEqualTo("TechCorp");
        /** name */

        /** Test collections of deeply nested structures */
        Object[] employeeList = (Object[]) row.getField(2);
        assertThat(employeeList).isNotNull().isNotEmpty();

        assertThat(row.getField(3)).isNotNull();
        /** employee_map */
    }

    /**
     * Tests edge cases including empty collections, null values, single-element collections, large
     * binary data, and partial UDT structures. Validates robust handling of boundary conditions and
     * real-world data variations.
     */
    @Test
    void testEdgeCasesAndNullHandling() throws Exception {
        String createFlinkTable =
                "CREATE TABLE flink_edge_cases ("
                        + "  id INT,"
                        + "  empty_list ARRAY<STRING>,"
                        + "  empty_set MULTISET<INT>,"
                        + "  empty_map MAP<STRING, BOOLEAN>,"
                        + "  list_with_values ARRAY<STRING>,"
                        + "  map_with_values MAP<STRING, STRING>,"
                        + "  single_list ARRAY<DOUBLE>,"
                        + "  single_set MULTISET<STRING>,"
                        + "  single_map MAP<INT, STRING>,"
                        + "  list_of_empty_lists ARRAY<ARRAY<STRING>>,"
                        + "  map_of_empty_maps MAP<STRING, MAP<STRING, INT>>,"
                        + "  large_blob VARBINARY,"
                        + "  tuple_with_nulls ROW<field1 STRING, field2 INT, field3 BOOLEAN>,"
                        + "  partial_udt ROW<street STRING, city STRING, zipcode INT, country STRING>"
                        + ") WITH ("
                        + "  'connector' = 'cassandra',"
                        + "  'hosts' = '"
                        + cassandraTestEnvironment.getContactPoint()
                        + "',"
                        + "  'port' = '"
                        + cassandraTestEnvironment.getPort()
                        + "',"
                        + "  'keyspace' = '"
                        + CassandraTestEnvironment.KEYSPACE
                        + "',"
                        + "  'table' = 'edge_cases',"
                        + "  'username' = 'cassandra',"
                        + "  'password' = 'cassandra'"
                        + ")";

        tableEnv.executeSql(createFlinkTable);

        Table result = tableEnv.sqlQuery("SELECT * FROM flink_edge_cases");

        List<Row> actualRows = collectResults(result, 1);

        assertThat(actualRows).hasSize(1);
        Row row = actualRows.get(0);

        assertThat(row.getField(0)).isEqualTo(1);

        /** Test empty collections (Cassandra stores as null for empty collections) */
        assertThat(row.getField(1)).isNull();
        /** empty_list */
        assertThat(row.getField(2)).isNull();
        /** empty_set */
        assertThat(row.getField(3)).isNull();
        /** empty_map */

        /** Test collections with values */
        Object[] listWithValues = (Object[]) row.getField(4);
        assertThat(listWithValues).containsExactly("value1", "value2", "value3");

        assertThat(row.getField(5)).isNotNull();
        /** map_with_values */

        /** Test single-element collections */
        Object[] singleList = (Object[]) row.getField(6);
        assertThat(singleList).containsExactly(3.14159);

        assertThat(row.getField(7)).isNotNull();
        /** single_set */
        assertThat(row.getField(8)).isNotNull();
        /** single_map */

        /** Test nested empty collections */
        Object[] listOfEmptyLists = (Object[]) row.getField(9);
        assertThat(listOfEmptyLists).hasSize(2);
        /** First list is empty (null), second has one item */
        assertThat((Object[]) listOfEmptyLists[1]).containsExactly("item");

        assertThat(row.getField(10)).isNotNull();
        /** map_of_empty_maps */

        /** Test large binary data */
        assertThat(row.getField(11)).isNotNull();
        /** large_blob */

        /** Test tuple and UDT with nulls */
        assertThat(row.getField(12)).isNotNull();
        /** tuple_with_nulls */
        assertThat(row.getField(13)).isNotNull();
        /** partial_udt */
    }

    /**
     * Tests extremely complex nested structures with multiple levels of nesting, mixed collection
     * types, and complex tuple/UDT combinations. Validates that the mapper can handle real-world
     * complex schemas at scale.
     */
    @Test
    void testMegaComplexScenarios() throws Exception {
        String createFlinkTable =
                "CREATE TABLE flink_mega_complex ("
                        + "  id INT,"
                        + "  ultimate_complex MAP<ROW<text_field STRING, int_field INT>, ARRAY<MAP<STRING, ROW<"
                        + "    id INT, "
                        + "    name STRING, "
                        + "    address ROW<street STRING, city STRING, zipcode INT, country STRING>, "
                        + "    contacts ARRAY<ROW<email STRING, phone STRING, preferred BOOLEAN>>, "
                        + "    company ROW<"
                        + "      name STRING, "
                        + "      employees ARRAY<STRING>, "
                        + "      departments MULTISET<STRING>, "
                        + "      budget_by_dept MAP<STRING, DECIMAL>"
                        + "    >"
                        + "  >>>>,"
                        + "  tuple_collection_madness ARRAY<ROW<f0 STRING, f1 ARRAY<INT>, f2 MAP<STRING, BOOLEAN>, f3 MULTISET<DOUBLE>>>,"
                        + "  numeric_soup ARRAY<MAP<STRING, ROW<f0 TINYINT, f1 SMALLINT, f2 INT, f3 BIGINT, f4 FLOAT, f5 DOUBLE, f6 DECIMAL, f7 DECIMAL>>>"
                        + ") WITH ("
                        + "  'connector' = 'cassandra',"
                        + "  'hosts' = '"
                        + cassandraTestEnvironment.getContactPoint()
                        + "',"
                        + "  'port' = '"
                        + cassandraTestEnvironment.getPort()
                        + "',"
                        + "  'keyspace' = '"
                        + CassandraTestEnvironment.KEYSPACE
                        + "',"
                        + "  'table' = 'mega_complex',"
                        + "  'username' = 'cassandra',"
                        + "  'password' = 'cassandra'"
                        + ")";

        tableEnv.executeSql(createFlinkTable);

        Table result = tableEnv.sqlQuery("SELECT * FROM flink_mega_complex");

        List<Row> actualRows = collectResults(result, 1);

        assertThat(actualRows).hasSize(1);
        Row row = actualRows.get(0);

        assertThat(row.getField(0)).isEqualTo(1);

        /** Test ultimate complex nested structure */
        assertThat(row.getField(1)).isNotNull();
        /** ultimate_complex */

        /** Test tuple collection madness */
        Object[] tupleCollectionMadness = (Object[]) row.getField(2);
        assertThat(tupleCollectionMadness).isNotNull().isNotEmpty();

        /** Test numeric soup with all numeric types */
        Object[] numericSoup = (Object[]) row.getField(3);
        assertThat(numericSoup).isNotNull().isNotEmpty();
    }

    /**
     * Tests comprehensive null handling scenarios including nullable UDTs, partial UDTs, optional
     * collections, large binary data, and edge values like empty strings and zero. Validates that
     * null values are properly handled throughout the type hierarchy.
     */
    @Test
    void testComprehensiveNullHandling() throws Exception {
        String createFlinkTable =
                "CREATE TABLE flink_null_handling ("
                        + "  id INT,"
                        + "  nullable_udt ROW<street STRING, city STRING, zipcode INT, country STRING>,"
                        + "  partial_udt ROW<street STRING, city STRING, zipcode INT, country STRING>,"
                        + "  optional_list ARRAY<STRING>,"
                        + "  optional_set MULTISET<INT>,"
                        + "  optional_map MAP<STRING, STRING>,"
                        + "  large_binary_data VARBINARY,"
                        + "  very_large_binary VARBINARY,"
                        + "  empty_text STRING,"
                        + "  zero_int INT,"
                        + "  false_boolean BOOLEAN"
                        + ") WITH ("
                        + "  'connector' = 'cassandra',"
                        + "  'hosts' = '"
                        + cassandraTestEnvironment.getContactPoint()
                        + "',"
                        + "  'port' = '"
                        + cassandraTestEnvironment.getPort()
                        + "',"
                        + "  'keyspace' = '"
                        + CassandraTestEnvironment.KEYSPACE
                        + "',"
                        + "  'table' = 'null_handling',"
                        + "  'username' = 'cassandra',"
                        + "  'password' = 'cassandra'"
                        + ")";

        tableEnv.executeSql(createFlinkTable);

        Table result = tableEnv.sqlQuery("SELECT * FROM flink_null_handling");

        List<Row> rows = collectResults(result, 2);
        assertThat(rows).hasSize(2);

        Row nullRow = rows.stream().filter(r -> r.getField(0).equals(1)).findFirst().orElse(null);
        Row populatedRow =
                rows.stream().filter(r -> r.getField(0).equals(2)).findFirst().orElse(null);

        assertThat(nullRow).isNotNull();
        assertThat(populatedRow).isNotNull();

        /** Test null handling in first row */
        assertThat(nullRow.getField(0)).isEqualTo(1);
        assertThat(nullRow.getField(1)).isNull();
        /** nullable_udt */
        assertThat(nullRow.getField(2)).isNotNull();
        /** partial_udt (has some fields) */
        assertThat(nullRow.getField(3)).isNull();
        /** optional_list */
        assertThat(nullRow.getField(4)).isNull();
        /** optional_set */
        assertThat(nullRow.getField(5)).isNull();
        /** optional_map */
        assertThat(nullRow.getField(6)).isNotNull();
        /** large_binary_data */
        assertThat(nullRow.getField(7)).isNotNull();
        /** very_large_binary */
        assertThat(nullRow.getField(8)).isEqualTo("");
        /** empty_text */
        assertThat(nullRow.getField(9)).isEqualTo(0);
        /** zero_int */
        assertThat(nullRow.getField(10)).isEqualTo(false);
        /** false_boolean */

        /** Test populated values in second row */
        assertThat(populatedRow.getField(0)).isEqualTo(2);
        assertThat(populatedRow.getField(1)).isNotNull();
        /** nullable_udt */
        assertThat(populatedRow.getField(2)).isNotNull();
        /** partial_udt */
        Object[] optionalList = (Object[]) populatedRow.getField(3);
        assertThat(optionalList).containsExactly("item1", "item2", "item3");

        @SuppressWarnings("unchecked")
        Map<Integer, Integer> optionalSet = (Map<Integer, Integer>) populatedRow.getField(4);
        assertThat(optionalSet.keySet()).containsExactlyInAnyOrder(100, 200, 300);

        @SuppressWarnings("unchecked")
        Map<String, String> optionalMap = (Map<String, String>) populatedRow.getField(5);
        assertThat(optionalMap).containsEntry("key1", "value1").containsEntry("key2", "value2");

        assertThat(populatedRow.getField(6)).isNotNull();
        /** large_binary_data */
        assertThat(populatedRow.getField(7)).isNotNull();
        /** very_large_binary */
        assertThat(populatedRow.getField(8)).isEqualTo("populated text");
        /** empty_text */
        assertThat(populatedRow.getField(9)).isEqualTo(42);
        /** zero_int */
        assertThat(populatedRow.getField(10)).isEqualTo(true);
        /** false_boolean */
    }

    /**
     * Tests nested collection scenarios including nested arrays and nested maps. Validates that
     * recursive collection mapping works correctly for complex nested structures.
     */
    @Test
    void testNestedCollectionScenarios() throws Exception {
        String createFlinkTable =
                "CREATE TABLE flink_nested ("
                        + "  id INT,"
                        + "  nested_list ARRAY<ARRAY<INT>>,"
                        + "  nested_map MAP<STRING, MAP<STRING, INT>>"
                        + ") WITH ("
                        + "  'connector' = 'cassandra',"
                        + "  'hosts' = '"
                        + cassandraTestEnvironment.getContactPoint()
                        + "',"
                        + "  'port' = '"
                        + cassandraTestEnvironment.getPort()
                        + "',"
                        + "  'keyspace' = '"
                        + CassandraTestEnvironment.KEYSPACE
                        + "',"
                        + "  'table' = 'nested_collections',"
                        + "  'username' = 'cassandra',"
                        + "  'password' = 'cassandra'"
                        + ")";

        tableEnv.executeSql(createFlinkTable);

        Table result = tableEnv.sqlQuery("SELECT * FROM flink_nested");

        List<Row> actualRows = collectResults(result, 1);

        assertThat(actualRows).hasSize(1);
        Row row = actualRows.get(0);

        assertThat(row.getField(0)).isEqualTo(1);

        /** Test nested array */
        Object[] nestedList = (Object[]) row.getField(1);
        assertThat(nestedList).hasSize(2);
        assertThat((Object[]) nestedList[0]).containsExactly(1, 2);
        assertThat((Object[]) nestedList[1]).containsExactly(3, 4);

        /** Test nested map */
        @SuppressWarnings("unchecked")
        Map<String, Map<String, Integer>> nestedMap =
                (Map<String, Map<String, Integer>>) row.getField(2);
        assertThat(nestedMap).hasSize(1);
        assertThat(nestedMap.get("outer1")).containsEntry("inner1", 10).containsEntry("inner2", 20);
    }

    /**
     * Tests field projection and query optimization to ensure that only requested fields are
     * processed and that the connector works correctly with Flink's query planning.
     */
    @Test
    void testFieldProjectionAndQueryOptimization() throws Exception {
        String createFlinkTable =
                "CREATE TABLE flink_primitives ("
                        + "  id INT,"
                        + "  name STRING,"
                        + "  age INT,"
                        + "  salary DOUBLE,"
                        + "  active BOOLEAN"
                        + ") WITH ("
                        + "  'connector' = 'cassandra',"
                        + "  'hosts' = '"
                        + cassandraTestEnvironment.getContactPoint()
                        + "',"
                        + "  'port' = '"
                        + cassandraTestEnvironment.getPort()
                        + "',"
                        + "  'keyspace' = '"
                        + CassandraTestEnvironment.KEYSPACE
                        + "',"
                        + "  'table' = 'primitives_table',"
                        + "  'username' = 'cassandra',"
                        + "  'password' = 'cassandra'"
                        + ")";

        tableEnv.executeSql(createFlinkTable);

        /** Test projection - only select specific fields */
        Table result = tableEnv.sqlQuery("SELECT name, age FROM flink_primitives");

        List<Row> actualRows = collectResults(result, 2);

        assertThat(actualRows).hasSize(2);
        assertThat(actualRows)
                .anySatisfy(
                        row -> {
                            assertThat(row.getArity()).isEqualTo(2);
                            assertThat(row.getField(0)).isIn("Alice", "Bob");
                            assertThat(row.getField(1)).isIn(30, 25);
                        });
    }

    /**
     * Tests custom query configuration option to ensure that users can specify custom CQL queries
     * instead of using table name.
     */
    @Test
    void testCustomQueryConfiguration() throws Exception {
        String createFlinkTable =
                "CREATE TABLE flink_custom_query ("
                        + "  id INT,"
                        + "  name STRING,"
                        + "  age INT"
                        + ") WITH ("
                        + "  'connector' = 'cassandra',"
                        + "  'hosts' = '"
                        + cassandraTestEnvironment.getContactPoint()
                        + "',"
                        + "  'port' = '"
                        + cassandraTestEnvironment.getPort()
                        + "',"
                        + "  'keyspace' = '"
                        + CassandraTestEnvironment.KEYSPACE
                        + "',"
                        + "  'query' = 'SELECT id, name, age FROM "
                        + CassandraTestEnvironment.KEYSPACE
                        + ".primitives_table;',"
                        + "  'username' = 'cassandra',"
                        + "  'password' = 'cassandra'"
                        + ")";

        tableEnv.executeSql(createFlinkTable);

        Table result =
                tableEnv.sqlQuery(
                        "SELECT id as user_id, name as full_name, age as years_old FROM flink_custom_query");

        List<Row> actualRows = collectResults(result, 2);

        assertThat(actualRows).hasSize(2);
        assertThat(actualRows)
                .anySatisfy(
                        row -> {
                            assertThat(row.getField(0)).isIn(1, 2);
                            assertThat(row.getField(1)).isIn("Alice", "Bob");
                            assertThat(row.getField(2)).isIn(30, 25);
                        });
    }

    private List<Row> collectResults(Table table, int expectedCount) throws Exception {
        List<Row> actualRows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = table.execute().collect()) {
            int count = 0;
            while (iterator.hasNext() && count < expectedCount) {
                actualRows.add(iterator.next());
                count++;
            }
        }
        return actualRows;
    }
}
