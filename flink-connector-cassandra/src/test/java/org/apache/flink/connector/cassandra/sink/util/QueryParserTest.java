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

package org.apache.flink.connector.cassandra.sink.util;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link QueryParser}. */
class QueryParserTest {

    @Test
    void testParseBasicInsertQuery() {
        String query =
                "INSERT INTO analytics.events (user_id, event_time, event_type) VALUES (?, ?, ?)";

        QueryParser.QueryInfo queryInfo = QueryParser.parseInsertQuery(query);

        assertThat(queryInfo.getKeyspace()).isEqualTo("analytics");
        assertThat(queryInfo.getTableName()).isEqualTo("events");
        assertThat(queryInfo.getColumnNames())
                .containsExactly("user_id", "event_time", "event_type");
    }

    @Test
    void testParseBasicUpdateQuery() {
        String query = "UPDATE analytics.events SET event_type=?, last_seen=? WHERE user_id=?";

        QueryParser.UpdateQueryInfo updateInfo = QueryParser.parseUpdateQuery(query);

        assertThat(updateInfo.getKeyspace()).isEqualTo("analytics");
        assertThat(updateInfo.getTableName()).isEqualTo("events");
        assertThat(updateInfo.getSetColumns()).containsExactly("event_type", "last_seen");
        assertThat(updateInfo.getWhereColumns()).containsExactly("user_id");
    }

    @Test
    void testParseInsertWithQuotedIdentifiers() {
        String query =
                "INSERT INTO \"MixedCase\".\"user-profile\" (\"user-id\", \"full name\") VALUES (?, ?)";

        QueryParser.QueryInfo queryInfo = QueryParser.parseInsertQuery(query);

        assertThat(queryInfo.getKeyspace()).isEqualTo("MixedCase");
        assertThat(queryInfo.getTableName()).isEqualTo("user-profile");
        assertThat(queryInfo.getColumnNames()).containsExactly("user-id", "full name");
    }

    @Test
    void testParseUpdateWithQuotedIdentifiers() {
        String query = "UPDATE \"keyspace\".\"table\" SET \"mixed-case\"=? WHERE \"id\"=?";

        QueryParser.UpdateQueryInfo updateInfo = QueryParser.parseUpdateQuery(query);

        assertThat(updateInfo.getKeyspace()).isEqualTo("keyspace");
        assertThat(updateInfo.getTableName()).isEqualTo("table");
        assertThat(updateInfo.getSetColumns()).containsExactly("mixed-case");
        assertThat(updateInfo.getWhereColumns()).containsExactly("id");
    }

    @Test
    void testParseInsertWithUsingClause() {
        String query = "INSERT INTO ks.table (id, name) VALUES (?, ?) USING TTL 3600";

        QueryParser.QueryInfo queryInfo = QueryParser.parseInsertQuery(query);

        assertThat(queryInfo.getKeyspace()).isEqualTo("ks");
        assertThat(queryInfo.getTableName()).isEqualTo("table");
        assertThat(queryInfo.getColumnNames()).containsExactly("id", "name");
    }

    @Test
    void testParseUpdateWithUsingClause() {
        String query = "UPDATE ks.table USING TTL 3600 SET name=? WHERE id=?";

        QueryParser.UpdateQueryInfo updateInfo = QueryParser.parseUpdateQuery(query);

        assertThat(updateInfo.getKeyspace()).isEqualTo("ks");
        assertThat(updateInfo.getTableName()).isEqualTo("table");
        assertThat(updateInfo.getSetColumns()).containsExactly("name");
        assertThat(updateInfo.getWhereColumns()).containsExactly("id");
    }

    @Test
    void testParseQueryWithSemicolon() {
        String query = "INSERT INTO ks.table (id) VALUES (?);";

        QueryParser.QueryInfo queryInfo = QueryParser.parseInsertQuery(query);

        assertThat(queryInfo.getKeyspace()).isEqualTo("ks");
        assertThat(queryInfo.getTableName()).isEqualTo("table");
        assertThat(queryInfo.getColumnNames()).containsExactly("id");
    }

    @Test
    void testParseUpdateWithMultipleWhereConditions() {
        String query = "UPDATE ks.table SET col1=?, col2=? WHERE pk1=? AND pk2=? AND ck1=?";

        QueryParser.UpdateQueryInfo updateInfo = QueryParser.parseUpdateQuery(query);

        assertThat(updateInfo.getKeyspace()).isEqualTo("ks");
        assertThat(updateInfo.getTableName()).isEqualTo("table");
        assertThat(updateInfo.getSetColumns()).containsExactly("col1", "col2");
        assertThat(updateInfo.getWhereColumns()).containsExactly("pk1", "pk2", "ck1");
    }

    @Test
    void testParseInsertWithIfNotExists() {
        String query = "INSERT INTO ks.table (id, name) VALUES (?, ?) IF NOT EXISTS";

        QueryParser.QueryInfo queryInfo = QueryParser.parseInsertQuery(query);

        assertThat(queryInfo.getKeyspace()).isEqualTo("ks");
        assertThat(queryInfo.getTableName()).isEqualTo("table");
        assertThat(queryInfo.getColumnNames()).containsExactly("id", "name");
    }

    @Test
    void testParseUpdateWithIfClause() {
        String query = "UPDATE ks.table SET name=? WHERE id=? IF name='old_value'";

        QueryParser.UpdateQueryInfo updateInfo = QueryParser.parseUpdateQuery(query);

        assertThat(updateInfo.getKeyspace()).isEqualTo("ks");
        assertThat(updateInfo.getTableName()).isEqualTo("table");
        assertThat(updateInfo.getSetColumns()).containsExactly("name");
        assertThat(updateInfo.getWhereColumns()).containsExactly("id");
    }

    @Test
    void testParseQueryWithEscapedQuotes() {
        String query = "INSERT INTO ks.table (\"col\"\"with\"\"quotes\") VALUES (?)";

        QueryParser.QueryInfo queryInfo = QueryParser.parseInsertQuery(query);

        assertThat(queryInfo.getKeyspace()).isEqualTo("ks");
        assertThat(queryInfo.getTableName()).isEqualTo("table");
        assertThat(queryInfo.getColumnNames()).containsExactly("col\"with\"quotes");
    }

    // Negative test cases

    @Test
    void testParseInsertFailsWithoutKeyspace() {
        String query = "INSERT INTO table (id) VALUES (?)";

        assertThatThrownBy(() -> QueryParser.parseInsertQuery(query))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Static mode requires fully qualified keyspace.table format");
    }

    @Test
    void testParseUpdateFailsWithoutKeyspace() {
        String query = "UPDATE table SET col=? WHERE id=?";

        assertThatThrownBy(() -> QueryParser.parseUpdateQuery(query))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Static mode requires fully qualified keyspace.table format");
    }

    @Test
    void testParseInsertFailsWithLiterals() {
        String query = "INSERT INTO ks.table (id, name) VALUES (123, 'literal')";

        assertThatThrownBy(() -> QueryParser.parseInsertQuery(query))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Static mode supports only parameter placeholders")
                .hasMessageContaining("Found: '123'");
    }

    @Test
    void testParseUpdateFailsWithLiteralsInSet() {
        String query = "UPDATE ks.table SET name='literal' WHERE id=?";

        assertThatThrownBy(() -> QueryParser.parseUpdateQuery(query))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Static mode supports only parameter placeholders")
                .hasMessageContaining("Found: ''literal''");
    }

    @Test
    void testParseUpdateFailsWithLiteralsInWhere() {
        String query = "UPDATE ks.table SET name=? WHERE id=123";

        assertThatThrownBy(() -> QueryParser.parseUpdateQuery(query))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "WHERE condition must contain at least one parameter placeholder")
                .hasMessageContaining("Found: 'id=123'");
    }

    @Test
    void testParseInsertFailsWithColumnValueCountMismatch() {
        String query =
                "INSERT INTO ks.table (id, name, email) VALUES (?, ?)"; // 3 columns, 2 values

        assertThatThrownBy(() -> QueryParser.parseInsertQuery(query))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Column count")
                .hasMessageContaining("must match value placeholder count");
    }

    @Test
    void testParseUpdateSucceedsWithNonEqualityOperator() {
        String query = "UPDATE ks.table SET name=? WHERE id > ?";

        QueryParser.UpdateQueryInfo info = QueryParser.parseUpdateQuery(query);
        assertThat(info.getKeyspace()).isEqualTo("ks");
        assertThat(info.getTableName()).isEqualTo("table");
        assertThat(info.getSetColumns()).containsExactly("name");
        assertThat(info.getWhereColumns()).containsExactly("id");
    }

    @Test
    void testParseValidationFailures() {
        // INSERT with invalid format - missing parentheses
        assertThatThrownBy(() -> QueryParser.parseInsertQuery("INSERT INTO ks.table id VALUES ?"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid INSERT query format");

        // UPDATE with invalid format - missing SET keyword
        assertThatThrownBy(() -> QueryParser.parseUpdateQuery("UPDATE ks.table name=? WHERE id=?"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid UPDATE query format");

        // Null queries
        assertThatThrownBy(() -> QueryParser.parseInsertQuery(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("insertQuery cannot be null or blank");

        assertThatThrownBy(() -> QueryParser.parseUpdateQuery(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("updateQuery cannot be null or blank");

        // Blank/empty queries
        assertThatThrownBy(() -> QueryParser.parseInsertQuery("   "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("insertQuery cannot be null or blank");

        assertThatThrownBy(() -> QueryParser.parseUpdateQuery(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("updateQuery cannot be null or blank");

        // INSERT with empty columns
        assertThatThrownBy(() -> QueryParser.parseInsertQuery("INSERT INTO ks.table () VALUES ()"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("At least one column must be specified");

        // UPDATE with empty SET clause
        assertThatThrownBy(() -> QueryParser.parseUpdateQuery("UPDATE ks.table SET WHERE id=?"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid UPDATE query format");

        // UPDATE with empty WHERE clause
        assertThatThrownBy(() -> QueryParser.parseUpdateQuery("UPDATE ks.table SET name=? WHERE"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid UPDATE query format");
    }

    // Edge cases

    @Test
    void testParseWithExtraWhitespace() {
        String query = "  INSERT   INTO   ks.table   (  id  ,  name  )   VALUES   (  ?  ,  ?  )  ";

        QueryParser.QueryInfo queryInfo = QueryParser.parseInsertQuery(query);

        assertThat(queryInfo.getKeyspace()).isEqualTo("ks");
        assertThat(queryInfo.getTableName()).isEqualTo("table");
        assertThat(queryInfo.getColumnNames()).containsExactly("id", "name");
    }

    @Test
    void testParseCaseInsensitive() {
        String query = "insert into ks.table (id, name) values (?, ?)";

        QueryParser.QueryInfo queryInfo = QueryParser.parseInsertQuery(query);

        assertThat(queryInfo.getKeyspace()).isEqualTo("ks");
        assertThat(queryInfo.getTableName()).isEqualTo("table");
        assertThat(queryInfo.getColumnNames()).containsExactly("id", "name");
    }

    @Test
    void testParseUpdateCaseInsensitive() {
        String query = "update ks.table set name=? where id=?";

        QueryParser.UpdateQueryInfo updateInfo = QueryParser.parseUpdateQuery(query);

        assertThat(updateInfo.getKeyspace()).isEqualTo("ks");
        assertThat(updateInfo.getTableName()).isEqualTo("table");
        assertThat(updateInfo.getSetColumns()).containsExactly("name");
        assertThat(updateInfo.getWhereColumns()).containsExactly("id");
    }

    @Test
    void testParseComplexRealWorldQuery() {
        String query =
                "INSERT INTO \"analytics\".\"user_events\" "
                        + "(\"user_id\", \"event_type\", \"timestamp\", \"properties\") "
                        + "VALUES (?, ?, ?, ?) "
                        + "USING TTL 86400 AND TIMESTAMP 1234567890";

        QueryParser.QueryInfo queryInfo = QueryParser.parseInsertQuery(query);

        assertThat(queryInfo.getKeyspace()).isEqualTo("analytics");
        assertThat(queryInfo.getTableName()).isEqualTo("user_events");
        assertThat(queryInfo.getColumnNames())
                .containsExactly("user_id", "event_type", "timestamp", "properties");
    }

    @Test
    void testToStringMethods() {
        QueryParser.QueryInfo queryInfo =
                new QueryParser.QueryInfo("ks", "table", Arrays.asList("id", "name"));

        // Test getters directly instead of relying on toString
        assertThat(queryInfo.getKeyspace()).isEqualTo("ks");
        assertThat(queryInfo.getTableName()).isEqualTo("table");
        assertThat(queryInfo.getColumnNames()).containsExactly("id", "name");

        // Test that toString() doesn't throw and returns something
        String result = queryInfo.toString();
        assertThat(result).isNotNull();
        assertThat(result).isNotEmpty();

        QueryParser.UpdateQueryInfo updateInfo =
                new QueryParser.UpdateQueryInfo(
                        "ks", "table", Arrays.asList("name"), Arrays.asList("id"));

        // Test getters directly
        assertThat(updateInfo.getKeyspace()).isEqualTo("ks");
        assertThat(updateInfo.getTableName()).isEqualTo("table");
        assertThat(updateInfo.getSetColumns()).containsExactly("name");
        assertThat(updateInfo.getWhereColumns()).containsExactly("id");

        // Test that toString() doesn't throw and returns something
        String updateResult = updateInfo.toString();
        assertThat(updateResult).isNotNull();
        assertThat(updateResult).isNotEmpty();
    }

    // Extended tests for various WHERE operators (merged from QueryParserExtendedTest)

    @Test
    void testUpdateWithInOperator() {
        String query = "UPDATE ks.table SET status=? WHERE id IN (?, ?, ?)";
        QueryParser.UpdateQueryInfo info = QueryParser.parseUpdateQuery(query);

        assertThat(info.getKeyspace()).isEqualTo("ks");
        assertThat(info.getTableName()).isEqualTo("table");
        assertThat(info.getSetColumns()).containsExactly("status");
        assertThat(info.getWhereColumns()).containsExactly("id");
    }

    @Test
    void testUpdateWithGreaterThanOperator() {
        String query = "UPDATE ks.table SET flag=? WHERE timestamp > ?";
        QueryParser.UpdateQueryInfo info = QueryParser.parseUpdateQuery(query);

        assertThat(info.getKeyspace()).isEqualTo("ks");
        assertThat(info.getTableName()).isEqualTo("table");
        assertThat(info.getSetColumns()).containsExactly("flag");
        assertThat(info.getWhereColumns()).containsExactly("timestamp");
    }

    @Test
    void testUpdateWithLessThanEqualOperator() {
        String query = "UPDATE ks.table SET active=? WHERE age <= ?";
        QueryParser.UpdateQueryInfo info = QueryParser.parseUpdateQuery(query);

        assertThat(info.getKeyspace()).isEqualTo("ks");
        assertThat(info.getTableName()).isEqualTo("table");
        assertThat(info.getSetColumns()).containsExactly("active");
        assertThat(info.getWhereColumns()).containsExactly("age");
    }

    @Test
    void testUpdateWithNotEqualOperator() {
        String query = "UPDATE ks.table SET deleted=? WHERE status != ?";
        QueryParser.UpdateQueryInfo info = QueryParser.parseUpdateQuery(query);

        assertThat(info.getKeyspace()).isEqualTo("ks");
        assertThat(info.getTableName()).isEqualTo("table");
        assertThat(info.getSetColumns()).containsExactly("deleted");
        assertThat(info.getWhereColumns()).containsExactly("status");
    }

    @Test
    void testUpdateWithMultipleOperators() {
        String query =
                "UPDATE ks.table SET flag=?, status=? WHERE id=? AND timestamp > ? AND type IN (?, ?)";
        QueryParser.UpdateQueryInfo info = QueryParser.parseUpdateQuery(query);

        assertThat(info.getKeyspace()).isEqualTo("ks");
        assertThat(info.getTableName()).isEqualTo("table");
        assertThat(info.getSetColumns()).containsExactly("flag", "status");
        assertThat(info.getWhereColumns()).containsExactlyInAnyOrder("id", "timestamp", "type");
    }

    @Test
    void testUpdateWithIfExistsAndInOperator() {
        String query = "UPDATE ks.table SET status=? WHERE id IN (?, ?, ?) IF EXISTS";
        QueryParser.UpdateQueryInfo info = QueryParser.parseUpdateQuery(query);

        assertThat(info.getKeyspace()).isEqualTo("ks");
        assertThat(info.getTableName()).isEqualTo("table");
        assertThat(info.getSetColumns()).containsExactly("status");
        assertThat(info.getWhereColumns()).containsExactly("id");
    }

    @Test
    void testUpdateWithUsingTTLAndInOperator() {
        String query = "UPDATE ks.table USING TTL 3600 SET status=? WHERE id IN (?, ?)";
        QueryParser.UpdateQueryInfo info = QueryParser.parseUpdateQuery(query);

        assertThat(info.getKeyspace()).isEqualTo("ks");
        assertThat(info.getTableName()).isEqualTo("table");
        assertThat(info.getSetColumns()).containsExactly("status");
        assertThat(info.getWhereColumns()).containsExactly("id");
    }

    @Test
    void testUpdateWithComplexIfCondition() {
        String query =
                "UPDATE ks.table SET status=? WHERE id=? IF status != 'deleted' AND timestamp > 1000";
        QueryParser.UpdateQueryInfo info = QueryParser.parseUpdateQuery(query);

        assertThat(info.getKeyspace()).isEqualTo("ks");
        assertThat(info.getTableName()).isEqualTo("table");
        assertThat(info.getSetColumns()).containsExactly("status");
        assertThat(info.getWhereColumns()).containsExactly("id");
    }

    @Test
    void testUpdateWithQuotedColumnAndInOperator() {
        String query = "UPDATE ks.table SET \"Status\"=? WHERE \"User-ID\" IN (?, ?, ?)";
        QueryParser.UpdateQueryInfo info = QueryParser.parseUpdateQuery(query);

        assertThat(info.getKeyspace()).isEqualTo("ks");
        assertThat(info.getTableName()).isEqualTo("table");
        assertThat(info.getSetColumns()).containsExactly("Status");
        assertThat(info.getWhereColumns()).containsExactly("User-ID");
    }

    // Comprehensive INSERT query tests

    @Test
    void testInsertQueryComprehensive() {
        // Test with newlines and whitespace
        QueryParser.QueryInfo info1 =
                QueryParser.parseInsertQuery(
                        "INSERT  INTO\n\t  ks.tbl\n  (id,\n  name)\n  VALUES\n  (?,\n  ?)");
        assertThat(info1.getKeyspace()).isEqualTo("ks");
        assertThat(info1.getTableName()).isEqualTo("tbl");
        assertThat(info1.getColumnNames()).containsExactly("id", "name");

        // Test with dollar signs in identifiers
        QueryParser.QueryInfo info2 =
                QueryParser.parseInsertQuery(
                        "INSERT INTO keyspace1.table1 (user_$id, name$value) VALUES (?, ?)");
        assertThat(info2.getKeyspace()).isEqualTo("keyspace1");
        assertThat(info2.getTableName()).isEqualTo("table1");
        assertThat(info2.getColumnNames()).containsExactly("user_$id", "name$value");

        // Test with escaped quotes in identifiers
        QueryParser.QueryInfo info3 =
                QueryParser.parseInsertQuery(
                        "INSERT INTO ks2.tbl2 (\"na\"\"me\", \"val\"\"ue\") VALUES (?, ?)");
        assertThat(info3.getKeyspace()).isEqualTo("ks2");
        assertThat(info3.getTableName()).isEqualTo("tbl2");
        assertThat(info3.getColumnNames()).containsExactly("na\"me", "val\"ue");

        // Test with multiple USING clauses (TTL and TIMESTAMP)
        QueryParser.QueryInfo info4 =
                QueryParser.parseInsertQuery(
                        "INSERT INTO ks3.tbl3 (id) VALUES (?) USING TTL 3600 AND TIMESTAMP 123456789");
        assertThat(info4.getKeyspace()).isEqualTo("ks3");
        assertThat(info4.getTableName()).isEqualTo("tbl3");
        assertThat(info4.getColumnNames()).containsExactly("id");

        // Test with Unicode identifiers
        QueryParser.QueryInfo info5 =
                QueryParser.parseInsertQuery(
                        "INSERT INTO unicode_ks.unicode_tbl (用户名, 电子邮件) VALUES (?, ?)");
        assertThat(info5.getKeyspace()).isEqualTo("unicode_ks");
        assertThat(info5.getTableName()).isEqualTo("unicode_tbl");
        assertThat(info5.getColumnNames()).containsExactly("用户名", "电子邮件");

        // Test error case: mixed placeholders and literals
        assertThatThrownBy(
                        () ->
                                QueryParser.parseInsertQuery(
                                        "INSERT INTO ks.tbl (id, age, name) VALUES (?, 42, ?)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only parameter placeholders (?)")
                .hasMessageContaining("42");
    }

    // Comprehensive UPDATE query tests

    @Test
    void testUpdateQueryComprehensive() {
        // Test all comparison operators
        String[] operatorQueries = {
            "UPDATE ks1.tbl1 SET val=? WHERE id = ?",
            "UPDATE ks2.tbl2 SET val=? WHERE id != ?",
            "UPDATE ks3.tbl3 SET val=? WHERE id < ?",
            "UPDATE ks4.tbl4 SET val=? WHERE id > ?",
            "UPDATE ks5.tbl5 SET val=? WHERE id <= ?",
            "UPDATE ks6.tbl6 SET val=? WHERE id >= ?"
        };
        String[] expectedKeyspaces = {"ks1", "ks2", "ks3", "ks4", "ks5", "ks6"};
        String[] expectedTables = {"tbl1", "tbl2", "tbl3", "tbl4", "tbl5", "tbl6"};

        for (int i = 0; i < operatorQueries.length; i++) {
            QueryParser.UpdateQueryInfo info = QueryParser.parseUpdateQuery(operatorQueries[i]);
            assertThat(info.getKeyspace()).isEqualTo(expectedKeyspaces[i]);
            assertThat(info.getTableName()).isEqualTo(expectedTables[i]);
            assertThat(info.getSetColumns()).containsExactly("val");
            assertThat(info.getWhereColumns()).containsExactly("id");
        }

        // Test IN operator with multiple placeholders
        QueryParser.UpdateQueryInfo info1 =
                QueryParser.parseUpdateQuery(
                        "UPDATE in_ks.in_tbl SET status=? WHERE id IN (?, ?, ?)");
        assertThat(info1.getKeyspace()).isEqualTo("in_ks");
        assertThat(info1.getTableName()).isEqualTo("in_tbl");
        assertThat(info1.getSetColumns()).containsExactly("status");
        assertThat(info1.getWhereColumns()).containsExactly("id");

        // Test complex WHERE conditions with AND/OR
        // Note: OR splits conditions differently, so we only get columns before OR
        QueryParser.UpdateQueryInfo info2 =
                QueryParser.parseUpdateQuery(
                        "UPDATE complex_ks.complex_tbl SET val=? WHERE id=? AND status IN(?,?) AND age> ?");
        assertThat(info2.getKeyspace()).isEqualTo("complex_ks");
        assertThat(info2.getTableName()).isEqualTo("complex_tbl");
        assertThat(info2.getSetColumns()).containsExactly("val");
        assertThat(info2.getWhereColumns()).containsExactly("id", "status", "age");

        // Test USING clause after table name
        QueryParser.UpdateQueryInfo info3 =
                QueryParser.parseUpdateQuery(
                        "UPDATE using_ks.using_tbl USING TTL 3600 AND TIMESTAMP 1 SET a=? WHERE id=?");
        assertThat(info3.getKeyspace()).isEqualTo("using_ks");
        assertThat(info3.getTableName()).isEqualTo("using_tbl");
        assertThat(info3.getSetColumns()).containsExactly("a");
        assertThat(info3.getWhereColumns()).containsExactly("id");

        // Test quoted identifiers with special characters
        QueryParser.UpdateQueryInfo info5 =
                QueryParser.parseUpdateQuery(
                        "UPDATE quoted_ks.quoted_tbl SET \"A-Name\" = ?, \"B\"\"Q\" = ? WHERE \"C-Id\" = ?");
        assertThat(info5.getKeyspace()).isEqualTo("quoted_ks");
        assertThat(info5.getTableName()).isEqualTo("quoted_tbl");
        assertThat(info5.getSetColumns()).containsExactly("A-Name", "B\"Q");
        assertThat(info5.getWhereColumns()).containsExactly("C-Id");
    }

    @Test
    void testSpecialCasesAndEdgeCases() {
        // Test very long column list (100 columns)
        StringBuilder query = new StringBuilder("INSERT INTO long_ks.long_tbl (");
        StringBuilder values = new StringBuilder(" VALUES (");
        for (int i = 0; i < 100; i++) {
            if (i > 0) {
                query.append(", ");
                values.append(", ");
            }
            query.append("col").append(i);
            values.append("?");
        }
        query.append(")").append(values).append(")");

        QueryParser.QueryInfo longInfo = QueryParser.parseInsertQuery(query.toString());
        assertThat(longInfo.getKeyspace()).isEqualTo("long_ks");
        assertThat(longInfo.getTableName()).isEqualTo("long_tbl");
        assertThat(longInfo.getColumnNames()).hasSize(100);
        assertThat(longInfo.getColumnNames().get(0)).isEqualTo("col0");
        assertThat(longInfo.getColumnNames().get(99)).isEqualTo("col99");

        // Test empty SET clause detection
        assertThatThrownBy(() -> QueryParser.parseUpdateQuery("UPDATE ks.tbl SET WHERE id=?"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> QueryParser.parseUpdateQuery("UPDATE ks.tbl SET   WHERE id=?"))
                .isInstanceOf(IllegalArgumentException.class);

        // Test missing keyspace with quoted table
        assertThatThrownBy(
                        () ->
                                QueryParser.parseInsertQuery(
                                        "INSERT INTO \"MyTable\" (id) VALUES (?)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fully qualified keyspace.table");
    }
}
