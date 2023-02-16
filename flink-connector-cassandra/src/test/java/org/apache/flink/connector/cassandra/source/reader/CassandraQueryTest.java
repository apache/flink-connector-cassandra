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

package org.apache.flink.connector.cassandra.source.reader;

import org.apache.flink.connector.cassandra.source.CassandraSource;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/** tests for query generation and query sanity checks. */
class CassandraQueryTest {

    private static final Pattern SELECT_PATTERN = Pattern.compile(CassandraSource.SELECT_REGEXP);

    @Test
    public void testKeySpaceTableExtractionRegexp() {
        Arrays.asList(
                        "select field FROM keyspace.table where field = value;",
                        "select * FROM keyspace.table;",
                        "select field1, field2 from keyspace.table;",
                        "select field1, field2 from keyspace.table LIMIT(1000);",
                        "select field1 from keyspace.table ;",
                        "select field1 from keyspace.table where field1=1;")
                .forEach(CassandraQueryTest::assertQueryFormatCorrect);

        Arrays.asList(
                        "select field1 from table;", // missing keyspace
                        "select field1 from keyspace.table" // missing ";"
                        )
                .forEach(CassandraQueryTest::assertQueryFormatIncorrect);
    }

    @Test
    public void testProhibitedClauses() {
        Arrays.asList(
                        "SELECT COUNT(*) from flink.table;",
                        "SELECT AVG(*) from flink.table;",
                        "SELECT MIN(*) from flink.table;",
                        "SELECT MAX(*) from flink.table;",
                        "SELECT SUM(*) from flink.table;",
                        "SELECT field1, field2 from flink.table ORDER BY field1;",
                        "SELECT field1, field2 from flink.table GROUP BY field1;")
                .forEach(CassandraQueryTest::assertProhibitedClauseRejected);
    }

    @Test
    public void testGenerateRangeQuery() {
        String query;
        String outputQuery;

        // query with where clause
        query = "SELECT field FROM keyspace.table WHERE field = value;";
        outputQuery = CassandraSplitReader.generateRangeQuery(query, "field");
        assertThat(outputQuery)
                .isEqualTo(
                        "SELECT field FROM keyspace.table WHERE (token(field) >= ?) AND (token(field) < ?) AND field = value;");

        // query without where clause
        query = "SELECT * FROM keyspace.table;";
        outputQuery = CassandraSplitReader.generateRangeQuery(query, "field");
        assertThat(outputQuery)
                .isEqualTo(
                        "SELECT * FROM keyspace.table WHERE (token(field) >= ?) AND (token(field) < ?);");

        // query without where clause but with another trailing clause
        query = "SELECT field FROM keyspace.table LIMIT(1000);";
        outputQuery = CassandraSplitReader.generateRangeQuery(query, "field");
        assertThat(outputQuery)
                .isEqualTo(
                        "SELECT field FROM keyspace.table WHERE (token(field) >= ?) AND (token(field) < ?) LIMIT(1000);");
    }

    private static void assertQueryFormatIncorrect(String query) {
        assertThat(query.matches(CassandraSource.SELECT_REGEXP)).isFalse();
    }

    private static void assertQueryFormatCorrect(String query) {
        Matcher matcher = SELECT_PATTERN.matcher(query);
        assertThat(matcher.matches()).isTrue();
        assertThat(matcher.group(1)).isEqualTo("keyspace");
        assertThat(matcher.group(2)).isEqualTo("table");
    }

    private static void assertProhibitedClauseRejected(String query) {
        assertThat(query.matches(CassandraSource.CQL_PROHIBITED_CLAUSES_REGEXP)).isTrue();
    }
}
