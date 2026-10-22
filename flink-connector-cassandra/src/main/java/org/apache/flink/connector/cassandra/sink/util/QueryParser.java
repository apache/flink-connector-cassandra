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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for parsing CQL INSERT and UPDATE queries to extract table information and column
 * names.
 *
 * <p>This parser supports standard INSERT INTO and UPDATE statements with the following formats:
 *
 * <ul>
 *   <li>INSERT: {@code INSERT INTO keyspace.table (column1, column2, ...) VALUES (?, ?, ...)}
 *   <li>UPDATE: {@code UPDATE keyspace.table SET column1=?, column2=? WHERE id=? AND status IN (?,
 *       ?)}
 * </ul>
 *
 * <p>The parser extracts keyspace, table, and column names from queries while ignoring USING and IF
 * clauses which should contain literal values in static mode.
 *
 * <p>Supported query variations include:
 *
 * <ul>
 *   <li>Quoted identifiers: {@code "MixedCase", "user-profile", "order"}
 *   <li>Trailing clauses: {@code USING TTL 3600, IF NOT EXISTS}
 *   <li>Flexible whitespace and case variations
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * String insertQuery = "INSERT INTO analytics.events (user_id, event_time, event_type) VALUES (?, ?, ?)";
 * QueryInfo insertInfo = QueryParser.parseInsertQuery(insertQuery);
 * // insertInfo.getKeyspace() = "analytics"
 * // insertInfo.getTableName() = "events"
 * // insertInfo.getColumnNames() = ["user_id", "event_time", "event_type"]
 *
 * String updateQuery = "UPDATE analytics.events SET event_type=?, last_seen=? WHERE user_id IN (?, ?, ?)";
 * UpdateQueryInfo updateInfo = QueryParser.parseUpdateQuery(updateQuery);
 * // updateInfo.getKeyspace() = "analytics"
 * // updateInfo.getTableName() = "events"
 * // updateInfo.getSetColumns() = ["event_type", "last_seen"]
 * // updateInfo.getWhereColumns() = ["user_id"]
 * }</pre>
 */
@Internal
public final class QueryParser {

    /**
     * Regex pattern for CQL identifiers: unquoted (letter/underscore start) or quoted (with escaped
     * quotes).
     */
    private static final String IDENTIFIER = "(?:\"(?:[^\"]|\"\")+\"|[A-Za-z_][A-Za-z0-9_\\$]*)";

    /**
     * Pattern for INSERT queries - captures core structure while being permissive of trailing
     * clauses. Groups: (1) keyspace, (2) table, (3) columns, (4) values placeholders
     */
    private static final Pattern INSERT_PATTERN =
            Pattern.compile(
                    "INSERT\\s+INTO\\s+(?:("
                            + IDENTIFIER
                            + ")\\.)?("
                            + IDENTIFIER
                            + ")\\s*"
                            + "\\(([^)]*)\\)\\s*VALUES\\s*\\(([^)]*)\\)",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    /**
     * Pattern for UPDATE queries - captures SET and WHERE while allowing USING after table and IF
     * at end. Groups: (1) keyspace, (2) table, (3) SET clause, (4) WHERE clause
     */
    private static final Pattern UPDATE_PATTERN =
            Pattern.compile(
                    "UPDATE\\s+(?:("
                            + IDENTIFIER
                            + ")\\.)?("
                            + IDENTIFIER
                            + ")\\s*"
                            + "(?:USING\\b.+?\\s+)?" // optional USING ... AFTER table
                            + "SET\\s+(.+?)\\s+WHERE\\s+(.+?)" // non-greedy SET/WHERE groups
                            + "(?:\\s+IF\\b.+)?\\s*$", // optional IF ... tail
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    /**
     * Pattern for WHERE conditions - matches column name followed by any operator and value.
     * Groups: (1) column name, (2) operator and value part
     */
    private static final Pattern WHERE_CONDITION_PATTERN =
            Pattern.compile(
                    "^(" + IDENTIFIER + ")\\s*(.+)$", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    /** Pattern for splitting WHERE conditions on AND (case-insensitive with flexible spacing). */
    private static final Pattern AND_SPLITTER =
            Pattern.compile("\\s+AND\\s+", Pattern.CASE_INSENSITIVE);

    /**
     * Parses a CQL INSERT query to extract table and column information.
     *
     * @param insertQuery the INSERT query to parse
     * @return QueryInfo containing parsed keyspace, table, and column names
     * @throws IllegalArgumentException if the query format is invalid
     */
    public static QueryInfo parseInsertQuery(String insertQuery) {
        Preconditions.checkArgument(
                insertQuery != null && !StringUtils.isBlank(insertQuery),
                "insertQuery cannot be null or blank");

        Matcher matcher = INSERT_PATTERN.matcher(insertQuery);
        Preconditions.checkArgument(
                matcher.find(),
                "Invalid INSERT query format. Expected 'INSERT INTO keyspace.table (columns) VALUES (placeholders)'. "
                        + "Static mode requires fully qualified keyspace.table. Got: %s",
                insertQuery);

        String keyspace = dequote(matcher.group(1));
        String tableName = dequote(matcher.group(2));
        validateKeyspaceAndTable(keyspace, tableName, "INSERT");

        String columnsText = matcher.group(3);
        String valuesText = matcher.group(4);

        List<String> columnNames = parseColumnNames(columnsText);
        Preconditions.checkArgument(
                !columnNames.isEmpty(), "At least one column must be specified");

        List<String> valuePlaceholders = parseValuePlaceholders(valuesText);
        Preconditions.checkArgument(
                columnNames.size() == valuePlaceholders.size(),
                "Column count (%d) must match value placeholder count (%d). Columns: %s, Values: %s",
                columnNames.size(),
                valuePlaceholders.size(),
                columnNames,
                valuePlaceholders);

        return new QueryInfo(keyspace, tableName, columnNames);
    }

    /**
     * Parses a CQL UPDATE query to extract table and column information.
     *
     * @param updateQuery the UPDATE query to parse
     * @return UpdateQueryInfo containing parsed keyspace, table, SET columns, and WHERE columns
     * @throws IllegalArgumentException if the query format is invalid
     */
    public static UpdateQueryInfo parseUpdateQuery(String updateQuery) {
        Preconditions.checkArgument(
                updateQuery != null && !StringUtils.isBlank(updateQuery),
                "updateQuery cannot be null or blank");

        Matcher matcher = UPDATE_PATTERN.matcher(updateQuery);
        Preconditions.checkArgument(
                matcher.find(),
                "Invalid UPDATE query format. Expected 'UPDATE keyspace.table SET col1=?, col2=? WHERE id=?'. "
                        + "Static mode requires fully qualified keyspace.table. Got: %s",
                updateQuery);

        String keyspace = dequote(matcher.group(1));
        String tableName = dequote(matcher.group(2));
        validateKeyspaceAndTable(keyspace, tableName, "UPDATE");

        String setClause = matcher.group(3);
        String whereClause = matcher.group(4);
        List<String> setColumns = parseSetClause(setClause);
        Preconditions.checkArgument(
                !setColumns.isEmpty(), "At least one SET column must be specified");
        List<String> whereColumns = parseWhereClause(whereClause);
        Preconditions.checkArgument(
                !whereColumns.isEmpty(), "At least one WHERE column must be specified for UPDATE.");

        return new UpdateQueryInfo(keyspace, tableName, setColumns, whereColumns);
    }

    /**
     * Validates keyspace and table extracted from query. Example: validateKeyspaceAndTable(null,
     * "users", "UPDATE") throws "Static mode requires fully qualified keyspace.table format"
     */
    private static void validateKeyspaceAndTable(
            String keyspace, String tableName, String queryType) {
        Preconditions.checkArgument(!StringUtils.isEmpty(tableName), "Table name cannot be empty");
        Preconditions.checkArgument(
                !StringUtils.isEmpty(keyspace),
                "Static mode requires fully qualified keyspace.table format. "
                        + "Use '%s keyspace.table' instead of just 'table'",
                queryType);
    }

    /**
     * De-quotes CQL identifiers if quoted, handling escaped quotes. Example:
     * dequote("\"MixedCase\"") returns "MixedCase", dequote("\"has\"\"quote\"") returns
     * "has\"quote"
     */
    private static String dequote(String identifier) {
        if (identifier != null
                && identifier.length() >= 2
                && identifier.startsWith("\"")
                && identifier.endsWith("\"")) {
            return identifier.substring(1, identifier.length() - 1).replace("\"\"", "\"");
        }
        return identifier;
    }

    /**
     * Parses column names from comma-separated text, properly handling quoted identifiers with
     * escaped quotes. Example: parseColumnNames("id, \"User-Name\", age") returns ["id",
     * "User-Name", "age"]
     */
    private static List<String> parseColumnNames(String columnsText) {
        if (StringUtils.isEmpty(columnsText)) {
            return Collections.emptyList();
        }

        List<String> columnNames = new ArrayList<>();
        StringBuilder currentToken = new StringBuilder();
        boolean insideQuotedIdentifier = false;

        for (int i = 0; i < columnsText.length(); i++) {
            char currentChar = columnsText.charAt(i);
            if (currentChar == '"') {
                currentToken.append(currentChar);
                // Check for doubled quote inside quotes (escaped quote)
                if (insideQuotedIdentifier
                        && i + 1 < columnsText.length()
                        && columnsText.charAt(i + 1) == '"') {
                    currentToken.append('"');
                    i++; // consume second quote
                } else {
                    insideQuotedIdentifier = !insideQuotedIdentifier;
                }
            } else if (currentChar == ',' && !insideQuotedIdentifier) {
                String columnName = currentToken.toString().trim();
                if (!columnName.isEmpty()) {
                    columnNames.add(dequote(columnName));
                }
                currentToken.setLength(0);
            } else {
                currentToken.append(currentChar);
            }
        }

        // Add the last column
        String lastColumnName = currentToken.toString().trim();
        if (!lastColumnName.isEmpty()) {
            columnNames.add(dequote(lastColumnName));
        }

        return columnNames;
    }

    /**
     * Parses value placeholders from the VALUES section of an INSERT query, ensuring all are ?.
     * Example: parseValuePlaceholders("?, ?, ?") returns ["?", "?", "?"], but "?, 'literal'" throws
     * exception
     */
    private static List<String> parseValuePlaceholders(String valuesText) {
        if (StringUtils.isEmpty(valuesText)) {
            return Collections.emptyList();
        }

        String[] values = valuesText.split(",");
        List<String> trimmedValues = new ArrayList<>();

        for (String value : values) {
            String trimmed = value.trim();
            if (!trimmed.isEmpty()) {
                Preconditions.checkArgument(
                        "?".equals(trimmed),
                        "Static mode supports only parameter placeholders (?) in VALUES clause. "
                                + "Literals and expressions are not supported. Found: '%s'",
                        trimmed);
                trimmedValues.add(trimmed);
            }
        }

        return trimmedValues;
    }

    /**
     * Parses the SET clause of an UPDATE query to extract column names. Example:
     * parseSetClause("name = ?, age = ?, \"Status\" = ?") returns ["name", "age", "Status"]
     */
    private static List<String> parseSetClause(String setClause) {
        if (StringUtils.isEmpty(setClause)) {
            return Collections.emptyList();
        }

        List<String> setColumnNames = new ArrayList<>();
        StringBuilder currentAssignmentBuilder = new StringBuilder();
        boolean insideQuotedIdentifier = false;
        int parenthesesNestingLevel = 0;

        for (char currentChar : setClause.toCharArray()) {
            if (currentChar == '"'
                    && (currentAssignmentBuilder.length() == 0
                            || currentAssignmentBuilder.charAt(
                                            currentAssignmentBuilder.length() - 1)
                                    != '"')) {
                insideQuotedIdentifier = !insideQuotedIdentifier;
                currentAssignmentBuilder.append(currentChar);
            } else if (!insideQuotedIdentifier && currentChar == '(') {
                parenthesesNestingLevel++;
                currentAssignmentBuilder.append(currentChar);
            } else if (!insideQuotedIdentifier && currentChar == ')') {
                parenthesesNestingLevel--;
                currentAssignmentBuilder.append(currentChar);
            } else if (currentChar == ','
                    && !insideQuotedIdentifier
                    && parenthesesNestingLevel == 0) {
                String setAssignment = currentAssignmentBuilder.toString().trim();
                if (!setAssignment.isEmpty()) {
                    setColumnNames.add(parseSetAssignment(setAssignment));
                }
                currentAssignmentBuilder = new StringBuilder();
            } else {
                currentAssignmentBuilder.append(currentChar);
            }
        }

        // Add the last assignment
        String finalSetAssignment = currentAssignmentBuilder.toString().trim();
        if (!finalSetAssignment.isEmpty()) {
            setColumnNames.add(parseSetAssignment(finalSetAssignment));
        }

        return setColumnNames;
    }

    /**
     * Parses a single SET assignment and returns the column name. Example:
     * parseSetAssignment("status = ?") returns "status", but "status = 'active'" throws exception
     */
    private static String parseSetAssignment(String assignment) {
        String[] parts = assignment.split("=", 2);
        Preconditions.checkArgument(
                parts.length == 2,
                "Invalid SET assignment format. Expected 'column = ?'. Got: '%s'",
                assignment);

        String column = parts[0].trim();
        String value = parts[1].trim();

        Preconditions.checkArgument(
                "?".equals(value),
                "Static mode supports only parameter placeholders (?) in SET clause. "
                        + "Literals and expressions are not supported. Found: '%s'",
                value);

        return dequote(column);
    }

    /**
     * Parses the WHERE clause of an UPDATE query to extract column names. Example:
     * parseWhereClause("id = ? AND status IN (?, ?) AND age > ?") returns ["id", "status", "age"]
     */
    private static List<String> parseWhereClause(String whereClause) {
        if (StringUtils.isEmpty(whereClause)) {
            return Collections.emptyList();
        }

        List<String> columns = new ArrayList<>();
        String[] conditions = AND_SPLITTER.split(whereClause.trim());

        for (String condition : conditions) {
            String trimmed = condition.trim();
            if (!trimmed.isEmpty()) {
                columns.add(parseWhereCondition(trimmed));
            }
        }

        return columns;
    }

    /**
     * Parses a single WHERE condition (supports =, !=, <, >, <=, >=, IN operators). Example:
     * parseWhereCondition("id IN (?, ?, ?)") returns "id", parseWhereCondition("age > ?") returns
     * "age"
     */
    private static String parseWhereCondition(String condition) {
        Matcher matcher = WHERE_CONDITION_PATTERN.matcher(condition.trim());
        Preconditions.checkArgument(
                matcher.matches(),
                "Invalid WHERE condition format. Expected 'column <operator> <value>'. Got: '%s'",
                condition);

        String column = dequote(matcher.group(1));
        String operatorAndValue = matcher.group(2);

        // Just verify that there's at least one placeholder in the value part
        Preconditions.checkArgument(
                operatorAndValue.contains("?"),
                "WHERE condition must contain at least one parameter placeholder (?). "
                        + "Literals should be part of the query text in static mode. Found: '%s'",
                condition);

        return column;
    }

    /** Container for parsed query information. */
    public static class QueryInfo {
        private final String keyspace;
        private final String tableName;
        private final List<String> columnNames;

        public QueryInfo(String keyspace, String tableName, List<String> columnNames) {
            this.keyspace = keyspace;
            this.tableName = tableName;
            this.columnNames = Collections.unmodifiableList(new ArrayList<>(columnNames));
        }

        public String getKeyspace() {
            return keyspace;
        }

        public String getTableName() {
            return tableName;
        }

        public List<String> getColumnNames() {
            return columnNames;
        }
    }

    /** Container for parsed UPDATE query information. */
    public static class UpdateQueryInfo {
        private final String keyspace;
        private final String tableName;
        private final List<String> setColumns;
        private final List<String> whereColumns;

        public UpdateQueryInfo(
                String keyspace,
                String tableName,
                List<String> setColumns,
                List<String> whereColumns) {
            this.keyspace = keyspace;
            this.tableName = tableName;
            this.setColumns = Collections.unmodifiableList(new ArrayList<>(setColumns));
            this.whereColumns = Collections.unmodifiableList(new ArrayList<>(whereColumns));
        }

        public String getKeyspace() {
            return keyspace;
        }

        public String getTableName() {
            return tableName;
        }

        public List<String> getSetColumns() {
            return setColumns;
        }

        public List<String> getWhereColumns() {
            return whereColumns;
        }
    }
}
