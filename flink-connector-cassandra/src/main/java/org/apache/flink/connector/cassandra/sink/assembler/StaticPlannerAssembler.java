package org.apache.flink.connector.cassandra.sink.assembler;

import org.apache.flink.connector.cassandra.sink.config.CqlSinkConfig;
import org.apache.flink.connector.cassandra.sink.config.RecordFormatType;
import org.apache.flink.connector.cassandra.sink.config.RowDataSinkConfig;
import org.apache.flink.connector.cassandra.sink.planner.core.components.StatementPlanner;
import org.apache.flink.connector.cassandra.sink.planner.core.customization.NoOpCustomizer;
import org.apache.flink.connector.cassandra.sink.planner.core.customization.NullUnsettingCustomizer;
import org.apache.flink.connector.cassandra.sink.planner.core.customization.StatementCustomizer;
import org.apache.flink.connector.cassandra.sink.planner.core.strategy.StaticInsertStrategy;
import org.apache.flink.connector.cassandra.sink.planner.core.strategy.StaticUpdateStrategy;
import org.apache.flink.connector.cassandra.sink.planner.resolver.ColumnValueResolver;
import org.apache.flink.connector.cassandra.sink.planner.resolver.FixedColumnValueResolver;
import org.apache.flink.connector.cassandra.sink.planner.resolver.FixedTableResolver;
import org.apache.flink.connector.cassandra.sink.planner.resolver.RowDataFieldsResolver;
import org.apache.flink.connector.cassandra.sink.planner.resolver.TableResolver;
import org.apache.flink.connector.cassandra.sink.util.QueryParser;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;

/**
 * Assembler for static (CQL query-based) sink configurations.
 *
 * <p>Creates a StatementPlanner by parsing the provided CQL query to determine INSERT or UPDATE
 * mode, then building appropriate resolvers and strategies. Uses optimized RowDataFieldsResolver
 * for RowData input types and falls back to StaticColumnValueResolver for other types.
 *
 * @param <INPUT> the input record type
 */
public class StaticPlannerAssembler<INPUT> implements PlannerAssembler {

    private final CqlSinkConfig<INPUT> config;

    public StaticPlannerAssembler(CqlSinkConfig<INPUT> config) {
        this.config = config;
    }

    @Override
    public StatementPlanner<INPUT> assemble() {
        String query = config.getQuery();
        Preconditions.checkArgument(!StringUtils.isEmpty(query), "Query cannot be empty.");
        StaticQueryType staticQueryType = determineQueryType(query);
        switch (staticQueryType) {
            case INSERT:
                return assembleInsertPlanner(query);
            case UPDATE:
                return assembleUpdatePlanner(query);
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Static mode only supports INSERT and UPDATE queries. Got: %s",
                                query));
        }
    }

    /** Determines the type of CQL query (INSERT or UPDATE). */
    private StaticQueryType determineQueryType(String query) {
        String normalizedQuery = query.toUpperCase().trim();
        if (normalizedQuery.startsWith("INSERT")) {
            return StaticQueryType.INSERT;
        } else if (normalizedQuery.startsWith("UPDATE")) {
            return StaticQueryType.UPDATE;
        } else {
            return StaticQueryType.UNKNOWN;
        }
    }

    /** Assembles a StatementPlanner for INSERT queries. */
    private StatementPlanner<INPUT> assembleInsertPlanner(String query) {
        QueryParser.QueryInfo queryInfo = QueryParser.parseInsertQuery(query);

        TableResolver<INPUT> tableResolver =
                createTableResolver(queryInfo.getKeyspace(), queryInfo.getTableName());
        ColumnValueResolver<INPUT> resolver =
                createColumnValueResolver(
                        config, queryInfo.getColumnNames(), Collections.emptyList());
        StatementCustomizer<INPUT> customizer = createStatementCustomizer();

        return new StatementPlanner<>(
                tableResolver, resolver, new StaticInsertStrategy<>(), null, customizer);
    }

    /** Assembles a StatementPlanner for UPDATE queries. */
    private StatementPlanner<INPUT> assembleUpdatePlanner(String query) {
        QueryParser.UpdateQueryInfo updateInfo = QueryParser.parseUpdateQuery(query);

        TableResolver<INPUT> tableResolver =
                createTableResolver(updateInfo.getKeyspace(), updateInfo.getTableName());
        ColumnValueResolver<INPUT> resolver =
                createColumnValueResolver(
                        config, updateInfo.getSetColumns(), updateInfo.getWhereColumns());
        StatementCustomizer<INPUT> customizer = createStatementCustomizer();

        return new StatementPlanner<>(
                tableResolver, resolver, new StaticUpdateStrategy<>(), null, customizer);
    }

    /** Creates a FixedTableResolver for the given keyspace and table. */
    private TableResolver<INPUT> createTableResolver(String keyspace, String tableName) {
        return new FixedTableResolver<>(keyspace, tableName);
    }

    /** Creates the appropriate StatementCustomizer based on config. */
    private StatementCustomizer<INPUT> createStatementCustomizer() {
        return config.getIgnoreNullFields()
                ? new NullUnsettingCustomizer<>()
                : new NoOpCustomizer<>();
    }

    /** Enum for query types supported in static mode. */
    private enum StaticQueryType {
        INSERT,
        UPDATE,
        UNKNOWN
    }

    /**
     * Creates the appropriate ColumnValueResolver based on RecordFormatType. RowData uses
     * RowDataFieldsResolver for optimal performance.
     */
    @SuppressWarnings("unchecked")
    private static <T> ColumnValueResolver<T> createColumnValueResolver(
            CqlSinkConfig<T> config, List<String> columnNames, List<String> whereColumns) {
        if (config.getRecordFormatType() == RecordFormatType.ROWDATA) {
            RowDataSinkConfig rowConfig = (RowDataSinkConfig) config;
            RowType rowType = (RowType) rowConfig.getRowDataType().getLogicalType();
            return (ColumnValueResolver<T>)
                    new RowDataFieldsResolver(rowType, columnNames, whereColumns);
        } else {
            return new FixedColumnValueResolver<>(
                    config.getRecordFormatType(), columnNames, whereColumns);
        }
    }
}
