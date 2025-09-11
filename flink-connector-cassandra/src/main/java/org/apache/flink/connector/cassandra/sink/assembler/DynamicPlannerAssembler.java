package org.apache.flink.connector.cassandra.sink.assembler;

import org.apache.flink.connector.cassandra.sink.config.CqlSinkConfig;
import org.apache.flink.connector.cassandra.sink.planner.SinkPluggable;
import org.apache.flink.connector.cassandra.sink.planner.core.components.StatementPlanner;
import org.apache.flink.connector.cassandra.sink.planner.core.strategy.InsertStrategy;
import org.apache.flink.connector.cassandra.sink.planner.core.strategy.PlannerStrategy;
import org.apache.flink.connector.cassandra.sink.planner.core.strategy.UpdateStrategy;
import org.apache.flink.connector.cassandra.sink.planner.resolver.ColumnValueResolver;
import org.apache.flink.util.Preconditions;

/**
 * Assembler for dynamic (pluggable) sink configurations.
 *
 * <p>Creates a StatementPlanner using pluggable resolvers and customizers from the configuration.
 * The planner strategy (INSERT vs UPDATE) is determined by the column value resolver's kind.
 *
 * @param <INPUT> the input record type
 */
public class DynamicPlannerAssembler<INPUT> implements PlannerAssembler {

    private final CqlSinkConfig<INPUT> config;

    public DynamicPlannerAssembler(CqlSinkConfig<INPUT> config) {
        this.config = config;
    }

    @Override
    public StatementPlanner<INPUT> assemble() {
        SinkPluggable<INPUT> pluggable = config.getPluggable();
        Preconditions.checkArgument(
                pluggable != null, "SinkPluggable cannot be null in DYNAMIC mode");
        Preconditions.checkArgument(
                pluggable.getTableResolver() != null,
                "TableResolver cannot be null in DYNAMIC mode");
        Preconditions.checkArgument(
                pluggable.getColumnValueResolver() != null,
                "ColumnValueResolver cannot be null in DYNAMIC mode");

        // Choose strategy based on resolver's kind
        PlannerStrategy<INPUT> strategy;
        ColumnValueResolver.Kind resolverKind = pluggable.getColumnValueResolver().kind();
        switch (resolverKind) {
            case INSERT:
                strategy = new InsertStrategy<>();
                break;
            case UPDATE:
                strategy = new UpdateStrategy<>();
                break;
            default:
                throw new IllegalArgumentException("Unknown resolver kind: " + resolverKind);
        }

        return new StatementPlanner<>(
                pluggable.getTableResolver(),
                pluggable.getColumnValueResolver(),
                strategy,
                pluggable.getCqlClauseResolver(),
                pluggable.getStatementCustomizer());
    }
}
