package org.apache.flink.connector.cassandra.sink.assembler;

import org.apache.flink.connector.cassandra.sink.planner.core.components.StatementPlanner;

/** Interface for assembling {@link StatementPlanner} instances from sink configurations. */
public interface PlannerAssembler {

    /**
     * Assembles a StatementPlanner from the given configuration.
     *
     * @return the assembled statement planner
     */
    StatementPlanner<?> assemble();
}
