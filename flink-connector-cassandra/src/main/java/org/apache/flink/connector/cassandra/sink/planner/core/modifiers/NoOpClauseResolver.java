package org.apache.flink.connector.cassandra.sink.planner.core.modifiers;

/** A no-op implementation of {@link CqlClauseResolver} that adds no CQL clauses. */
public class NoOpClauseResolver<INPUT> implements CqlClauseResolver<INPUT> {}
