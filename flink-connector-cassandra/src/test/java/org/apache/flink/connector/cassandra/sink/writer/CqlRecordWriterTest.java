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

package org.apache.flink.connector.cassandra.sink.writer;

import org.apache.flink.connector.cassandra.sink.config.CqlSinkConfig;
import org.apache.flink.connector.cassandra.sink.planner.core.components.StatementPlanner;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.types.Row;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Unit tests for {@link CqlRecordWriter}. */
public class CqlRecordWriterTest {

    @Mock private ClusterBuilder clusterBuilder;
    @Mock private CqlSinkConfig<Row> config;
    @Mock private Cluster cluster;
    @Mock private Session session;
    @Mock private StatementPlanner<Row> planner;
    @Mock private Statement statement;
    @Mock private ResultSetFuture future;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testSuccessfulClusterCreationAndConnection() {
        // Execute - using @VisibleForTesting constructor
        CqlRecordWriter<Row> writer = new CqlRecordWriter<>(session, planner, config);

        // Verify
        assertThat(writer).isNotNull();
        assertThat(writer.getSession()).isSameAs(session);
    }

    @Test
    void testGetClusterFailsThrowsRuntimeException() {
        // Setup
        RuntimeException clusterError = new RuntimeException("Cluster build failed");
        when(clusterBuilder.getCluster()).thenThrow(clusterError);

        // Execute & Verify
        assertThatThrownBy(() -> new CqlRecordWriter<>(clusterBuilder, config))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to create Cassandra cluster from ClusterBuilder")
                .hasCause(clusterError);
    }

    @Test
    void testConnectFailsThrowsRuntimeExceptionWithCleanup() {
        // Setup
        RuntimeException connectError = new RuntimeException("Connect failed");
        when(clusterBuilder.getCluster()).thenReturn(cluster);
        when(cluster.connect()).thenThrow(connectError);

        // Execute & Verify
        assertThatThrownBy(() -> new CqlRecordWriter<>(clusterBuilder, config))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to connect to Cassandra cluster")
                .hasCause(connectError);

        // Verify cleanup
        verify(cluster).close();
    }

    @Test
    void testPrepareStatementDelegatesToPlannerPlan() throws Exception {
        // Setup writer using @VisibleForTesting constructor
        CqlRecordWriter<Row> writer = new CqlRecordWriter<>(session, planner, config);

        // Setup test
        Row input = Row.of(1, "test");
        when(planner.plan(input, session, config)).thenReturn(statement);

        // Execute
        Statement result = writer.prepareStatement(input);

        // Verify
        assertThat(result).isSameAs(statement);
        verify(planner).plan(input, session, config);
    }

    @Test
    void testPrepareStatementPlannerThrowsExceptionPropagated() throws Exception {
        // Setup writer using @VisibleForTesting constructor
        CqlRecordWriter<Row> writer = new CqlRecordWriter<>(session, planner, config);

        // Setup test
        Row input = Row.of(1, "test");
        RuntimeException plannerError = new RuntimeException("Planning failed");
        when(planner.plan(input, session, config)).thenThrow(plannerError);

        // Execute & Verify
        assertThatThrownBy(() -> writer.prepareStatement(input)).isSameAs(plannerError);
    }

    @Test
    void testGetSessionReturnsSessionFromClusterConnect() {
        // Execute - using @VisibleForTesting constructor
        CqlRecordWriter<Row> writer = new CqlRecordWriter<>(session, planner, config);

        // Verify
        assertThat(writer.getSession()).isSameAs(session);
    }

    @Test
    void testCloseCallsSuperClose() throws Exception {
        // Setup writer using @VisibleForTesting constructor
        CqlRecordWriter<Row> writer = new CqlRecordWriter<>(session, planner, config);

        // Execute
        writer.close();

        // Verify
        verify(planner).close();
        verify(session).close();
    }

    @Test
    void testCloseIdempotency() throws Exception {
        // Setup writer using @VisibleForTesting constructor
        CqlRecordWriter<Row> writer = new CqlRecordWriter<>(session, planner, config);

        // Execute
        writer.close();
        writer.close();

        // Verify idempotent close
        verify(planner, atLeastOnce()).close();
        verify(session, atLeastOnce()).close();
    }

    @Test
    void testCompleteWriteFlowPlanToExecuteAsyncToFuture() throws Exception {
        // Setup writer using @VisibleForTesting constructor
        CqlRecordWriter<Row> writer = new CqlRecordWriter<>(session, planner, config);

        // Setup test
        Row input = Row.of(1, "test");
        when(planner.plan(input, session, config)).thenReturn(statement);
        when(session.executeAsync(any(Statement.class))).thenReturn(future);

        // Execute
        ListenableFuture<ResultSet> result = writer.write(input);

        // Verify
        assertThat(result).isSameAs(future);
        verify(planner).plan(input, session, config);
        verify(session).executeAsync(statement);
    }

    @Test
    void testWritePassesPlannerStatementUnchanged() throws Exception {
        // Setup writer using @VisibleForTesting constructor
        CqlRecordWriter<Row> writer = new CqlRecordWriter<>(session, planner, config);

        // Setup test - planner returns a statement
        Row input = Row.of(1, "test");
        when(planner.plan(input, session, config)).thenReturn(statement);
        when(session.executeAsync(statement)).thenReturn(future);

        // Execute
        writer.write(input);

        // Verify exact statement instance passed to executeAsync
        verify(session).executeAsync(statement);
    }

    @Test
    void testResourceSafetyWhenPrepareStatementFails() throws Exception {
        // Setup writer using @VisibleForTesting constructor
        CqlRecordWriter<Row> writer = new CqlRecordWriter<>(session, planner, config);

        // Setup test
        Row input = Row.of(1, "test");
        RuntimeException prepareError = new RuntimeException("Prepare failed");
        when(planner.plan(input, session, config)).thenThrow(prepareError);

        // Execute
        assertThatThrownBy(() -> writer.write(input)).isSameAs(prepareError);

        // Verify no executeAsync called
        verify(session, never()).executeAsync(any(Statement.class));

        // Verify close still works after failure
        writer.close();
        verify(planner).close();
        verify(session).close();
    }

    @Test
    void testResourceSafetyWhenExecuteAsyncFails() throws Exception {
        // Setup writer using @VisibleForTesting constructor
        CqlRecordWriter<Row> writer = new CqlRecordWriter<>(session, planner, config);

        // Setup test
        Row input = Row.of(1, "test");
        RuntimeException executeError = new RuntimeException("Execute failed");
        when(planner.plan(input, session, config)).thenReturn(statement);
        when(session.executeAsync(statement)).thenThrow(executeError);

        // Execute
        assertThatThrownBy(() -> writer.write(input)).isSameAs(executeError);

        // Verify close still works after failure
        writer.close();
        verify(planner).close();
        verify(session).close();
    }
}
