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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/** Unit tests for {@link AbstractRecordWriter}. */
public class AbstractRecordWriterTest {

    @Mock private Session session;
    @Mock private Cluster cluster;
    @Mock private Statement statement;
    @Mock private ResultSetFuture future;

    private TestAbstractRecordWriter writer;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testExecuteStatementDelegatesToSessionExecuteAsync() {
        writer = new TestAbstractRecordWriter(session, cluster);
        when(session.executeAsync(statement)).thenReturn(future);
        ListenableFuture<ResultSet> result = writer.executeStatement(statement);
        assertThat(result).isSameAs(future);
        verify(session, times(1)).executeAsync(statement);
        verifyNoMoreInteractions(session);
    }

    @Test
    void testCloseComprehensive() throws Exception {
        // Test normal close with both session and cluster
        writer = new TestAbstractRecordWriter(session, cluster);
        writer.close();
        InOrder inOrder = inOrder(session, cluster);
        inOrder.verify(session).close();
        inOrder.verify(cluster).close();

        // Reset mocks for next test
        MockitoAnnotations.openMocks(this);

        // Test idempotency - close() can be called multiple times
        writer = new TestAbstractRecordWriter(session, cluster);
        writer.close();
        writer.close();
        verify(session, atLeastOnce()).close();
        verify(cluster, atLeastOnce()).close();

        // Reset mocks for next test
        MockitoAnnotations.openMocks(this);

        // Test with null session - only cluster should be closed
        writer = new TestAbstractRecordWriter(null, cluster);
        writer.close();
        verify(cluster).close();
        verifyNoInteractions(session);

        // Reset mocks for next test
        MockitoAnnotations.openMocks(this);

        // Test with null cluster - only session should be closed
        writer = new TestAbstractRecordWriter(session, null);
        writer.close();
        verify(session).close();
        verifyNoInteractions(cluster);

        // Reset mocks for next test
        MockitoAnnotations.openMocks(this);

        // Test with both null - should not throw
        writer = new TestAbstractRecordWriter(null, null);
        writer.close();
        verifyNoInteractions(session, cluster);
    }

    @Test
    void testCloseWhenSessionCloseThrows() {
        writer = new TestAbstractRecordWriter(session, cluster);
        RuntimeException sessionError = new RuntimeException("Session close failed");
        doThrow(sessionError).when(session).close();

        // The exception should be thrown, cluster.close() won't be reached
        assertThatThrownBy(() -> writer.close())
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Session close failed");

        verify(session).close();
        verify(cluster, never()).close();
    }

    @Test
    void testCloseWhenClusterCloseThrows() {
        writer = new TestAbstractRecordWriter(session, cluster);
        RuntimeException clusterError = new RuntimeException("Cluster close failed");
        doThrow(clusterError).when(cluster).close();

        // Session closes successfully, then cluster.close() throws
        assertThatThrownBy(() -> writer.close())
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Cluster close failed");

        verify(session).close();
        verify(cluster).close();
    }

    @Test
    void testGetSessionAndCluster() {
        writer = new TestAbstractRecordWriter(session, cluster);
        assertThat(writer.getSession()).isSameAs(session);
        assertThat(writer.getCluster()).isSameAs(cluster);
        writer = new TestAbstractRecordWriter(null, null);
        assertThat(writer.getSession()).isNull();
        assertThat(writer.getCluster()).isNull();
    }

    // Test implementation of AbstractRecordWriter
    static class TestAbstractRecordWriter extends AbstractRecordWriter<String> {
        private final Session session;
        private final Cluster cluster;

        TestAbstractRecordWriter(Session session, Cluster cluster) {
            this.session = session;
            this.cluster = cluster;
        }

        @Override
        public Statement prepareStatement(String input) {
            throw new UnsupportedOperationException("Not needed for AbstractRecordWriter tests");
        }

        @Override
        public Session getSession() {
            return session;
        }

        @Override
        protected Cluster getCluster() {
            return cluster;
        }
    }
}
