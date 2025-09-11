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

import org.apache.flink.connector.cassandra.sink.config.PojoSinkConfig;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/** Unit tests for {@link PojoRecordWriter}. */
public class PojoRecordWriterTest {

    /** Test POJO with Cassandra annotations. */
    @Table(keyspace = "test_keyspace", name = "users")
    public static class User {
        @PartitionKey
        @Column(name = "user_id")
        private UUID userId;

        @Column(name = "username")
        private String username;

        @Column(name = "email")
        private String email;

        @Column(name = "age")
        private Integer age;

        public User() {}

        public User(UUID userId, String username, String email, Integer age) {
            this.userId = userId;
            this.username = username;
            this.email = email;
            this.age = age;
        }

        public UUID getUserId() {
            return userId;
        }

        public void setUserId(UUID userId) {
            this.userId = userId;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }
    }

    @Mock private ClusterBuilder clusterBuilder;
    @Mock private PojoSinkConfig<User> config;
    @Mock private Cluster cluster;
    @Mock private Session session;
    @Mock private Mapper<User> mapper;
    @Mock private Statement statement;
    @Mock private ResultSetFuture future;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testConstructorValidationRejectsNullParameters() {
        // Test null ClusterBuilder rejection
        assertThatThrownBy(() -> new PojoRecordWriter<>(null, config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ClusterBuilder cannot be null");

        // Test null PojoSinkConfig rejection
        assertThatThrownBy(() -> new PojoRecordWriter<>(clusterBuilder, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("PojoSinkConfig cannot be null");
    }

    @Test
    void testConstructorCallsBuilderAndClusterMethods() {
        String keyspace = "test_keyspace";
        when(config.getKeyspace()).thenReturn(keyspace);
        when(config.getPojoClass()).thenReturn(User.class);
        when(config.getMapperOptions()).thenReturn(null);

        // Test getCluster() failure
        RuntimeException clusterError = new RuntimeException("Cluster creation failed");
        when(clusterBuilder.getCluster()).thenThrow(clusterError);

        assertThatThrownBy(() -> new PojoRecordWriter<>(clusterBuilder, config))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to initialize PojoRecordWriter")
                .hasMessageContaining(keyspace)
                .hasMessageContaining(User.class.getName())
                .hasCause(clusterError);

        // Verify builder.getCluster() was called exactly once
        verify(clusterBuilder, times(1)).getCluster();
        verify(cluster, never()).connect(any());

        // Reset and test connect() failure
        MockitoAnnotations.openMocks(this);
        when(config.getKeyspace()).thenReturn(keyspace);
        when(config.getPojoClass()).thenReturn(User.class);
        when(clusterBuilder.getCluster()).thenReturn(cluster);

        RuntimeException connectError = new RuntimeException("Connection failed");
        when(cluster.connect(keyspace)).thenThrow(connectError);

        assertThatThrownBy(() -> new PojoRecordWriter<>(clusterBuilder, config))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to initialize PojoRecordWriter")
                .hasMessageContaining(keyspace)
                .hasMessageContaining(User.class.getName())
                .hasCause(connectError);

        // Verify cluster.connect() was called with correct keyspace
        verify(clusterBuilder, times(1)).getCluster();
        verify(cluster, times(1)).connect(keyspace);
    }

    @Test
    void testGetSessionAndGetCluster() {
        // Create writer using test constructor
        PojoRecordWriter<User> writer = new PojoRecordWriter<>(cluster, session, mapper);

        // Verify getSession returns the same Session instance
        assertThat(writer.getSession()).isSameAs(session);

        // Verify getCluster returns the same Cluster instance
        assertThat(writer.getCluster()).isSameAs(cluster);
    }

    @Test
    void testPrepareStatementDelegatesToMapper() {
        PojoRecordWriter<User> writer = new PojoRecordWriter<>(cluster, session, mapper);

        // Test with valid user
        User user = new User(UUID.randomUUID(), "alice", "alice@example.com", 30);
        when(mapper.saveQuery(user)).thenReturn(statement);

        Statement result = writer.prepareStatement(user);

        assertThat(result).isSameAs(statement);
        verify(mapper, times(1)).saveQuery(user);
        verifyNoMoreInteractions(mapper);

        // Test with null input - propagates mapper exception
        when(mapper.saveQuery(null)).thenThrow(new NullPointerException("Cannot save null"));

        assertThatThrownBy(() -> writer.prepareStatement(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Cannot save null");

        verify(mapper, times(1)).saveQuery(null);
    }

    @Test
    void testExecuteStatementDelegatesToSession() {
        PojoRecordWriter<User> writer = new PojoRecordWriter<>(cluster, session, mapper);

        // Test executeStatement delegates to session.executeAsync
        when(session.executeAsync(statement)).thenReturn(future);

        ListenableFuture<ResultSet> result = writer.executeStatement(statement);

        assertThat(result).isSameAs(future);
        verify(session, times(1)).executeAsync(statement);
        verifyNoMoreInteractions(session);

        // Test with null statement - propagates session exception
        when(session.executeAsync((Statement) null))
                .thenThrow(new IllegalArgumentException("Statement is null"));

        assertThatThrownBy(() -> writer.executeStatement(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Statement is null");

        verify(session, times(1)).executeAsync((Statement) null);
    }

    @Test
    void testWriteMethodCallsPrepareStatementThenExecuteStatement() throws Exception {
        PojoRecordWriter<User> writer = spy(new PojoRecordWriter<>(cluster, session, mapper));

        User user = new User(UUID.randomUUID(), "bob", "bob@example.com", 25);
        when(mapper.saveQuery(user)).thenReturn(statement);
        when(session.executeAsync(statement)).thenReturn(future);

        ListenableFuture<ResultSet> result = writer.write(user);

        assertThat(result).isSameAs(future);

        // Verify interaction order
        InOrder inOrder = inOrder(writer, mapper, session);
        inOrder.verify(writer).prepareStatement(user);
        inOrder.verify(mapper).saveQuery(user);
        inOrder.verify(writer).executeStatement(statement);
        inOrder.verify(session).executeAsync(statement);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void testCloseMethodClosesSessionThenCluster() throws Exception {
        PojoRecordWriter<User> writer = new PojoRecordWriter<>(cluster, session, mapper);

        // First close
        writer.close();

        // Verify close order
        InOrder closeOrder = inOrder(session, cluster);
        closeOrder.verify(session).close();
        closeOrder.verify(cluster).close();

        // Test idempotency - multiple calls don't throw
        writer.close(); // Second call
        writer.close(); // Third call

        // Verify close was called at least once (idempotent)
        verify(session, atLeastOnce()).close();
        verify(cluster, atLeastOnce()).close();
    }

    @Test
    void testNoUnexpectedInteractionsWithMocks() {
        PojoRecordWriter<User> writer = new PojoRecordWriter<>(cluster, session, mapper);
        assertThat(writer).isNotNull();
        verifyNoMoreInteractions(session, cluster, mapper);
    }
}
