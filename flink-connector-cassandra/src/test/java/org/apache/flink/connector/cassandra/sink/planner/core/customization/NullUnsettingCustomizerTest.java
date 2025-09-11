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

package org.apache.flink.connector.cassandra.sink.planner.core.customization;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Unit tests for {@link NullUnsettingCustomizer}. */
public class NullUnsettingCustomizerTest {

    @Mock private BoundStatement boundStatement;
    @Mock private Statement nonBoundStatement;
    @Mock private PreparedStatement preparedStatement;
    @Mock private ColumnDefinitions columnDefinitions;

    private NullUnsettingCustomizer<String> customizer;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        customizer = new NullUnsettingCustomizer<>();

        // Common mock setup for BoundStatement
        when(boundStatement.preparedStatement()).thenReturn(preparedStatement);
        when(preparedStatement.getVariables()).thenReturn(columnDefinitions);
    }

    @Test
    void testUnsetsExplicitlySetNulls() {
        // Setup: statement has 3 variables, middle one is null
        when(columnDefinitions.size()).thenReturn(3);

        when(boundStatement.isSet(0)).thenReturn(true);
        when(boundStatement.isSet(1)).thenReturn(true);
        when(boundStatement.isSet(2)).thenReturn(true);

        when(boundStatement.isNull(0)).thenReturn(false);
        when(boundStatement.isNull(1)).thenReturn(true);
        when(boundStatement.isNull(2)).thenReturn(false);

        customizer.apply(boundStatement, "test-record");

        verify(boundStatement, never()).unset(0);
        verify(boundStatement).unset(1);
        verify(boundStatement, never()).unset(2);
    }

    @Test
    void testMultipleNullsUnset() {
        // Setup: multiple nulls
        when(columnDefinitions.size()).thenReturn(4);

        when(boundStatement.isSet(0)).thenReturn(true);
        when(boundStatement.isSet(1)).thenReturn(true);
        when(boundStatement.isSet(2)).thenReturn(true);
        when(boundStatement.isSet(3)).thenReturn(true);

        when(boundStatement.isNull(0)).thenReturn(true); // null
        when(boundStatement.isNull(1)).thenReturn(false);
        when(boundStatement.isNull(2)).thenReturn(true); // null
        when(boundStatement.isNull(3)).thenReturn(true); // null

        // Execute
        customizer.apply(boundStatement, "test-record");

        // Verify
        verify(boundStatement).unset(0);
        verify(boundStatement, never()).unset(1);
        verify(boundStatement).unset(2);
        verify(boundStatement).unset(3);
    }

    @Test
    void testNonNullValuesRemainSet() {
        when(columnDefinitions.size()).thenReturn(3);

        when(boundStatement.isSet(0)).thenReturn(true);
        when(boundStatement.isSet(1)).thenReturn(true);
        when(boundStatement.isSet(2)).thenReturn(true);

        when(boundStatement.isNull(0)).thenReturn(false);
        when(boundStatement.isNull(1)).thenReturn(false);
        when(boundStatement.isNull(2)).thenReturn(false);

        // Execute
        customizer.apply(boundStatement, "test-record");

        // Verify nothing was unset
        verify(boundStatement, never()).unset(anyInt());
    }

    @Test
    void testAlreadyUnsetPositionsRemainUnset() {
        when(columnDefinitions.size()).thenReturn(3);

        when(boundStatement.isSet(0)).thenReturn(true);
        when(boundStatement.isSet(1)).thenReturn(false); // Already unset
        when(boundStatement.isSet(2)).thenReturn(true);

        when(boundStatement.isNull(0)).thenReturn(false);
        // isNull(1) should not be called since it's not set
        when(boundStatement.isNull(2)).thenReturn(true);

        customizer.apply(boundStatement, "test-record");
        verify(boundStatement, never()).unset(0);
        verify(boundStatement, never()).unset(1); // Already unset, not touched
        verify(boundStatement).unset(2);
        verify(boundStatement, never()).isNull(1); // Should not check null for unset position
    }

    @Test
    void testNonBoundStatementIsNoOp() {
        customizer.apply(nonBoundStatement, "test-record");
        verifyNoInteractions(nonBoundStatement);
    }

    @Test
    void testZeroVariablesStatement() {
        // Setup: statement with no variables
        when(columnDefinitions.size()).thenReturn(0);

        // Execute
        customizer.apply(boundStatement, "test-record");

        // Verify no operations performed
        verify(boundStatement, never()).isSet(anyInt());
        verify(boundStatement, never()).isNull(anyInt());
        verify(boundStatement, never()).unset(anyInt());
    }

    @Test
    void testIdempotency() {
        when(columnDefinitions.size()).thenReturn(2);

        when(boundStatement.isSet(0)).thenReturn(true);
        when(boundStatement.isSet(1)).thenReturn(true);
        when(boundStatement.isNull(0)).thenReturn(false);
        when(boundStatement.isNull(1)).thenReturn(true);

        customizer.apply(boundStatement, "test-record");

        when(boundStatement.isSet(1)).thenReturn(false);

        customizer.apply(boundStatement, "test-record");

        verify(boundStatement, times(1)).unset(1);
    }

    @Test
    void testNullStatementReturnsNull() {
        customizer.apply(null, "test-record");
    }
}
