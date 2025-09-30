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

package org.apache.flink.connector.cassandra.sink.planner.cache;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.cache.CacheStats;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Unit tests for {@link PreparedStatementCache}. */
public class PreparedStatementCacheTest {

    @Mock private Session session;
    @Mock private PreparedStatement preparedStatement1;
    @Mock private PreparedStatement preparedStatement2;

    private PreparedStatementCache cache;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        cache = new PreparedStatementCache();
    }

    @Test
    void testReturnsSameInstanceForSameKey() {
        String insertQuery = "INSERT INTO ks.table (id, name) VALUES (?, ?)";
        when(session.prepare(insertQuery)).thenReturn(preparedStatement1);

        PreparedStatement result1 = cache.getOrPrepare(session, insertQuery);
        PreparedStatement result2 = cache.getOrPrepare(session, insertQuery);

        assertThat(result1).isSameAs(result2);
        verify(session, times(1)).prepare(insertQuery);
    }

    @Test
    void testDifferentKeysReturnDifferentStatements() {
        String insertQuery1 = "INSERT INTO ks.table1 (id) VALUES (?)";
        String insertQuery2 = "INSERT INTO ks.table2 (id, name) VALUES (?, ?)";
        when(session.prepare(insertQuery1)).thenReturn(preparedStatement1);
        when(session.prepare(insertQuery2)).thenReturn(preparedStatement2);

        PreparedStatement result1 = cache.getOrPrepare(session, insertQuery1);
        PreparedStatement result2 = cache.getOrPrepare(session, insertQuery2);

        assertThat(result1).isNotSameAs(result2);
        assertThat(result1).isSameAs(preparedStatement1);
        assertThat(result2).isSameAs(preparedStatement2);
    }

    @Test
    void testExceptionsArePropagatedWithWrapping() {
        // Test session.prepare() exceptions
        RuntimeException sessionException = new RuntimeException("Session prepare failed");
        when(session.prepare(anyString())).thenThrow(sessionException);

        assertThatThrownBy(
                        () -> cache.getOrPrepare(session, "INSERT INTO ks.table (id) VALUES (?)"))
                .isInstanceOf(UncheckedExecutionException.class)
                .hasCauseInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("Session prepare failed");
    }

    @Test
    void testThreadSafety() throws InterruptedException {
        String insertQuery = "INSERT INTO ks.table (id) VALUES (?)";
        when(session.prepare(insertQuery)).thenReturn(preparedStatement1);

        int threadCount = 10;
        CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CompletableFuture<PreparedStatement>[] futures = new CompletableFuture[threadCount];

        for (int i = 0; i < threadCount; i++) {
            futures[i] =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return cache.getOrPrepare(session, insertQuery);
                                } finally {
                                    latch.countDown();
                                }
                            },
                            executor);
        }

        latch.await();
        executor.shutdown();

        PreparedStatement firstResult = futures[0].join();
        for (CompletableFuture<PreparedStatement> future : futures) {
            assertThat(future.join()).isSameAs(firstResult);
        }

        verify(session, times(1)).prepare(insertQuery);
    }

    @Test
    void testStatisticsTracking() {
        // Create a fresh cache for this test to avoid contamination from other tests
        PreparedStatementCache testCache = new PreparedStatementCache();
        String insertQuery = "INSERT INTO ks.table (id) VALUES (?)";
        when(session.prepare(insertQuery)).thenReturn(preparedStatement1);

        testCache.getOrPrepare(session, insertQuery); // miss
        testCache.getOrPrepare(session, insertQuery); // hit
        testCache.getOrPrepare(session, insertQuery); // hit

        CacheStats stats = testCache.getStats();
        // The cache uses getIfPresent first, then get if not present
        // So first call: getIfPresent (miss) + get (load) = 2 requests, 1 miss
        // Subsequent calls: getIfPresent (hit) = 1 request each, 1 hit each
        // Total: 4 requests, 2 hits, 1 miss, 1 load
        assertThat(stats.requestCount()).isEqualTo(4);
        assertThat(stats.hitCount()).isEqualTo(2);
        assertThat(stats.missCount()).isEqualTo(2);
        assertThat(stats.loadCount()).isEqualTo(1);
    }

    @Test
    void testCustomCacheSize() {
        PreparedStatementCache customCache = new PreparedStatementCache(500);
        String insertQuery = "INSERT INTO ks.table (id) VALUES (?)";
        when(session.prepare(insertQuery)).thenReturn(preparedStatement1);

        PreparedStatement result = customCache.getOrPrepare(session, insertQuery);
        assertThat(result).isSameAs(preparedStatement1);
    }

    @Test
    void testCacheAvoidsBugWithSimilarQueriesDifferentModifiers() {
        // This test demonstrates that different queries get different cache entries
        String queryWithoutTTL = "INSERT INTO ks.table (id, name) VALUES (?, ?)";
        String queryWithTTL = "INSERT INTO ks.table (id, name) VALUES (?, ?) USING TTL 60";

        when(session.prepare(queryWithoutTTL)).thenReturn(preparedStatement1);
        when(session.prepare(queryWithTTL)).thenReturn(preparedStatement2);

        // Each query should get its own prepared statement using the query as cache key
        PreparedStatement result1 = cache.getOrPrepare(session, queryWithoutTTL);
        PreparedStatement result2 = cache.getOrPrepare(session, queryWithTTL);

        assertThat(result1).isSameAs(preparedStatement1);
        assertThat(result2).isSameAs(preparedStatement2);
        assertThat(result1).isNotSameAs(result2);

        // Verify both queries were prepared separately
        verify(session, times(1)).prepare(queryWithoutTTL);
        verify(session, times(1)).prepare(queryWithTTL);
    }

    @Test
    void testInputValidation() {
        // Null session
        assertThatThrownBy(() -> cache.getOrPrepare(null, "query"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("session cannot be null");

        // Null query
        assertThatThrownBy(() -> cache.getOrPrepare(session, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cqlQuery cannot be empty");

        // Empty query
        assertThatThrownBy(() -> cache.getOrPrepare(session, ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cqlQuery cannot be empty");
    }

    @Test
    void testConcurrentDifferentQueries() throws InterruptedException {
        String query1 = "INSERT INTO ks.table1 (id) VALUES (?)";
        String query2 = "INSERT INTO ks.table2 (name) VALUES (?)";
        when(session.prepare(query1)).thenReturn(preparedStatement1);
        when(session.prepare(query2)).thenReturn(preparedStatement2);

        int threadCount = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CompletableFuture<PreparedStatement>[] futures = new CompletableFuture[threadCount];

        for (int i = 0; i < threadCount; i++) {
            final boolean useQuery1 = i % 2 == 0;
            futures[i] =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    startLatch.await();
                                    String query = useQuery1 ? query1 : query2;
                                    return cache.getOrPrepare(session, query);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                } finally {
                                    finishLatch.countDown();
                                }
                            },
                            executor);
        }

        startLatch.countDown();
        finishLatch.await();
        executor.shutdown();

        // Verify each query prepared exactly once
        verify(session, times(1)).prepare(query1);
        verify(session, times(1)).prepare(query2);

        // Verify correct statements returned
        for (int i = 0; i < threadCount; i++) {
            PreparedStatement expected = i % 2 == 0 ? preparedStatement1 : preparedStatement2;
            assertThat(futures[i].join()).isSameAs(expected);
        }
    }

    @Test
    void testCacheEvictionWithSmallSize() {
        PreparedStatementCache smallCache = new PreparedStatementCache(2);
        when(session.prepare(anyString()))
                .thenAnswer(
                        invocation -> {
                            // Return a new mock for each unique query
                            return org.mockito.Mockito.mock(PreparedStatement.class);
                        });

        // Fill cache to capacity
        smallCache.getOrPrepare(session, "query1");
        smallCache.getOrPrepare(session, "query2");

        assertThat(smallCache.size()).isLessThanOrEqualTo(2);

        // Add third entry should evict least recently used
        smallCache.getOrPrepare(session, "query3");
        assertThat(smallCache.size()).isLessThanOrEqualTo(2);
    }

    @Test
    void testCloseEmptiesCache() {
        String query = "INSERT INTO ks.table (id) VALUES (?)";
        when(session.prepare(query)).thenReturn(preparedStatement1);

        cache.getOrPrepare(session, query);
        assertThat(cache.size()).isGreaterThan(0);

        cache.close();
        assertThat(cache.size()).isEqualTo(0);

        // After close, cache should prepare again
        cache.getOrPrepare(session, query);
        verify(session, times(2)).prepare(query);
    }

    @Test
    void testErrorNotCached() {
        RuntimeException error = new RuntimeException("Prepare failed");
        when(session.prepare(anyString())).thenThrow(error).thenReturn(preparedStatement1);

        // First call fails
        assertThatThrownBy(() -> cache.getOrPrepare(session, "query"))
                .isInstanceOf(UncheckedExecutionException.class)
                .hasCauseInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("Prepare failed");

        // Second call should succeed and prepare again (error not cached)
        PreparedStatement result = cache.getOrPrepare(session, "query");
        assertThat(result).isSameAs(preparedStatement1);
        verify(session, times(2)).prepare("query");
    }
}
