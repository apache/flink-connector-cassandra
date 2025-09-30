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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import org.apache.commons.lang3.StringUtils;

import java.util.concurrent.ExecutionException;

/**
 * Cache for prepared statements to avoid re-preparing identical queries using Guava Cache.
 *
 * <p>This is especially important for dynamic use cases where the same queries are used repeatedly
 * across many records. Cache keys are the actual query strings to ensure different queries (e.g.,
 * with different TTL values) get separate cache entries. Uses Guava's thread-safe cache with
 * configurable size limits and statistics tracking.
 *
 * <p><b>Cache Key Examples by Strategy:</b>
 *
 * <ul>
 *   <li><b>StaticInsertStrategy:</b> User-provided query string<br>
 *       e.g., "INSERT INTO keyspace.events (id, timestamp, data) VALUES (?, ?, ?) USING TTL 3600"
 *   <li><b>StaticUpdateStrategy:</b> User-provided query string<br>
 *       e.g., "UPDATE keyspace.events SET data = ?, status = ? WHERE id = ? AND timestamp = ?"
 *   <li><b>InsertStrategy:</b> Generated INSERT query (varies if dynamic table/columns)<br>
 *       e.g., "INSERT INTO analytics.user_events (user_id,event_type,timestamp) VALUES (?,?,?)"
 *   <li><b>UpdateStrategy:</b> Generated UPDATE query (varies if dynamic table/columns)<br>
 *       e.g., "UPDATE analytics.user_profiles SET name=?,email=? WHERE user_id=?"
 * </ul>
 *
 * <p><b>Important:</b> One instance per Cassandra Session. Prepared statements are tied to a
 * specific session and are not portable between sessions.
 */
@Internal
public class PreparedStatementCache implements AutoCloseable {

    private static final int DEFAULT_MAXIMUM_SIZE = 1000;
    private final Cache<String, PreparedStatement> cache;

    /** Creates a cache with default configuration. */
    public PreparedStatementCache() {
        this(DEFAULT_MAXIMUM_SIZE);
    }

    /**
     * Creates a cache with specified maximum size.
     *
     * <p>Note: Prepared statements are not portable across sessions. Each session requires its own
     * cache instance.
     *
     * @param maximumSize maximum number of entries to cache
     */
    PreparedStatementCache(int maximumSize) {
        this.cache = CacheBuilder.newBuilder().maximumSize(maximumSize).recordStats().build();
    }

    /**
     * Gets a prepared statement from cache or creates a new one.
     *
     * @param session the Cassandra session
     * @param cqlQuery Key for the cache
     * @return the prepared statement
     */
    public PreparedStatement getOrPrepare(Session session, String cqlQuery) {
        Preconditions.checkArgument(
                !StringUtils.isEmpty(cqlQuery),
                "cqlQuery cannot be empty, it is the key for the cache.");
        Preconditions.checkArgument(session != null, "session cannot be null.");
        PreparedStatement ps = cache.getIfPresent(cqlQuery);
        if (ps != null) {
            return ps;
        }
        try {
            return cache.get(cqlQuery, () -> session.prepare(cqlQuery));
        } catch (ExecutionException e) {
            throw new RuntimeException(
                    String.format("Failed to prepare CQL [%s]", cqlQuery), e.getCause());
        }
    }

    /** Returns the number of cached prepared statements. */
    public long size() {
        return cache.size();
    }

    /**
     * Returns cache statistics including hits, misses, evictions.
     *
     * @return cache statistics for monitoring
     */
    public CacheStats getStats() {
        return cache.stats();
    }

    /**
     * Closes the cache and releases all resources.
     *
     * <p>This method clears all cached prepared statements and should be called when the cache is
     * no longer needed to ensure proper cleanup.
     */
    @Override
    public void close() {
        cache.invalidateAll();
        cache.cleanUp();
    }
}
