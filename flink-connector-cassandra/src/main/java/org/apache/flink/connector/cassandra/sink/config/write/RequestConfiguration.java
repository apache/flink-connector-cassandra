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

package org.apache.flink.connector.cassandra.sink.config.write;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.time.Duration;

/**
 * Configuration for controlling Cassandra sink request behavior including retry handling,
 * connection concurrency, and timeout limits.
 *
 * <p>This configuration determines how the sink handles:
 *
 * <ul>
 *   <li>Maximum number of concurrent requests to Cassandra
 *   <li>Number of retry attempts for failed requests
 *   <li>Timeout for acquiring permits during normal writes
 *   <li>Timeout for draining all in-flight requests during flush/checkpoint
 * </ul>
 *
 * <p><strong>Important:</strong> Separate timeouts for different operations are used.
 *
 * <ul>
 *   <li>{@code maxTimeout} - Used during regular write operations when acquiring permits. Should be
 *       generous to handle normal backpressure without failing the job.
 *   <li>{@code flushTimeout} - Used during flush/checkpoint when draining all in-flight requests.
 *       Should be bounded to ensure checkpoints complete or fail promptly.
 * </ul>
 */
@PublicEvolving
public class RequestConfiguration implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Default maximum number of concurrent Cassandra requests. */
    public static final int DEFAULT_MAX_CONCURRENT_REQUESTS = Integer.MAX_VALUE;

    /** Default number of retries on failure. */
    public static final int DEFAULT_MAX_RETRIES = 0;

    /** Default maximum timeout for acquiring permits during write operations. */
    public static final Duration DEFAULT_MAX_TIMEOUT = Duration.ofMinutes(1);

    /** Default timeout for draining all requests during flush/checkpoint. */
    public static final Duration DEFAULT_FLUSH_TIMEOUT = Duration.ofSeconds(30);

    private final int maxRetries;
    private final int maxConcurrentRequests;
    private final Duration maxTimeout;
    private final Duration flushTimeout;

    /**
     * Private constructor. Use {@link Builder} to create instances.
     *
     * @param maxRetries number of retry attempts per record
     * @param maxConcurrentRequests maximum number of concurrent Cassandra requests
     * @param maxTimeout maximum time to wait for acquiring permits during writes
     * @param flushTimeout maximum time to wait for draining requests during flush
     */
    private RequestConfiguration(
            int maxRetries, int maxConcurrentRequests, Duration maxTimeout, Duration flushTimeout) {
        this.maxRetries = maxRetries;
        this.maxConcurrentRequests = maxConcurrentRequests;
        this.maxTimeout = maxTimeout;
        this.flushTimeout = flushTimeout;
    }

    /**
     * Gets the maximum number of retry attempts for failed requests.
     *
     * @return maximum retry attempts
     */
    public int getMaxRetries() {
        return maxRetries;
    }

    /**
     * Gets the maximum number of concurrent requests allowed to Cassandra.
     *
     * @return maximum concurrent requests
     */
    public int getMaxConcurrentRequests() {
        return maxConcurrentRequests;
    }

    /**
     * Gets the maximum timeout for acquiring permits during write operations.
     *
     * <p>This timeout is used when acquiring permits for individual writes. It should be generous
     * enough to handle normal backpressure without causing unnecessary job failures.
     *
     * @return maximum timeout duration for permit acquisition
     */
    public Duration getMaxTimeout() {
        return maxTimeout;
    }

    /**
     * Gets the timeout for draining all in-flight requests during flush operations.
     *
     * <p>This timeout is used during checkpoints when waiting for all pending requests to complete.
     * It should be bounded to ensure checkpoints either succeed or fail within a reasonable time,
     * avoiding checkpoint timeout issues.
     *
     * @return flush timeout duration
     */
    public Duration getFlushTimeout() {
        return flushTimeout;
    }

    /**
     * Creates a new builder for RequestConfiguration.
     *
     * @return a new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link RequestConfiguration}. */
    @PublicEvolving
    public static class Builder {

        private Integer maxRetries = DEFAULT_MAX_RETRIES;
        private Integer maxConcurrentRequests = DEFAULT_MAX_CONCURRENT_REQUESTS;
        private Duration maxTimeout = DEFAULT_MAX_TIMEOUT;
        private Duration flushTimeout = DEFAULT_FLUSH_TIMEOUT;

        /**
         * Sets the maximum number of retries for failed requests.
         *
         * @param maxRetries maximum retry attempts (must be >= 0)
         * @return this builder
         * @throws IllegalArgumentException if maxRetries is negative
         */
        public Builder setMaxRetries(Integer maxRetries) {
            Preconditions.checkArgument(
                    maxRetries != null && maxRetries >= 0, "maxRetries must be non-null and >= 0");
            this.maxRetries = maxRetries;
            return this;
        }

        /**
         * Sets the maximum number of concurrent Cassandra requests.
         *
         * @param maxConcurrentRequests maximum concurrent requests (must be > 0)
         * @return this builder
         * @throws IllegalArgumentException if maxConcurrentRequests is <= 0
         */
        public Builder setMaxConcurrentRequests(Integer maxConcurrentRequests) {
            Preconditions.checkArgument(
                    maxConcurrentRequests != null && maxConcurrentRequests > 0,
                    "maxConcurrentRequests must be non-null and > 0");
            this.maxConcurrentRequests = maxConcurrentRequests;
            return this;
        }

        /**
         * Sets the maximum timeout duration for acquiring permits during write operations.
         *
         * @param maxTimeout timeout duration (must be positive)
         * @return this builder
         * @throws IllegalArgumentException if maxTimeout is not positive
         */
        public Builder setMaxTimeout(Duration maxTimeout) {
            Preconditions.checkArgument(
                    maxTimeout != null && !maxTimeout.isZero() && !maxTimeout.isNegative(),
                    "maxTimeout must be non-null and positive");
            this.maxTimeout = maxTimeout;
            return this;
        }

        /**
         * Sets the timeout for draining all in-flight requests during flush operations.
         *
         * <p>This timeout should be aligned with your checkpoint interval and timeout settings. A
         * good rule of thumb is to set it to less than half of your checkpoint timeout to allow
         * time for other checkpoint operations.
         *
         * @param flushTimeout flush timeout duration (must be positive)
         * @return this builder
         * @throws IllegalArgumentException if flushTimeout is not positive
         */
        public Builder setFlushTimeout(Duration flushTimeout) {
            Preconditions.checkArgument(
                    flushTimeout != null && !flushTimeout.isZero() && !flushTimeout.isNegative(),
                    "flushTimeout must be non-null and positive");
            this.flushTimeout = flushTimeout;
            return this;
        }

        /**
         * Builds the {@link RequestConfiguration} using the configured values or defaults where
         * values were not set.
         *
         * @return configured RequestConfiguration instance
         */
        public RequestConfiguration build() {
            return new RequestConfiguration(
                    maxRetries, maxConcurrentRequests, maxTimeout, flushTimeout);
        }
    }
}
