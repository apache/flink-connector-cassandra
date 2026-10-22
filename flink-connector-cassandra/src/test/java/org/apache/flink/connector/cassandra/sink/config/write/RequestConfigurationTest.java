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

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link RequestConfiguration}. */
class RequestConfigurationTest {

    @Test
    void testDefaultValues() {
        RequestConfiguration config = RequestConfiguration.builder().build();

        // Verify all defaults match constants
        assertThat(config.getMaxConcurrentRequests())
                .isEqualTo(RequestConfiguration.DEFAULT_MAX_CONCURRENT_REQUESTS)
                .isEqualTo(Integer.MAX_VALUE);
        assertThat(config.getMaxRetries())
                .isEqualTo(RequestConfiguration.DEFAULT_MAX_RETRIES)
                .isEqualTo(0);
        assertThat(config.getMaxTimeout())
                .isEqualTo(RequestConfiguration.DEFAULT_MAX_TIMEOUT)
                .isEqualTo(Duration.ofMinutes(1));
        assertThat(config.getFlushTimeout())
                .isEqualTo(RequestConfiguration.DEFAULT_FLUSH_TIMEOUT)
                .isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void testCustomValues() {
        RequestConfiguration config =
                RequestConfiguration.builder()
                        .setMaxTimeout(Duration.ofSeconds(5))
                        .setMaxRetries(5)
                        .setMaxConcurrentRequests(500)
                        .setFlushTimeout(Duration.ofSeconds(10))
                        .build();

        assertThat(config.getMaxTimeout()).isEqualTo(Duration.ofSeconds(5));
        assertThat(config.getMaxRetries()).isEqualTo(5);
        assertThat(config.getMaxConcurrentRequests()).isEqualTo(500);
        assertThat(config.getFlushTimeout()).isEqualTo(Duration.ofSeconds(10));
    }

    @Test
    void testBuilderValidation() {
        // Test maxConcurrentRequests validation
        assertThatThrownBy(() -> RequestConfiguration.builder().setMaxConcurrentRequests(0).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxConcurrentRequests must be");

        assertThatThrownBy(
                        () -> RequestConfiguration.builder().setMaxConcurrentRequests(-1).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxConcurrentRequests must be");

        // Test maxTimeout validation
        assertThatThrownBy(
                        () -> RequestConfiguration.builder().setMaxTimeout(Duration.ZERO).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxTimeout must be non-null and positive");

        assertThatThrownBy(
                        () ->
                                RequestConfiguration.builder()
                                        .setMaxTimeout(Duration.ofSeconds(-1))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxTimeout must be non-null and positive");

        assertThatThrownBy(() -> RequestConfiguration.builder().setMaxTimeout(null).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxTimeout must be non-null and positive");

        // Test maxRetries validation
        assertThatThrownBy(() -> RequestConfiguration.builder().setMaxRetries(-1).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxRetries must be");

        // Zero retries should be allowed (means no retries)
        RequestConfiguration config = RequestConfiguration.builder().setMaxRetries(0).build();
        assertThat(config.getMaxRetries()).isEqualTo(0);
    }

    @Test
    void testCombinedSetters() {
        // Test if there are any convenience methods that set multiple values
        RequestConfiguration config =
                RequestConfiguration.builder()
                        .setMaxTimeout(Duration.ofMillis(1500))
                        .setMaxRetries(2)
                        .setMaxConcurrentRequests(100)
                        .build();

        assertThat(config.getMaxTimeout()).isEqualTo(Duration.ofMillis(1500));
        assertThat(config.getMaxRetries()).isEqualTo(2);
        assertThat(config.getMaxConcurrentRequests()).isEqualTo(100);
    }

    @Test
    void testSerialization() throws Exception {
        RequestConfiguration original =
                RequestConfiguration.builder()
                        .setMaxTimeout(Duration.ofSeconds(10))
                        .setMaxRetries(7)
                        .setMaxConcurrentRequests(2000)
                        .setFlushTimeout(Duration.ofSeconds(45))
                        .build();

        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(original);
        oos.close();

        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        RequestConfiguration deserialized = (RequestConfiguration) ois.readObject();
        ois.close();

        // Verify all fields preserved
        assertThat(deserialized.getMaxTimeout()).isEqualTo(original.getMaxTimeout());
        assertThat(deserialized.getMaxRetries()).isEqualTo(original.getMaxRetries());
        assertThat(deserialized.getMaxConcurrentRequests())
                .isEqualTo(original.getMaxConcurrentRequests());
        assertThat(deserialized.getFlushTimeout()).isEqualTo(original.getFlushTimeout());
    }

    @Test
    void testRejectsNonPositiveFlushTimeout() {
        assertThatThrownBy(
                        () -> RequestConfiguration.builder().setFlushTimeout(Duration.ZERO).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("flushTimeout must be non-null and positive");

        assertThatThrownBy(
                        () ->
                                RequestConfiguration.builder()
                                        .setFlushTimeout(Duration.ofSeconds(-1))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("flushTimeout must be non-null and positive");

        assertThatThrownBy(() -> RequestConfiguration.builder().setFlushTimeout(null).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("flushTimeout must be non-null and positive");
    }

    @Test
    void testFlushTimeoutAndMaxTimeoutAreIndependent() {
        // Test that flushTimeout and maxTimeout are independent
        RequestConfiguration config1 =
                RequestConfiguration.builder()
                        .setMaxTimeout(Duration.ofSeconds(5))
                        .setFlushTimeout(Duration.ofMinutes(2))
                        .build();

        assertThat(config1.getMaxTimeout()).isEqualTo(Duration.ofSeconds(5));
        assertThat(config1.getFlushTimeout()).isEqualTo(Duration.ofMinutes(2));

        // Opposite configuration
        RequestConfiguration config2 =
                RequestConfiguration.builder()
                        .setMaxTimeout(Duration.ofMinutes(10))
                        .setFlushTimeout(Duration.ofSeconds(1))
                        .build();

        assertThat(config2.getMaxTimeout()).isEqualTo(Duration.ofMinutes(10));
        assertThat(config2.getFlushTimeout()).isEqualTo(Duration.ofSeconds(1));
    }
}
