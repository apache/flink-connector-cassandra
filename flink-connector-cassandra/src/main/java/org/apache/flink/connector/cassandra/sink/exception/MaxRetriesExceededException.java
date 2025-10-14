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

package org.apache.flink.connector.cassandra.sink.exception;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Exception thrown when maximum retry attempts are exhausted for a record. This is considered a
 * terminal failure that by default fails the job to preserve at-least-once semantics.
 */
@Internal
public final class MaxRetriesExceededException extends IOException {
    private static final long serialVersionUID = 1L;
    private final int attempts;
    private final String elementPreview;

    public MaxRetriesExceededException(@Nullable Object element, int attempts, Throwable cause) {
        super(
                "Max retries exceeded (attempts="
                        + attempts
                        + ") for element="
                        + summarize(element),
                cause);
        this.attempts = attempts;
        this.elementPreview = summarize(element);
    }

    public int getAttempts() {
        return attempts;
    }

    public String getElementPreview() {
        return elementPreview;
    }

    private static String summarize(@Nullable Object element) {
        if (element == null) {
            return "null";
        }
        String str = String.valueOf(element);
        return str.length() > 512 ? str.substring(0, 512) + "..." : str;
    }
}
