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

import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.exceptions.FunctionExecutionException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.driver.core.exceptions.UnauthorizedException;
import com.datastax.driver.core.exceptions.UnsupportedFeatureException;
import com.datastax.driver.core.exceptions.UnsupportedProtocolVersionException;

import javax.naming.ConfigurationException;

import java.util.Optional;

/**
 * Defines a central list of fatal Cassandra exceptions that should not be retried.
 *
 * <p>This classifier helps distinguish between retriable transient errors (like timeouts or
 * temporary unavailability) and fatal errors that indicate configuration problems or permanent
 * failures that will not be resolved by retrying.
 *
 * <p>Fatal exceptions include:
 *
 * <ul>
 *   <li>Authentication and authorization failures
 *   <li>Query syntax and schema errors
 *   <li>Driver configuration and codec issues
 *   <li>Protocol version mismatches
 * </ul>
 */
public class CassandraFatalExceptionClassifier {

    /** Enumeration of known fatal exception types with descriptive error messages. */
    private enum FatalExceptionType {
        INVALID_QUERY(
                InvalidQueryException.class, "Encountered a non-recoverable InvalidQueryException"),
        UNAUTHORIZED(
                UnauthorizedException.class, "Encountered a non-recoverable UnauthorizedException"),
        SYNTAX_ERROR(SyntaxError.class, "Encountered a non-recoverable SyntaxError"),
        CONFIGURATION(
                ConfigurationException.class,
                "Encountered a non-recoverable ConfigurationException"),
        AUTHENTICATION(
                AuthenticationException.class,
                "Encountered a non-recoverable AuthenticationException"),
        INVALID_TYPE(
                InvalidTypeException.class, "Encountered a non-recoverable InvalidTypeException"),
        UNSUPPORTED_FEATURE(
                UnsupportedFeatureException.class,
                "Encountered a non-recoverable UnsupportedFeatureException"),
        UNSUPPORTED_PROTOCOL(
                UnsupportedProtocolVersionException.class,
                "Encountered a non-recoverable UnsupportedProtocolVersionException"),
        DRIVER_ERROR(
                DriverInternalError.class, "Encountered a non-recoverable DriverInternalError"),
        CODEC_NOT_FOUND(
                CodecNotFoundException.class,
                "Encountered a non-recoverable CodecNotFoundException"),
        FUNCTION_EXECUTION(
                FunctionExecutionException.class,
                "Encountered a non-recoverable FunctionExecutionException");

        private final Class<? extends Exception> exceptionClass;
        private final String message;

        FatalExceptionType(Class<? extends Exception> exceptionClass, String message) {
            this.exceptionClass = exceptionClass;
            this.message = message;
        }
    }

    /**
     * Checks if the given exception or any of its root causes is a fatal Cassandra exception and
     * returns the specific error message if it is fatal.
     *
     * @param throwable the exception to check
     * @return Optional containing the fatal error message, or empty if not fatal
     */
    public static Optional<String> getFatalExceptionMessage(Throwable throwable) {
        if (throwable == null) {
            return Optional.empty();
        }

        // Check the entire cause chain
        Throwable current = throwable;
        while (current != null) {
            // Check against all fatal exception types
            for (FatalExceptionType type : FatalExceptionType.values()) {
                if (type.exceptionClass.isInstance(current)) {
                    return Optional.of(type.message);
                }
            }
            current = current.getCause();
        }
        return Optional.empty();
    }

    /**
     * Checks if the given exception or any of its root causes is a fatal Cassandra exception.
     *
     * @param throwable the exception to check
     * @return true if the exception is fatal and should not be retried
     */
    public static boolean isFatalException(Throwable throwable) {
        return getFatalExceptionMessage(throwable).isPresent();
    }
}
