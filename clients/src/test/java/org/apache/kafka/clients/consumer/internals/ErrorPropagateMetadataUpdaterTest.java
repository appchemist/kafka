/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MetadataUpdater;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ErrorPropagateMetadataUpdaterTest {
    private Metadata metadata;
    private MetadataUpdater metadataUpdater;
    private Queue<BackgroundEvent> backgroundEventQueue;

    @BeforeEach
    public void setUp() {
        metadata = new Metadata(100, 100, 50000, new LogContext(),
                new ClusterResourceListeners());
        backgroundEventQueue = new LinkedList<>();
        metadataUpdater = new ErrorPropagateMetadataUpdater(
                new ManualMetadataUpdater(),
                metadata,
                new BackgroundEventHandler(new LogContext(), backgroundEventQueue));
    }

    @Test
    public void testHandleServerDisconnect() {
        metadataUpdater.handleServerDisconnect(0, "nodeId", Optional.empty());

        assertNull(backgroundEventQueue.poll());
    }

    @Test
    public void testHandleFailedRequest() {
        metadataUpdater.handleFailedRequest(0, Optional.empty());

        assertNull(backgroundEventQueue.poll());
    }

    @Test
    public void testHandleSuccessfulResponse() {
        metadataUpdater.handleSuccessfulResponse(null, 0, null);

        assertNull(backgroundEventQueue.poll());
    }

    @Test
    public void testHandleServerDisconnectWithError() {
        KafkaException error = new AuthenticationException("Auth Exception");

        metadata.fatalError(error);
        metadataUpdater.handleServerDisconnect(0, "nodeId", Optional.empty());

        ErrorEvent errorEvent = (ErrorEvent) backgroundEventQueue.poll();
        assertNotNull(errorEvent);
        assertEquals(error, errorEvent.error());
    }

    @Test
    public void testHandleFailedRequestWithError() {
        KafkaException error = new UnsupportedVersionException("Unsupported Version Exception");

        metadata.fatalError(error);
        metadataUpdater.handleFailedRequest(0, Optional.empty());

        ErrorEvent errorEvent = (ErrorEvent) backgroundEventQueue.poll();
        assertNotNull(errorEvent);
        assertEquals(error, errorEvent.error());
    }

    @Test
    public void testHandleSuccessfulResponseWithError() {
        KafkaException error = new InvalidTopicException();

        metadata.fatalError(error);
        metadataUpdater.handleSuccessfulResponse(null, 0, null);

        ErrorEvent errorEvent = (ErrorEvent) backgroundEventQueue.poll();
        assertNotNull(errorEvent);
        assertEquals(error, errorEvent.error());
    }
}