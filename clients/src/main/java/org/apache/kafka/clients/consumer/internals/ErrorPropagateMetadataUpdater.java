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

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MetadataUpdater;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;

import java.util.List;
import java.util.Optional;

public class ErrorPropagateMetadataUpdater implements MetadataUpdater {
    private final MetadataUpdater metadataUpdater;
    private final Metadata metadata;
    private final BackgroundEventHandler backgroundEventHandler;

    public ErrorPropagateMetadataUpdater(MetadataUpdater metadataUpdater, Metadata metadata, BackgroundEventHandler backgroundEventHandler) {
        assert metadataUpdater != null;
        assert metadata != null;
        assert backgroundEventHandler != null;

        this.metadataUpdater = metadataUpdater;
        this.metadata = metadata;
        this.backgroundEventHandler = backgroundEventHandler;
    }

    @Override
    public List<Node> fetchNodes() {
        return metadataUpdater.fetchNodes();
    }

    @Override
    public boolean isUpdateDue(long now) {
        return metadataUpdater.isUpdateDue(now);
    }

    @Override
    public long maybeUpdate(long now) {
        return metadataUpdater.maybeUpdate(now);
    }

    @Override
    public void handleServerDisconnect(long now, String destinationId, Optional<AuthenticationException> maybeFatalException) {
        metadataUpdater.handleServerDisconnect(now, destinationId, maybeFatalException);
        propagateError();
    }

    @Override
    public void handleFailedRequest(long now, Optional<KafkaException> maybeFatalException) {
        metadataUpdater.handleFailedRequest(now, maybeFatalException);
        propagateError();
    }

    @Override
    public void handleSuccessfulResponse(RequestHeader requestHeader, long now, MetadataResponse response) {
        metadataUpdater.handleSuccessfulResponse(requestHeader, now, response);
        propagateError();
    }

    private void propagateError() {
        KafkaException metadataException = metadata.maybeGetAnyException();
        if (metadataException != null) {
            backgroundEventHandler.add(new ErrorEvent(metadataException));
        }
    }

    @Override
    public void close() {
        metadataUpdater.close();
    }
}