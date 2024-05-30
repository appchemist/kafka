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
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;

public class MetadataErrorManager implements RequestManager {
    private final Metadata metadata;
    private final BackgroundEventHandler backgroundEventHandler;

    public MetadataErrorManager(Metadata metadata, BackgroundEventHandler backgroundEventHandler) {
        this.metadata = metadata;
        this.backgroundEventHandler = backgroundEventHandler;
    }

    @Override
    public PollResult poll(long currentTimeMs) {
        try {
            metadata.maybeThrowAnyException();
        } catch (Exception ex) {
            backgroundEventHandler.add(new ErrorEvent(ex));
        }

        return PollResult.EMPTY;
    }
}