/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.perf;

import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.ClientFactory;
import io.pravega.client.stream.impl.ByteArraySerializer;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReinitializationRequiredException;

/**
 * Class for Pravega reader/consumer.
 */
public class PravegaReaderWorker extends ReaderWorker {
    private final EventStreamReader<byte[]> reader;

    PravegaReaderWorker(int readerId, int events, int secondsToRun,
                        long start, PerfStats stats, String readergrp,
                        int timeout, boolean writeAndRead, ClientFactory factory) {
        super(readerId, events, secondsToRun, start, stats, readergrp, timeout, writeAndRead);

        final String readerSt = Integer.toString(readerId);
        reader = factory.createReader(
                readerSt, readergrp, new ByteArraySerializer(), ReaderConfig.builder().build());
    }

    @Override
    public byte[] readData() {
        try {
            return reader.readNextEvent(timeout).getEvent();
        } catch (ReinitializationRequiredException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() {
        reader.close();
    }
}
