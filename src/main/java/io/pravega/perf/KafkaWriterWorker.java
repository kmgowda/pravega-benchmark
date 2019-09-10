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

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Class for Kafka writer/producer.
 */
public class KafkaWriterWorker extends WriterWorker {
    final private KafkaProducer<byte[], byte[]> producer;

    KafkaWriterWorker(int sensorId, int events, int flushEvents,
                      int secondsToRun, boolean isRandomKey, int messageSize,
                      long start, PerfStats stats, String streamName,
                      int eventsPerSec, boolean writeAndRead, Properties producerProps) {

        super(sensorId, events, flushEvents,
                secondsToRun, isRandomKey, messageSize,
                start, stats, streamName, eventsPerSec, writeAndRead);

        this.producer = new KafkaProducer<>(producerProps);
    }

    public long recordWrite(byte[] data, TriConsumer record) {
        final long time = System.currentTimeMillis();
        Callback cb = new PerfCallback(time, data.length, record);
        producer.send(new ProducerRecord<>(streamName, data), cb);
        return time;
    }

    private static final class PerfCallback implements Callback {
        private final long start;
        private final int bytes;
        private final TriConsumer record;

        public PerfCallback(long start, int bytes, TriConsumer record) {
            this.start = start;
            this.bytes = bytes;
            this.record = record;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            final long endTime = System.currentTimeMillis();
            record.accept(start, endTime, bytes);
        }
    }

    @Override
    public void writeData(byte[] data) {
        producer.send(new ProducerRecord<>(streamName, data));
    }


    @Override
    public void flush() {
        producer.flush();
    }

    @Override
    public synchronized void close() {
        producer.close();
    }
}