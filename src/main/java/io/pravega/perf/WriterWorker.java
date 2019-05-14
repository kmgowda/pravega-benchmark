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

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * Abstract class for Writers.
 */
public abstract class WriterWorker extends Worker implements Callable<Void> {
    final private static int MS_PER_SEC = 1000;
    final private Performance perf;
    final private String payload;
    final private int eventsPerSec;
    final private int flushEvents;
    final private boolean writeAndRead;

    WriterWorker(int sensorId, int events, int flushEvents, int secondsToRun,
                 boolean isRandomKey, int messageSize, long start,
                 PerfStats stats, String streamName, int eventsPerSec, boolean writeAndRead) {

        super(sensorId, events, secondsToRun, messageSize, start, stats, streamName, 0);
        this.eventsPerSec = eventsPerSec;
        this.flushEvents = flushEvents;
        this.writeAndRead = writeAndRead;
        perf = createBenchmark();
        payload = createPayload(messageSize);
    }


    private String createPayload(int size) {
        Random random = new Random();
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; ++i) {
            bytes[i] = (byte) (random.nextInt(26) + 65);
        }
        return new String(bytes, StandardCharsets.US_ASCII);
    }


    private Performance createBenchmark() {
        final Performance perfWriter;
        if (secondsToRun > 0) {
            if (writeAndRead) {
                perfWriter = new EventsWriterTimeRW();
            } else {
                if (eventsPerSec > 0 || flushEvents < Integer.MAX_VALUE) {
                    perfWriter = new EventsWriterTimeSleep();
                } else {
                    perfWriter = new EventsWriterTime();
                }
            }
        } else {
            if (writeAndRead) {
                perfWriter = new EventsWriterRW();
            } else {
                if (eventsPerSec > 0 || flushEvents < Integer.MAX_VALUE) {
                    perfWriter = new EventsWriterSleep();
                } else {
                    perfWriter = new EventsWriter();
                }
            }
        }
        return perfWriter;
    }


    /**
     * Writes the data and benchmark.
     *
     * @param data   data to write
     * @param record to call for benchmarking
     * @return time return the data sent time
     */
    public abstract long recordWrite(String data, TriConsumer record);

    /**
     * Writes the data and benchmark.
     *
     * @param data data to write
     */
    public abstract void writeData(String data);

    /**
     * Flush the producer data.
     */
    public abstract void flush();

    /**
     * Flush the producer data.
     */
    public abstract void close();


    @Override
    public Void call() throws InterruptedException, ExecutionException, IOException {
        perf.benchmark();
        return null;
    }


    private class EventsWriter implements Performance {

        public void benchmark() throws InterruptedException, IOException {
            for (int i = 0; i < events; i++) {
                recordWrite(payload, stats::recordTime);
            }
            flush();
        }
    }


    private class EventsWriterSleep implements Performance {

        public void benchmark() throws InterruptedException, IOException {
            final EventsController eCnt = new EventsController(System.currentTimeMillis(), eventsPerSec);
            int cnt = 0;
            while (cnt < events) {
                int loopMax = Math.min(flushEvents, events - cnt);
                for (int i = 0; i < loopMax; i++) {
                    eCnt.control(cnt++, recordWrite(payload, stats::recordTime));
                }
                flush();
            }
        }
    }


    private class EventsWriterTime implements Performance {

        public void benchmark() throws InterruptedException, IOException {
            final long msToRun = secondsToRun * MS_PER_SEC;
            long time = System.currentTimeMillis();
            while ((time - startTime) < msToRun) {
                time = recordWrite(payload, stats::recordTime);
            }
            flush();
        }
    }


    private class EventsWriterTimeSleep implements Performance {

        public void benchmark() throws InterruptedException, IOException {
            final long msToRun = secondsToRun * MS_PER_SEC;
            long time = System.currentTimeMillis();
            final EventsController eCnt = new EventsController(time, eventsPerSec);
            long msElapsed = time - startTime;
            int cnt = 0;
            while (msElapsed < msToRun) {
                for (int i = 0; (msElapsed < msToRun) && (i < flushEvents); i++) {
                    time = recordWrite(payload, stats::recordTime);
                    eCnt.control(cnt++, time);
                    msElapsed = time - startTime;
                }
                flush();
            }
        }
    }


    private class EventsWriterRW implements Performance {

        public void benchmark() throws InterruptedException, IOException {
            final long time = System.currentTimeMillis();
            final String timeHeader = String.format(TIME_HEADER_FORMAT, time);
            final EventsController eCnt = new EventsController(time, eventsPerSec);
            final StringBuilder buffer = new StringBuilder(timeHeader + ", " + workerID + ", " + payload);
            buffer.setLength(messageSize);
            for (int i = 0; i < events; i++) {
                final String header = String.format(TIME_HEADER_FORMAT, System.currentTimeMillis());
                final String data = buffer.replace(0, TIME_HEADER_SIZE, header).toString();
                writeData(data);
                /*
                flush is required here for following reasons:
                1. The writeData is called for End to End latency mode; hence make sure that data is sent.
                2. In case of kafka benchmarking, the buffering makes too many writes;
                   flushing moderates the kafka producer.
                3. If the flush called after several iterations, then flush may take too much of time.
                */
                flush();
                eCnt.control(i);
            }
        }
    }


    private class EventsWriterTimeRW implements Performance {

        public void benchmark() throws InterruptedException, IOException {
            final long msToRun = secondsToRun * MS_PER_SEC;
            long time = System.currentTimeMillis();
            final String timeHeader = String.format(TIME_HEADER_FORMAT, time);
            final EventsController eCnt = new EventsController(time, eventsPerSec);
            final StringBuilder buffer = new StringBuilder(timeHeader + ", " + workerID + ", " + payload);
            buffer.setLength(messageSize);
            for (int i = 0; (time - startTime) < msToRun; i++) {
                time = System.currentTimeMillis();
                final String header = String.format(TIME_HEADER_FORMAT, time);
                final String data = buffer.replace(0, TIME_HEADER_SIZE, header).toString();
                writeData(data);
                /*
                flush is required here for following reasons:
                1. The writeData is called for End to End latency mode; hence make sure that data is sent.
                2. In case of kafka benchmarking, the buffering makes too many writes;
                   flushing moderates the kafka producer.
                3. If the flush called after several iterations, then flush may take too much of time.
                */
                flush();
                eCnt.control(i);
            }
        }
    }

    @NotThreadSafe
    final static private class EventsController {
        private static final long NS_PER_MS = 1000000L;
        private static final long NS_PER_SEC = 1000 * NS_PER_MS;
        private static final long MIN_SLEEP_NS = 2 * NS_PER_MS;
        private final long startTime;
        private final long sleepTimeNs;
        private final int eventsPerSec;
        private long toSleepNs = 0;

        /**
         * @param eventsPerSec events per second
         */
        private EventsController(long start, int eventsPerSec) {
            this.startTime = start;
            this.eventsPerSec = eventsPerSec;
            this.sleepTimeNs = this.eventsPerSec > 0 ?
                    NS_PER_SEC / this.eventsPerSec : 0;
        }

        /**
         * Blocks for small amounts of time to achieve targetThroughput/events per sec
         *
         * @param events current events
         */
        void control(long events) {
            if (this.eventsPerSec <= 0) {
                return;
            }
            needSleep(events, System.currentTimeMillis());
        }

        /**
         * Blocks for small amounts of time to achieve targetThroughput/events per sec
         *
         * @param events current events
         * @param time   current time
         */
        void control(long events, long time) {
            if (this.eventsPerSec <= 0) {
                return;
            }
            needSleep(events, time);
        }

        private void needSleep(long events, long time) {
            float elapsedSec = (time - startTime) / 1000.f;

            if ((events / elapsedSec) < this.eventsPerSec) {
                return;
            }

            // control throughput / number of events by sleeping, on average,
            toSleepNs += sleepTimeNs;
            // If threshold reached, sleep a little
            if (toSleepNs >= MIN_SLEEP_NS) {
                long sleepStart = System.nanoTime();
                try {
                    final long sleepMs = toSleepNs / NS_PER_MS;
                    final long sleepNs = toSleepNs - sleepMs * NS_PER_MS;
                    Thread.sleep(sleepMs, (int) sleepNs);
                } catch (InterruptedException e) {
                    // will be taken care in finally block
                } finally {
                    // in case of short sleeps or oversleep ;adjust it for next sleep duration
                    final long sleptNS = System.nanoTime() - sleepStart;
                    if (sleptNS > 0) {
                        toSleepNs -= sleptNS;
                    } else {
                        toSleepNs = 0;
                    }
                }
            }
        }
    }
}
