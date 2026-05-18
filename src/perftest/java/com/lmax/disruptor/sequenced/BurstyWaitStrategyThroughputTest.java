/*
 * Copyright 2011 LMAX Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lmax.disruptor.sequenced;

import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BatchEventProcessorBuilder;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.PhasedBackoffWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.sun.jna.LastErrorException;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Structure;
import com.sun.jna.FunctionMapper;
import tddy.TddyWaitStrategy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bursty wait-strategy throughput test.
 *
 * <p>The saturated Disruptor throughput tests mostly measure publish/consume hot paths.  This test deliberately
 * alternates producer bursts and idle gaps, and can pin donor threads to the consumer CPU, so wait strategies spend
 * measurable time in their waiting paths before the consumer has to wake up and drain the next burst.
 */
public final class BurstyWaitStrategyThroughputTest
{
    private static final int DEFAULT_RUNS = 7;
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 64;
    private static final int DEFAULT_BURST_SIZE = 256;
    private static final int DEFAULT_BURSTS = 100_000;
    private static final long DEFAULT_PAUSE_NANOS = 100_000L;
    private static final long DEFAULT_CONSUMER_WORK_NANOS = 0L;
    private static final int DEFAULT_PRODUCERS = 1;
    private static final int DEFAULT_DONOR_THREADS = 0;
    private static final String DEFAULT_STRATEGY = "yielding";
    private static final String DEFAULT_PRODUCER_CPUS = "";
    private static final String DEFAULT_DONOR_CPUS = "";
    private static final int NO_CPU = -1;

    private static volatile long blackhole;

    private final Config config;

    private BurstyWaitStrategyThroughputTest(final Config config)
    {
        this.config = config;
    }

    public static void main(final String[] args) throws Exception
    {
        Config config = Config.fromProperties();
        BurstyWaitStrategyThroughputTest test = new BurstyWaitStrategyThroughputTest(config);

        System.out.println(config.toHeader());
        for (int i = 0; i < config.runs; i++)
        {
            System.gc();
            PassResult result = test.runPass(i);
            System.out.println(result.toLine(config));
        }
    }

    private PassResult runPass(final int run) throws Exception
    {
        final WaitStrategy waitStrategy = newWaitStrategy(config);
        final RingBuffer<BurstyEvent> ringBuffer = config.producers == 1
            ? RingBuffer.createSingleProducer(BurstyEvent::new, config.bufferSize, waitStrategy)
            : RingBuffer.createMultiProducer(BurstyEvent::new, config.bufferSize, waitStrategy);
        final SequenceBarrier sequenceBarrier = ringBuffer.newBarrier();
        final CountDownLatch completionLatch = new CountDownLatch(1);
        final BurstRecordingEventHandler handler = new BurstRecordingEventHandler(config, completionLatch);
        final BatchEventProcessor<BurstyEvent> processor =
            new BatchEventProcessorBuilder().build(ringBuffer, sequenceBarrier, handler);
        final CyclicBarrier startBarrier = new CyclicBarrier(config.producers + 1);
        final AtomicBoolean donorsRunning = new AtomicBoolean(true);

        ringBuffer.addGatingSequences(processor.getSequence());

        ExecutorService consumerExecutor = Executors.newSingleThreadExecutor(
            new PinnedThreadFactory("bursty-consumer", singleCpu(config.consumerCpu)));
        ExecutorService producerExecutor = Executors.newFixedThreadPool(
            config.producers, new PinnedThreadFactory("bursty-producer", config.producerCpus));
        ExecutorService donorExecutor = config.donorThreads == 0
            ? null
            : Executors.newFixedThreadPool(
                config.donorThreads, new PinnedThreadFactory("bursty-donor", config.donorCpus));

        List<Future<?>> donorFutures = new ArrayList<>();
        if (donorExecutor != null)
        {
            for (int i = 0; i < config.donorThreads; i++)
            {
                donorFutures.add(donorExecutor.submit(new Donor(donorsRunning)));
            }
        }

        List<Future<?>> producerFutures = new ArrayList<>();
        for (int i = 0; i < config.producers; i++)
        {
            producerFutures.add(producerExecutor.submit(new Producer(i, ringBuffer, startBarrier, handler.burstStartTimes)));
        }

        Future<?> consumerFuture = consumerExecutor.submit(processor);
        long startedAt = System.nanoTime();
        startBarrier.await();

        for (Future<?> future : producerFutures)
        {
            future.get();
        }

        completionLatch.await();
        long completedAt = System.nanoTime();

        processor.halt();
        consumerFuture.get();
        donorsRunning.set(false);

        shutdown(producerExecutor);
        shutdown(consumerExecutor);
        if (donorExecutor != null)
        {
            shutdown(donorExecutor);
            for (Future<?> future : donorFutures)
            {
                future.get();
            }
        }

        return handler.toResult(run, completedAt - startedAt);
    }

    private static int[] singleCpu(final int cpu)
    {
        if (cpu < 0)
        {
            return new int[0];
        }
        return new int[]{cpu};
    }

    private static void shutdown(final ExecutorService executor) throws InterruptedException
    {
        executor.shutdown();
        if (!executor.awaitTermination(5, TimeUnit.SECONDS))
        {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static WaitStrategy newWaitStrategy(final Config config)
    {
        final WaitStrategy waitStrategy;
        switch (config.waitStrategy)
        {
            case "busyspin":
                waitStrategy = new BusySpinWaitStrategy();
                break;
            case "yielding":
                waitStrategy = new YieldingWaitStrategy();
                break;
            case "sleeping":
                waitStrategy = new SleepingWaitStrategy();
                break;
            case "blocking":
                waitStrategy = new BlockingWaitStrategy();
                break;
            case "liteblocking":
                waitStrategy = new LiteBlockingWaitStrategy();
                break;
            case "phasedbackoff":
                waitStrategy = PhasedBackoffWaitStrategy.withLiteLock(10, 100, TimeUnit.MICROSECONDS);
                break;
            case "tddy":
                waitStrategy = new TddyWaitStrategy(config.tddySpinTries);
                break;
            default:
                throw new IllegalArgumentException("Unknown waitStrategy: " + config.waitStrategy);
        }
        return waitStrategy;
    }

    private static void spinForNanos(final long nanos)
    {
        if (nanos <= 0)
        {
            return;
        }

        long deadline = System.nanoTime() + nanos;
        while (System.nanoTime() < deadline)
        {
            Thread.onSpinWait();
        }
    }

    private static void burnForNanos(final long nanos)
    {
        if (nanos <= 0)
        {
            return;
        }

        long value = blackhole;
        long deadline = System.nanoTime() + nanos;
        while (System.nanoTime() < deadline)
        {
            value = (value * 31) + 17;
        }
        blackhole = value;
    }

    public static final class BurstyEvent
    {
        private int burstSlot;
        private int indexInBurst;

        public int getBurstSlot()
        {
            return burstSlot;
        }

        public void setBurstSlot(final int burstSlot)
        {
            this.burstSlot = burstSlot;
        }

        public int getIndexInBurst()
        {
            return indexInBurst;
        }

        public void setIndexInBurst(final int indexInBurst)
        {
            this.indexInBurst = indexInBurst;
        }
    }

    private final class Producer implements Runnable
    {
        private final int producerIndex;
        private final RingBuffer<BurstyEvent> ringBuffer;
        private final CyclicBarrier startBarrier;
        private final long[] burstStartTimes;

        Producer(
            final int producerIndex,
            final RingBuffer<BurstyEvent> ringBuffer,
            final CyclicBarrier startBarrier,
            final long[] burstStartTimes)
        {
            this.producerIndex = producerIndex;
            this.ringBuffer = ringBuffer;
            this.startBarrier = startBarrier;
            this.burstStartTimes = burstStartTimes;
        }

        @Override
        public void run()
        {
            try
            {
                startBarrier.await();
                for (int burst = 0; burst < config.bursts; burst++)
                {
                    int burstSlot = producerIndex * config.bursts + burst;
                    burstStartTimes[burstSlot] = System.nanoTime();
                    for (int i = 0; i < config.burstSize; i++)
                    {
                        long sequence = ringBuffer.next();
                        try
                        {
                            BurstyEvent event = ringBuffer.get(sequence);
                            event.setBurstSlot(burstSlot);
                            event.setIndexInBurst(i);
                        }
                        finally
                        {
                            ringBuffer.publish(sequence);
                        }
                    }
                    spinForNanos(config.pauseNanos);
                }
            }
            catch (Exception ex)
            {
                throw new RuntimeException(ex);
            }
        }
    }

    private static final class Donor implements Runnable
    {
        private final AtomicBoolean running;

        Donor(final AtomicBoolean running)
        {
            this.running = running;
        }

        @Override
        public void run()
        {
            long value = blackhole;
            while (running.get())
            {
                value = (value * 31) + 17;
                if ((value & 1023L) == 0)
                {
                    Thread.onSpinWait();
                }
            }
            blackhole = value;
        }
    }

    private static final class BurstRecordingEventHandler implements com.lmax.disruptor.EventHandler<BurstyEvent>
    {
        private final Config config;
        private final CountDownLatch completionLatch;
        private final long totalEvents;
        private final long[] burstStartTimes;
        private final long[] burstDrainTimes;
        private long batchesProcessed;

        BurstRecordingEventHandler(final Config config, final CountDownLatch completionLatch)
        {
            this.config = config;
            this.completionLatch = completionLatch;
            this.totalEvents = config.totalEvents();
            this.burstStartTimes = new long[config.totalBursts()];
            this.burstDrainTimes = new long[config.totalBursts()];
        }

        @Override
        public void onEvent(final BurstyEvent event, final long sequence, final boolean endOfBatch)
        {
            burnForNanos(config.consumerWorkNanos);
            if (event.getIndexInBurst() == config.burstSize - 1)
            {
                burstDrainTimes[event.getBurstSlot()] = System.nanoTime() - burstStartTimes[event.getBurstSlot()];
            }
            if (sequence == totalEvents - 1)
            {
                completionLatch.countDown();
            }
        }

        @Override
        public void onBatchStart(final long batchSize, final long queueDepth)
        {
            batchesProcessed++;
        }

        PassResult toResult(final int run, final long wallNanos)
        {
            long[] sortedDrainTimes = burstDrainTimes.clone();
            Arrays.sort(sortedDrainTimes);

            long drainTotal = 0;
            for (long drainTime : burstDrainTimes)
            {
                drainTotal += drainTime;
            }

            return new PassResult(
                run,
                wallNanos,
                totalEvents,
                batchesProcessed,
                percentile(sortedDrainTimes, 50),
                percentile(sortedDrainTimes, 95),
                percentile(sortedDrainTimes, 99),
                drainTotal);
        }
    }

    private static long percentile(final long[] sortedValues, final int percentile)
    {
        if (sortedValues.length == 0)
        {
            return 0;
        }

        int index = (int) Math.ceil((percentile / 100.0d) * sortedValues.length) - 1;
        return sortedValues[Math.max(0, Math.min(sortedValues.length - 1, index))];
    }

    private static final class PassResult
    {
        private final int run;
        private final long wallNanos;
        private final long totalEvents;
        private final long batchesProcessed;
        private final long drainP50Nanos;
        private final long drainP95Nanos;
        private final long drainP99Nanos;
        private final long drainTotalNanos;

        PassResult(
            final int run,
            final long wallNanos,
            final long totalEvents,
            final long batchesProcessed,
            final long drainP50Nanos,
            final long drainP95Nanos,
            final long drainP99Nanos,
            final long drainTotalNanos)
        {
            this.run = run;
            this.wallNanos = wallNanos;
            this.totalEvents = totalEvents;
            this.batchesProcessed = batchesProcessed;
            this.drainP50Nanos = drainP50Nanos;
            this.drainP95Nanos = drainP95Nanos;
            this.drainP99Nanos = drainP99Nanos;
            this.drainTotalNanos = drainTotalNanos;
        }

        String toLine(final Config config)
        {
            return String.format(
                Locale.ROOT,
                "result wait_strategy=%s producers=%d burst_size=%d bursts=%d pause_ns=%d " +
                    "consumer_work_ns=%d donor_threads=%d run=%d ops_sec=%d active_drain_ops_sec=%d " +
                    "batch_percent=%.2f avg_batch_size=%d drain_p50_ns=%d drain_p95_ns=%d drain_p99_ns=%d",
                config.waitStrategy,
                config.producers,
                config.burstSize,
                config.bursts,
                config.pauseNanos,
                config.consumerWorkNanos,
                config.donorThreads,
                run,
                opsPerSecond(totalEvents, wallNanos),
                opsPerSecond(totalEvents, drainTotalNanos),
                batchPercent(),
                averageBatchSize(),
                drainP50Nanos,
                drainP95Nanos,
                drainP99Nanos);
        }

        private double batchPercent()
        {
            if (batchesProcessed == 0)
            {
                return 0.0d;
            }
            return (1.0d - ((double) batchesProcessed / totalEvents)) * 100.0d;
        }

        private long averageBatchSize()
        {
            if (batchesProcessed == 0)
            {
                return -1L;
            }
            return totalEvents / batchesProcessed;
        }
    }

    private static long opsPerSecond(final long events, final long nanos)
    {
        if (nanos <= 0)
        {
            return 0L;
        }
        return (long) ((events * 1_000_000_000.0d) / nanos);
    }

    private static final class Config
    {
        private final int runs;
        private final String waitStrategy;
        private final int bufferSize;
        private final int burstSize;
        private final int bursts;
        private final long pauseNanos;
        private final long consumerWorkNanos;
        private final int producers;
        private final int consumerCpu;
        private final int[] producerCpus;
        private final int[] donorCpus;
        private final int donorThreads;
        private final int tddySpinTries;

        private Config(
            final int runs,
            final String waitStrategy,
            final int bufferSize,
            final int burstSize,
            final int bursts,
            final long pauseNanos,
            final long consumerWorkNanos,
            final int producers,
            final int consumerCpu,
            final int[] producerCpus,
            final int[] donorCpus,
            final int donorThreads,
            final int tddySpinTries)
        {
            this.runs = runs;
            this.waitStrategy = waitStrategy;
            this.bufferSize = bufferSize;
            this.burstSize = burstSize;
            this.bursts = bursts;
            this.pauseNanos = pauseNanos;
            this.consumerWorkNanos = consumerWorkNanos;
            this.producers = producers;
            this.consumerCpu = consumerCpu;
            this.producerCpus = producerCpus;
            this.donorCpus = donorCpus;
            this.donorThreads = donorThreads;
            this.tddySpinTries = tddySpinTries;
        }

        static Config fromProperties()
        {
            int producers = intProperty("producers", DEFAULT_PRODUCERS);
            int consumerCpu = intProperty("consumerCpu", NO_CPU);
            int[] producerCpus = cpuListProperty("producerCpus", DEFAULT_PRODUCER_CPUS);
            int[] donorCpus = cpuListProperty("donorCpus", DEFAULT_DONOR_CPUS);

            return new Config(
                intProperty("runs", DEFAULT_RUNS),
                System.getProperty("waitStrategy", DEFAULT_STRATEGY).toLowerCase(Locale.ROOT),
                intProperty("bufferSize", DEFAULT_BUFFER_SIZE),
                intProperty("burstSize", DEFAULT_BURST_SIZE),
                intProperty("bursts", DEFAULT_BURSTS),
                longProperty("pauseNs", DEFAULT_PAUSE_NANOS),
                longProperty("consumerWorkNs", DEFAULT_CONSUMER_WORK_NANOS),
                producers,
                consumerCpu,
                producerCpus,
                donorCpus,
                intProperty("donorThreads", DEFAULT_DONOR_THREADS),
                intProperty("tddySpinTries", 100));
        }

        long totalEvents()
        {
            return (long) producers * bursts * burstSize;
        }

        int totalBursts()
        {
            return producers * bursts;
        }

        String toHeader()
        {
            return String.format(
                Locale.ROOT,
                "config wait_strategy=%s runs=%d producers=%d buffer_size=%d burst_size=%d bursts=%d " +
                    "pause_ns=%d consumer_work_ns=%d consumer_cpu=%d producer_cpus=%s donor_cpus=%s " +
                    "donor_threads=%d tddy_spin_tries=%d",
                waitStrategy,
                runs,
                producers,
                bufferSize,
                burstSize,
                bursts,
                pauseNanos,
                consumerWorkNanos,
                consumerCpu,
                Arrays.toString(producerCpus),
                Arrays.toString(donorCpus),
                donorThreads,
                tddySpinTries);
        }
    }

    private static int intProperty(final String name, final int defaultValue)
    {
        String value = System.getProperty(name);
        if (value == null || value.isEmpty())
        {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }

    private static long longProperty(final String name, final long defaultValue)
    {
        String value = System.getProperty(name);
        if (value == null || value.isEmpty())
        {
            return defaultValue;
        }
        return Long.parseLong(value);
    }

    private static int[] cpuListProperty(final String name, final String defaultValue)
    {
        String value = System.getProperty(name, defaultValue);
        if (value == null || value.trim().isEmpty())
        {
            return new int[0];
        }

        String[] parts = value.split(",");
        int[] cpus = new int[parts.length];
        for (int i = 0; i < parts.length; i++)
        {
            cpus[i] = Integer.parseInt(parts[i].trim());
        }
        return cpus;
    }

    private static final class PinnedThreadFactory implements ThreadFactory
    {
        private final String namePrefix;
        private final int[] cpus;
        private final AtomicInteger counter = new AtomicInteger();

        PinnedThreadFactory(final String namePrefix, final int[] cpus)
        {
            this.namePrefix = namePrefix;
            this.cpus = cpus.clone();
        }

        @Override
        public Thread newThread(final Runnable runnable)
        {
            int threadIndex = counter.getAndIncrement();
            int cpu = cpus.length == 0 ? NO_CPU : cpus[threadIndex % cpus.length];
            Thread thread = new Thread(() ->
            {
                if (cpu >= 0)
                {
                    LinuxAffinity.bindToCpu(cpu);
                }
                runnable.run();
            });
            thread.setName(namePrefix + "-" + threadIndex + (cpu >= 0 ? "-cpu-" + cpu : ""));
            thread.setDaemon(true);
            return thread;
        }
    }

    private static final class LinuxAffinity
    {
        private static final LibC LIBC = Native.load("c", LibC.class, functionMapperOptions());
        private static final int LONG_BITS = Long.SIZE;
        private static final int CPU_SET_LONGS = 16;

        private LinuxAffinity()
        {
        }

        static void bindToCpu(final int cpu)
        {
            CpuSet cpuSet = new CpuSet();
            cpuSet.set(cpu);
            int rc = LIBC.schedSetAffinity(0, cpuSet.size(), cpuSet);
            if (rc != 0)
            {
                throw new IllegalStateException("sched_setaffinity cpu=" + cpu + " failed errno=" + Native.getLastError());
            }
        }

        private static Map<String, Object> functionMapperOptions()
        {
            Map<String, Object> options = new HashMap<>();
            options.put(Library.OPTION_FUNCTION_MAPPER, (FunctionMapper) (library, method) ->
                "schedSetAffinity".equals(method.getName()) ? "sched_setaffinity" : method.getName());
            return options;
        }

        public interface LibC extends Library
        {
            int schedSetAffinity(int pid, int cpusetsize, CpuSet mask) throws LastErrorException;
        }

        public static final class CpuSet extends Structure
        {
            public long[] bits = new long[CPU_SET_LONGS];

            void set(final int cpu)
            {
                if (cpu < 0 || cpu >= CPU_SET_LONGS * LONG_BITS)
                {
                    throw new IllegalArgumentException("cpu out of supported range: " + cpu);
                }
                bits[cpu / LONG_BITS] |= 1L << (cpu % LONG_BITS);
            }

            @Override
            protected List<String> getFieldOrder()
            {
                return Collections.singletonList("bits");
            }
        }
    }
}
