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
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.PhasedBackoffWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.sun.jna.FunctionMapper;
import com.sun.jna.LastErrorException;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Structure;
import tddy.TddyNative;
import tddy.TddyWaitStrategy;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
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
import java.util.concurrent.locks.LockSupport;

/**
 * Deadline-aware consumer policy benchmark.
 *
 * <p>
 * The producer emits mostly relaxed work with loose deadlines and a small
 * number
 * of critical ON-period bursts with tight deadlines. The adaptive policy yields
 * during relaxed chunks when deadline slack allows it, then asks TDDY for
 * speed-up around critical work.
 */
public final class DeadlineAwareConsumerPolicyBenchmark {
    private static final int DEFAULT_RUNS = 7;
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 64;
    private static final int DEFAULT_TOTAL_EVENTS = 50_000;
    private static final int DEFAULT_CRITICAL_RATIO_PERCENT = 5;
    private static final int DEFAULT_CRITICAL_BURST_SIZE = 16;
    private static final long DEFAULT_RELAXED_INTER_ARRIVAL_NANOS = TimeUnit.MICROSECONDS.toNanos(10);
    private static final long DEFAULT_CRITICAL_DEADLINE_NANOS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final long DEFAULT_RELAXED_DEADLINE_NANOS = TimeUnit.MILLISECONDS.toNanos(50);
    private static final long DEFAULT_CRITICAL_WORK_NANOS = TimeUnit.MICROSECONDS.toNanos(50);
    private static final long DEFAULT_RELAXED_WORK_NANOS = TimeUnit.MICROSECONDS.toNanos(50);
    private static final long DEFAULT_CRITICAL_POST_BURST_PAUSE_NANOS = 0L;
    private static final int DEFAULT_RELAXED_CHUNK_ITERATIONS = 500;
    private static final long DEFAULT_RELAXED_YIELD_SLACK_NANOS = TimeUnit.MILLISECONDS.toNanos(2);
    private static final long DEFAULT_RELAXED_YIELD_GUARD_NANOS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final long DEFAULT_RELAXED_YIELD_NANOS = TimeUnit.MICROSECONDS.toNanos(100);
    private static final long DEFAULT_TDDY_ESTIMATED_SLICE_NANOS = TimeUnit.MILLISECONDS.toNanos(5);
    private static final int DEFAULT_DONOR_THREADS = 0;
    private static final String DEFAULT_POLICY = "plain";
    private static final String DEFAULT_STRATEGY = "blocking";
    private static final String DEFAULT_SCHEDULER = "unknown";
    private static final String DEFAULT_PRODUCER_CPUS = "";
    private static final String DEFAULT_DONOR_CPUS = "";
    private static final int NO_CPU = -1;
    private static final long PAUSE_SPIN_THRESHOLD_NANOS = TimeUnit.MICROSECONDS.toNanos(50);
    private static final long DONOR_COUNTER_SAMPLE_MASK = 1023L;
    private static final int CPU_TIME_POLL_ITERATIONS = 256;
    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

    private static final int TYPE_RELAXED = 0;
    private static final int TYPE_CRITICAL = 1;

    private static volatile long blackhole;

    private final Config config;

    private DeadlineAwareConsumerPolicyBenchmark(final Config config) {
        this.config = config;
    }

    public static void main(final String[] args) throws Exception {
        ensureThreadCpuTimeAvailable();
        Config config = Config.fromProperties();
        DeadlineAwareConsumerPolicyBenchmark benchmark = new DeadlineAwareConsumerPolicyBenchmark(config);

        System.out.println(config.toHeader());
        for (int i = 0; i < config.runs; i++) {
            System.gc();
            PassResult result = benchmark.runPass(i);
            System.out.println(result.toLine(config));
        }
    }

    private PassResult runPass(final int run) throws Exception {
        final WaitStrategy waitStrategy = newWaitStrategy(config);
        final RingBuffer<DeadlineEvent> ringBuffer = RingBuffer.createSingleProducer(DeadlineEvent::new,
                config.bufferSize, waitStrategy);
        final SequenceBarrier sequenceBarrier = ringBuffer.newBarrier();
        final CountDownLatch completionLatch = new CountDownLatch(1);
        final DeadlineAwareEventHandler handler = new DeadlineAwareEventHandler(config, completionLatch);
        final BatchEventProcessor<DeadlineEvent> processor = new BatchEventProcessorBuilder().build(
                ringBuffer, sequenceBarrier, handler);
        final CyclicBarrier startBarrier = new CyclicBarrier(2);
        final AtomicBoolean donorsRunning = new AtomicBoolean(true);

        ringBuffer.addGatingSequences(processor.getSequence());

        ExecutorService consumerExecutor = Executors.newSingleThreadExecutor(
                new PinnedThreadFactory("deadline-consumer", singleCpu(config.consumerCpu)));
        ExecutorService producerExecutor = Executors.newSingleThreadExecutor(
                new PinnedThreadFactory("deadline-producer", config.producerCpus));
        ExecutorService donorExecutor = config.donorThreads == 0 ? null
                : Executors.newFixedThreadPool(
                        config.donorThreads, new PinnedThreadFactory("deadline-donor", config.donorCpus));

        Donor[] donors = new Donor[config.donorThreads];
        Future<?>[] donorFutures = new Future<?>[config.donorThreads];
        if (donorExecutor != null) {
            for (int i = 0; i < config.donorThreads; i++) {
                donors[i] = new Donor(donorsRunning);
                donorFutures[i] = donorExecutor.submit(donors[i]);
            }
        }

        Future<?> consumerFuture = consumerExecutor.submit(processor);
        handler.awaitStarted();
        if (handler.startupFailure() != null) {
            Throwable startupFailure = handler.startupFailure();
            processor.halt();
            consumerFuture.get();
            shutdown(producerExecutor);
            shutdown(consumerExecutor);
            if (donorExecutor != null) {
                donorsRunning.set(false);
                shutdown(donorExecutor);
            }
            throw asException(startupFailure);
        }

        Future<?> producerFuture = producerExecutor.submit(new Producer(ringBuffer, startBarrier));

        long startedAt = System.nanoTime();
        long donorStartedOperations = donorOperations(donors);
        startBarrier.await();

        producerFuture.get();
        completionLatch.await();

        long completedAt = System.nanoTime();
        long donorCompletedOperations = donorOperations(donors);
        donorsRunning.set(false);

        processor.halt();
        consumerFuture.get();

        shutdown(producerExecutor);
        shutdown(consumerExecutor);
        if (donorExecutor != null) {
            shutdown(donorExecutor);
            for (Future<?> future : donorFutures) {
                future.get();
            }
        }

        return handler.toResult(
                run,
                completedAt - startedAt,
                Math.max(0L, donorCompletedOperations - donorStartedOperations));
    }

    private static long donorOperations(final Donor[] donors) {
        long operations = 0;
        for (Donor donor : donors) {
            if (donor != null) {
                operations += donor.operations();
            }
        }
        return operations;
    }

    private static Exception asException(final Throwable failure) {
        if (failure instanceof Exception) {
            return (Exception) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        return new RuntimeException(failure);
    }

    private static int[] singleCpu(final int cpu) {
        if (cpu < 0) {
            return new int[0];
        }
        return new int[] { cpu };
    }

    private static void shutdown(final ExecutorService executor) throws InterruptedException {
        executor.shutdown();
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static WaitStrategy newWaitStrategy(final Config config) {
        final WaitStrategy waitStrategy;
        switch (config.waitStrategy) {
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
                waitStrategy = PhasedBackoffWaitStrategy.withSleep(10, 100, TimeUnit.MICROSECONDS);
                break;
            case "tddy":
                waitStrategy = new TddyWaitStrategy();
                break;
            default:
                throw new IllegalArgumentException("Unknown waitStrategy: " + config.waitStrategy);
        }
        return waitStrategy;
    }

    private static void pauseForNanos(final long nanos) {
        if (nanos <= 0) {
            return;
        }

        long deadline = System.nanoTime() + nanos;
        long remaining;
        while ((remaining = deadline - System.nanoTime()) > PAUSE_SPIN_THRESHOLD_NANOS) {
            LockSupport.parkNanos(remaining - PAUSE_SPIN_THRESHOLD_NANOS);
        }

        while (System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
    }

    private static void ensureThreadCpuTimeAvailable() {
        if (!THREAD_MX_BEAN.isCurrentThreadCpuTimeSupported()) {
            throw new IllegalStateException("Current thread CPU time is not supported by this JVM");
        }
        if (THREAD_MX_BEAN.isThreadCpuTimeSupported() && !THREAD_MX_BEAN.isThreadCpuTimeEnabled()) {
            THREAD_MX_BEAN.setThreadCpuTimeEnabled(true);
        }
        if (currentThreadCpuTimeNanos() < 0) {
            throw new IllegalStateException("Current thread CPU time is disabled by this JVM");
        }
    }

    private static long currentThreadCpuTimeNanos() {
        long cpuTimeNanos = THREAD_MX_BEAN.getCurrentThreadCpuTime();
        if (cpuTimeNanos < 0) {
            throw new IllegalStateException("Current thread CPU time is disabled by this JVM");
        }
        return cpuTimeNanos;
    }

    private static void burnCpuTime(final long targetNanos, final long seed) {
        if (targetNanos <= 0) {
            return;
        }

        long value = blackhole ^ seed;
        long startedAtCpu = currentThreadCpuTimeNanos();
        do {
            for (int i = 0; i < CPU_TIME_POLL_ITERATIONS; i++) {
                value ^= value << 13;
                value ^= value >>> 7;
                value ^= value << 17;
            }
            Thread.onSpinWait();
        } while (currentThreadCpuTimeNanos() - startedAtCpu < targetNanos);
        blackhole = value;
    }

    public static final class DeadlineEvent {
        private int type;
        private int sampleIndex;
        private long workNanos;
        private long publishedAtNanos;
        private long deadlineAtNanos;

        void set(
                final int type,
                final int sampleIndex,
                final long workNanos,
                final long publishedAtNanos,
                final long deadlineAtNanos) {
            this.type = type;
            this.sampleIndex = sampleIndex;
            this.workNanos = workNanos;
            this.publishedAtNanos = publishedAtNanos;
            this.deadlineAtNanos = deadlineAtNanos;
        }

        int type() {
            return type;
        }

        int sampleIndex() {
            return sampleIndex;
        }

        long workNanos() {
            return workNanos;
        }

        long publishedAtNanos() {
            return publishedAtNanos;
        }

        long deadlineAtNanos() {
            return deadlineAtNanos;
        }
    }

    private final class Producer implements Runnable {
        private final RingBuffer<DeadlineEvent> ringBuffer;
        private final CyclicBarrier startBarrier;

        Producer(final RingBuffer<DeadlineEvent> ringBuffer, final CyclicBarrier startBarrier) {
            this.ringBuffer = ringBuffer;
            this.startBarrier = startBarrier;
        }

        @Override
        public void run() {
            try {
                startBarrier.await();

                int relaxedIndex = 0;
                int criticalIndex = 0;
                while (relaxedIndex < config.relaxedEvents || criticalIndex < config.criticalEvents) {
                    int relaxedWindow = Math.min(
                            config.relaxedEvents - relaxedIndex, config.relaxedEventsPerOffWindow());
                    for (int i = 0; i < relaxedWindow; i++) {
                        publish(TYPE_RELAXED, relaxedIndex, config.relaxedWorkNanos, config.relaxedDeadlineNanos);
                        relaxedIndex++;
                        pauseForNanos(config.relaxedInterArrivalNanos);
                    }

                    int criticalBurst = Math.min(config.criticalEvents - criticalIndex, config.criticalBurstSize);
                    for (int i = 0; i < criticalBurst; i++) {
                        publish(
                                TYPE_CRITICAL,
                                criticalIndex,
                                config.criticalWorkNanos,
                                config.criticalDeadlineNanos);
                        criticalIndex++;
                    }
                    if (criticalBurst > 0 && relaxedIndex < config.relaxedEvents) {
                        pauseForNanos(config.criticalPostBurstPauseNanos);
                    }
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        private void publish(
                final int type,
                final int sampleIndex,
                final long workNanos,
                final long deadlineNanos) {
            long sequence = ringBuffer.next();
            try {
                long now = System.nanoTime();
                ringBuffer.get(sequence).set(type, sampleIndex, workNanos, now, now + deadlineNanos);
            } finally {
                ringBuffer.publish(sequence);
            }
        }
    }

    private static final class Donor implements Runnable {
        private final AtomicBoolean running;
        private volatile long operations;

        Donor(final AtomicBoolean running) {
            this.running = running;
        }

        long operations() {
            return operations;
        }

        @Override
        public void run() {
            long value = blackhole;
            long localOperations = operations;
            while (running.get()) {
                value = (value * 31) + 17;
                localOperations++;
                if ((localOperations & DONOR_COUNTER_SAMPLE_MASK) == 0) {
                    operations = localOperations;
                    Thread.onSpinWait();
                }
            }
            operations = localOperations;
            blackhole = value;
        }
    }

    private static final class DeadlineAwareEventHandler implements EventHandler<DeadlineEvent> {
        private final Config config;
        private final CountDownLatch completionLatch;
        private final CountDownLatch startedLatch = new CountDownLatch(1);
        private final long[] criticalLatencies;
        private final long[] relaxedLatencies;
        private Throwable startupFailure;
        private boolean speedupEnabled;
        private long criticalMisses;
        private long relaxedMisses;
        private long criticalLatencyTotalNanos;
        private long relaxedLatencyTotalNanos;
        private long relaxedYieldCalls;
        private long relaxedYieldedNanos;
        private long speedupCalls;

        DeadlineAwareEventHandler(final Config config, final CountDownLatch completionLatch) {
            this.config = config;
            this.completionLatch = completionLatch;
            this.criticalLatencies = new long[config.criticalEvents];
            this.relaxedLatencies = new long[config.relaxedEvents];
        }

        @Override
        public void onStart() {
            try {
                if (config.adaptivePolicy()) {
                    int rc = TddyNative.INSTANCE.tddy_java_init();
                    if (rc != 0) {
                        throw new IllegalStateException("tddy_java_init failed: is scx_tddy running?");
                    }
                }
            } catch (Throwable ex) {
                startupFailure = ex;
                if (ex instanceof RuntimeException) {
                    throw (RuntimeException) ex;
                }
                if (ex instanceof Error) {
                    throw (Error) ex;
                }
                throw new RuntimeException(ex);
            } finally {
                startedLatch.countDown();
            }
        }

        @Override
        public void onEvent(final DeadlineEvent event, final long sequence, final boolean endOfBatch) {
            if (event.type() == TYPE_CRITICAL) {
                processCritical(event, sequence);
            } else {
                processRelaxed(event, sequence);
            }

            long completedAt = System.nanoTime();
            long latency = completedAt - event.publishedAtNanos();
            if (event.type() == TYPE_CRITICAL) {
                criticalLatencies[event.sampleIndex()] = latency;
                criticalLatencyTotalNanos += latency;
                if (completedAt > event.deadlineAtNanos()) {
                    criticalMisses++;
                }
            } else {
                relaxedLatencies[event.sampleIndex()] = latency;
                relaxedLatencyTotalNanos += latency;
                if (completedAt > event.deadlineAtNanos()) {
                    relaxedMisses++;
                }
            }

            if (sequence == config.totalEvents - 1L) {
                completionLatch.countDown();
            }
        }

        @Override
        public void onShutdown() {
            if (config.adaptivePolicy()) {
                if (speedupEnabled) {
                    setSpeedup(false);
                }
                TddyNative.INSTANCE.tddy_java_cleanup();
            }
        }

        void awaitStarted() throws InterruptedException {
            if (!startedLatch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("consumer did not start within 5 seconds");
            }
        }

        Throwable startupFailure() {
            return startupFailure;
        }

        PassResult toResult(final int run, final long wallNanos, final long donorOperations) {
            long[] sortedCritical = criticalLatencies.clone();
            long[] sortedRelaxed = relaxedLatencies.clone();
            Arrays.sort(sortedCritical);
            Arrays.sort(sortedRelaxed);

            return new PassResult(
                    run,
                    wallNanos,
                    donorOperations,
                    criticalMisses,
                    relaxedMisses,
                    criticalLatencyTotalNanos,
                    relaxedLatencyTotalNanos,
                    percentile(sortedCritical, 50),
                    percentile(sortedCritical, 95),
                    percentile(sortedCritical, 99),
                    percentile(sortedRelaxed, 50),
                    percentile(sortedRelaxed, 95),
                    percentile(sortedRelaxed, 99),
                    relaxedYieldCalls,
                    relaxedYieldedNanos,
                    speedupCalls);
        }

        private void processCritical(final DeadlineEvent event, final long sequence) {
            if (config.adaptivePolicy()) {
                setSpeedup(true);
            }
            burnCpuTime(event.workNanos(), sequence);
            // if (config.adaptivePolicy()) {
            // setSpeedup(false);
            // }
        }

        private void processRelaxed(final DeadlineEvent event, final long sequence) {
            if (!config.adaptivePolicy()) {
                burnCpuTime(event.workNanos(), sequence);
                return;
            }

            long firstHalf = event.workNanos() / 2;
            long secondHalf = event.workNanos() - firstHalf;
            burnCpuTime(firstHalf, sequence);
            maybeYieldRelaxed(event);
            burnCpuTime(secondHalf, sequence + firstHalf);
        }

        private void maybeYieldRelaxed(final DeadlineEvent event) {
            // long slack = event.deadlineAtNanos() - System.nanoTime();
            // if (slack <= config.relaxedYieldSlackNanos) {
            // return;
            // }

            // long slackAfterGuardAndSlice = slack - config.relaxedYieldGuardNanos -
            // config.tddyEstimatedSliceNanos;
            // if (slackAfterGuardAndSlice < 0) {
            // return;
            // }

            // long extraYieldNanos = Math.min(config.relaxedYieldNanos,
            // slackAfterGuardAndSlice);
            relaxedYieldCalls++;
            setSpeedup(false);
            long issued = TddyNative.INSTANCE.tddy_java_yield_ns(config.relaxedYieldNanos);
            if (issued > 0) {
                relaxedYieldedNanos += issued;
            }
        }

        private void setSpeedup(final boolean enabled) {
            if (speedupEnabled == enabled) {
                return;
            }
            TddyNative.INSTANCE.tddy_java_speedup(enabled ? 1 : 0);
            speedupCalls++;
            speedupEnabled = enabled;
        }
    }

    private static long percentile(final long[] sortedValues, final int percentile) {
        if (sortedValues.length == 0) {
            return 0L;
        }

        int index = (int) Math.ceil((percentile / 100.0d) * sortedValues.length) - 1;
        return sortedValues[Math.max(0, Math.min(sortedValues.length - 1, index))];
    }

    private static final class PassResult {
        private final int run;
        private final long wallNanos;
        private final long donorOperations;
        private final long criticalMisses;
        private final long relaxedMisses;
        private final long criticalLatencyTotalNanos;
        private final long relaxedLatencyTotalNanos;
        private final long criticalP50Nanos;
        private final long criticalP95Nanos;
        private final long criticalP99Nanos;
        private final long relaxedP50Nanos;
        private final long relaxedP95Nanos;
        private final long relaxedP99Nanos;
        private final long relaxedYieldCalls;
        private final long relaxedYieldedNanos;
        private final long speedupCalls;

        PassResult(
                final int run,
                final long wallNanos,
                final long donorOperations,
                final long criticalMisses,
                final long relaxedMisses,
                final long criticalLatencyTotalNanos,
                final long relaxedLatencyTotalNanos,
                final long criticalP50Nanos,
                final long criticalP95Nanos,
                final long criticalP99Nanos,
                final long relaxedP50Nanos,
                final long relaxedP95Nanos,
                final long relaxedP99Nanos,
                final long relaxedYieldCalls,
                final long relaxedYieldedNanos,
                final long speedupCalls) {
            this.run = run;
            this.wallNanos = wallNanos;
            this.donorOperations = donorOperations;
            this.criticalMisses = criticalMisses;
            this.relaxedMisses = relaxedMisses;
            this.criticalLatencyTotalNanos = criticalLatencyTotalNanos;
            this.relaxedLatencyTotalNanos = relaxedLatencyTotalNanos;
            this.criticalP50Nanos = criticalP50Nanos;
            this.criticalP95Nanos = criticalP95Nanos;
            this.criticalP99Nanos = criticalP99Nanos;
            this.relaxedP50Nanos = relaxedP50Nanos;
            this.relaxedP95Nanos = relaxedP95Nanos;
            this.relaxedP99Nanos = relaxedP99Nanos;
            this.relaxedYieldCalls = relaxedYieldCalls;
            this.relaxedYieldedNanos = relaxedYieldedNanos;
            this.speedupCalls = speedupCalls;
        }

        String toLine(final Config config) {
            return String.format(
                    Locale.ROOT,
                    "result scheduler=%s policy=%s wait_strategy=%s run=%d total_events=%d " +
                            "critical_ratio_percent=%d critical_events=%d relaxed_events=%d buffer_size=%d " +
                            "critical_burst_size=%d critical_post_burst_pause_ns=%d " +
                            "relaxed_inter_arrival_ns=%d critical_deadline_ns=%d " +
                            "relaxed_deadline_ns=%d critical_work_ns=%d relaxed_work_ns=%d " +
                            "relaxed_chunk_iterations=%d relaxed_yield_slack_ns=%d relaxed_yield_guard_ns=%d " +
                            "relaxed_yield_ns=%d tddy_estimated_slice_ns=%d donor_threads=%d wall_ns=%d ops_sec=%d donor_ops_sec=%d "
                            +
                            "donor_ops_sec_per_thread=%d critical_count=%d critical_miss_ratio=%.6f " +
                            "critical_p50_ns=%d critical_p95_ns=%d critical_p99_ns=%d critical_mean_ns=%d relaxed_count=%d "
                            +
                            "relaxed_miss_ratio=%.6f relaxed_p50_ns=%d relaxed_p95_ns=%d relaxed_p99_ns=%d " +
                            "relaxed_mean_ns=%d relaxed_yield_calls=%d relaxed_yielded_ns=%d speedup_calls=%d",
                    config.scheduler,
                    config.policy,
                    config.waitStrategy,
                    run,
                    config.totalEvents,
                    config.criticalRatioPercent,
                    config.criticalEvents,
                    config.relaxedEvents,
                    config.bufferSize,
                    config.criticalBurstSize,
                    config.criticalPostBurstPauseNanos,
                    config.relaxedInterArrivalNanos,
                    config.criticalDeadlineNanos,
                    config.relaxedDeadlineNanos,
                    config.criticalWorkNanos,
                    config.relaxedWorkNanos,
                    config.relaxedChunkIterations,
                    config.relaxedYieldSlackNanos,
                    config.relaxedYieldGuardNanos,
                    config.relaxedYieldNanos,
                    config.tddyEstimatedSliceNanos,
                    config.donorThreads,
                    wallNanos,
                    opsPerSecond(config.totalEvents, wallNanos),
                    donorOpsPerSecond(),
                    donorOpsPerSecondPerThread(config),
                    config.criticalEvents,
                    missRatio(criticalMisses, config.criticalEvents),
                    criticalP50Nanos,
                    criticalP95Nanos,
                    criticalP99Nanos,
                    criticalMeanNanos(config),
                    config.relaxedEvents,
                    missRatio(relaxedMisses, config.relaxedEvents),
                    relaxedP50Nanos,
                    relaxedP95Nanos,
                    relaxedP99Nanos,
                    relaxedMeanNanos(config),
                    relaxedYieldCalls,
                    relaxedYieldedNanos,
                    speedupCalls);
        }

        private long relaxedMeanNanos(final Config config) {
            if (config.relaxedEvents == 0) {
                return 0L;
            }
            return relaxedLatencyTotalNanos / config.relaxedEvents;
        }

        private long criticalMeanNanos(final Config config) {
            if (config.criticalEvents == 0) {
                return 0L;
            }
            return criticalLatencyTotalNanos / config.criticalEvents;
        }

        private long donorOpsPerSecond() {
            return opsPerSecond(donorOperations, wallNanos);
        }

        private long donorOpsPerSecondPerThread(final Config config) {
            if (config.donorThreads == 0) {
                return 0L;
            }
            return donorOpsPerSecond() / config.donorThreads;
        }
    }

    private static double missRatio(final long misses, final int count) {
        if (count == 0) {
            return 0.0d;
        }
        return (double) misses / count;
    }

    private static long opsPerSecond(final long events, final long nanos) {
        if (nanos <= 0) {
            return 0L;
        }
        return (long) ((events * 1_000_000_000.0d) / nanos);
    }

    private static final class Config {
        private final int runs;
        private final String scheduler;
        private final String policy;
        private final String waitStrategy;
        private final int bufferSize;
        private final int totalEvents;
        private final int criticalRatioPercent;
        private final int criticalEvents;
        private final int relaxedEvents;
        private final int criticalBurstSize;
        private final long criticalPostBurstPauseNanos;
        private final long relaxedInterArrivalNanos;
        private final long criticalDeadlineNanos;
        private final long relaxedDeadlineNanos;
        private final long criticalWorkNanos;
        private final long relaxedWorkNanos;
        private final int relaxedChunkIterations;
        private final long relaxedYieldSlackNanos;
        private final long relaxedYieldGuardNanos;
        private final long relaxedYieldNanos;
        private final long tddyEstimatedSliceNanos;
        private final int consumerCpu;
        private final int[] producerCpus;
        private final int[] donorCpus;
        private final int donorThreads;

        private Config(
                final int runs,
                final String scheduler,
                final String policy,
                final String waitStrategy,
                final int bufferSize,
                final int totalEvents,
                final int criticalRatioPercent,
                final int criticalBurstSize,
                final long criticalPostBurstPauseNanos,
                final long relaxedInterArrivalNanos,
                final long criticalDeadlineNanos,
                final long relaxedDeadlineNanos,
                final long criticalWorkNanos,
                final long relaxedWorkNanos,
                final int relaxedChunkIterations,
                final long relaxedYieldSlackNanos,
                final long relaxedYieldGuardNanos,
                final long relaxedYieldNanos,
                final long tddyEstimatedSliceNanos,
                final int consumerCpu,
                final int[] producerCpus,
                final int[] donorCpus,
                final int donorThreads) {
            this.runs = runs;
            this.scheduler = scheduler;
            this.policy = policy;
            this.waitStrategy = waitStrategy;
            this.bufferSize = requirePositive("bufferSize", bufferSize);
            this.totalEvents = requirePositive("totalEvents", totalEvents);
            this.criticalRatioPercent = requirePercent("criticalRatioPercent", criticalRatioPercent);
            this.criticalEvents = (int) Math.round(totalEvents * (criticalRatioPercent / 100.0d));
            this.relaxedEvents = totalEvents - criticalEvents;
            this.criticalBurstSize = requirePositive("criticalBurstSize", criticalBurstSize);
            this.criticalPostBurstPauseNanos =
                    requireNonNegative("criticalPostBurstPauseNs", criticalPostBurstPauseNanos);
            this.relaxedInterArrivalNanos = requireNonNegative("relaxedInterArrivalNs", relaxedInterArrivalNanos);
            this.criticalDeadlineNanos = requirePositive("criticalDeadlineNs", criticalDeadlineNanos);
            this.relaxedDeadlineNanos = requirePositive("relaxedDeadlineNs", relaxedDeadlineNanos);
            this.criticalWorkNanos = requireNonNegative("criticalWorkNs", criticalWorkNanos);
            this.relaxedWorkNanos = requireNonNegative("relaxedWorkNs", relaxedWorkNanos);
            this.relaxedChunkIterations = requirePositive("relaxedChunkIterations", relaxedChunkIterations);
            this.relaxedYieldSlackNanos = requireNonNegative("relaxedYieldSlackNs", relaxedYieldSlackNanos);
            this.relaxedYieldGuardNanos = requireNonNegative("relaxedYieldGuardNs", relaxedYieldGuardNanos);
            this.relaxedYieldNanos = requireNonNegative("relaxedYieldNs", relaxedYieldNanos);
            this.tddyEstimatedSliceNanos = requireNonNegative("tddyEstimatedSliceNs", tddyEstimatedSliceNanos);
            this.consumerCpu = consumerCpu;
            this.producerCpus = producerCpus;
            this.donorCpus = donorCpus;
            this.donorThreads = requireNonNegative("donorThreads", donorThreads);

            if (!"plain".equals(policy) && !"tddy_adaptive".equals(policy)) {
                throw new IllegalArgumentException("Unknown policy: " + policy);
            }
        }

        static Config fromProperties() {
            return new Config(
                    intProperty("runs", DEFAULT_RUNS),
                    System.getProperty("scheduler", DEFAULT_SCHEDULER).toLowerCase(Locale.ROOT),
                    policyProperty(),
                    System.getProperty("waitStrategy", DEFAULT_STRATEGY).toLowerCase(Locale.ROOT),
                    intProperty("bufferSize", DEFAULT_BUFFER_SIZE),
                    intProperty("totalEvents", DEFAULT_TOTAL_EVENTS),
                    intProperty("criticalRatioPercent", DEFAULT_CRITICAL_RATIO_PERCENT),
                    intProperty("criticalBurstSize", DEFAULT_CRITICAL_BURST_SIZE),
                    longProperty("criticalPostBurstPauseNs", DEFAULT_CRITICAL_POST_BURST_PAUSE_NANOS),
                    longProperty("relaxedInterArrivalNs", DEFAULT_RELAXED_INTER_ARRIVAL_NANOS),
                    longProperty("criticalDeadlineNs", DEFAULT_CRITICAL_DEADLINE_NANOS),
                    longProperty("relaxedDeadlineNs", DEFAULT_RELAXED_DEADLINE_NANOS),
                    longProperty("criticalWorkNs", DEFAULT_CRITICAL_WORK_NANOS),
                    longProperty("relaxedWorkNs", DEFAULT_RELAXED_WORK_NANOS),
                    intProperty("relaxedChunkIterations", DEFAULT_RELAXED_CHUNK_ITERATIONS),
                    longProperty("relaxedYieldSlackNs", DEFAULT_RELAXED_YIELD_SLACK_NANOS),
                    longProperty("relaxedYieldGuardNs", DEFAULT_RELAXED_YIELD_GUARD_NANOS),
                    longProperty("relaxedYieldNs", DEFAULT_RELAXED_YIELD_NANOS),
                    longProperty("tddyEstimatedSliceNs", DEFAULT_TDDY_ESTIMATED_SLICE_NANOS),
                    intProperty("consumerCpu", NO_CPU),
                    cpuListProperty("producerCpus", DEFAULT_PRODUCER_CPUS),
                    cpuListProperty("donorCpus", DEFAULT_DONOR_CPUS),
                    intProperty("donorThreads", DEFAULT_DONOR_THREADS));
        }

        boolean adaptivePolicy() {
            return "tddy_adaptive".equals(policy);
        }

        int relaxedEventsPerOffWindow() {
            if (relaxedEvents == 0) {
                return 0;
            }
            if (criticalEvents == 0) {
                return relaxedEvents;
            }

            int criticalBursts = (criticalEvents + criticalBurstSize - 1) / criticalBurstSize;
            return Math.max(1, (relaxedEvents + criticalBursts - 1) / criticalBursts);
        }

        String toHeader() {
            return String.format(
                    Locale.ROOT,
                    "config scheduler=%s policy=%s wait_strategy=%s runs=%d total_events=%d " +
                            "critical_ratio_percent=%d critical_events=%d relaxed_events=%d buffer_size=%d " +
                            "critical_burst_size=%d critical_post_burst_pause_ns=%d " +
                            "relaxed_inter_arrival_ns=%d critical_deadline_ns=%d " +
                            "relaxed_deadline_ns=%d critical_work_ns=%d relaxed_work_ns=%d " +
                            "relaxed_chunk_iterations=%d relaxed_yield_slack_ns=%d relaxed_yield_guard_ns=%d " +
                            "relaxed_yield_ns=%d tddy_estimated_slice_ns=%d consumer_cpu=%d producer_cpus=%s donor_cpus=%s donor_threads=%d",
                    scheduler,
                    policy,
                    waitStrategy,
                    runs,
                    totalEvents,
                    criticalRatioPercent,
                    criticalEvents,
                    relaxedEvents,
                    bufferSize,
                    criticalBurstSize,
                    criticalPostBurstPauseNanos,
                    relaxedInterArrivalNanos,
                    criticalDeadlineNanos,
                    relaxedDeadlineNanos,
                    criticalWorkNanos,
                    relaxedWorkNanos,
                    relaxedChunkIterations,
                    relaxedYieldSlackNanos,
                    relaxedYieldGuardNanos,
                    relaxedYieldNanos,
                    tddyEstimatedSliceNanos,
                    consumerCpu,
                    Arrays.toString(producerCpus),
                    Arrays.toString(donorCpus),
                    donorThreads);
        }
    }

    private static String policyProperty() {
        String policy = System.getProperty("policy");
        if (policy == null || policy.isEmpty()) {
            policy = System.getProperty("consumerPolicy", DEFAULT_POLICY);
        }
        return policy.toLowerCase(Locale.ROOT);
    }

    private static int intProperty(final String name, final int defaultValue) {
        String value = System.getProperty(name);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }

    private static long longProperty(final String name, final long defaultValue) {
        String value = System.getProperty(name);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        return Long.parseLong(value);
    }

    private static int[] cpuListProperty(final String name, final String defaultValue) {
        String value = System.getProperty(name, defaultValue);
        if (value == null || value.trim().isEmpty()) {
            return new int[0];
        }

        String[] parts = value.split(",");
        int[] cpus = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            cpus[i] = Integer.parseInt(parts[i].trim());
        }
        return cpus;
    }

    private static int requirePositive(final String name, final int value) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be > 0: " + value);
        }
        return value;
    }

    private static long requirePositive(final String name, final long value) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be > 0: " + value);
        }
        return value;
    }

    private static int requireNonNegative(final String name, final int value) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be >= 0: " + value);
        }
        return value;
    }

    private static long requireNonNegative(final String name, final long value) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be >= 0: " + value);
        }
        return value;
    }

    private static int requirePercent(final String name, final int value) {
        if (value < 0 || value > 100) {
            throw new IllegalArgumentException(name + " must be in [0, 100]: " + value);
        }
        return value;
    }

    private static final class PinnedThreadFactory implements ThreadFactory {
        private final String namePrefix;
        private final int[] cpus;
        private final AtomicInteger counter = new AtomicInteger();

        PinnedThreadFactory(final String namePrefix, final int[] cpus) {
            this.namePrefix = namePrefix;
            this.cpus = cpus.clone();
        }

        @Override
        public Thread newThread(final Runnable runnable) {
            int threadIndex = counter.getAndIncrement();
            int cpu = cpus.length == 0 ? NO_CPU : cpus[threadIndex % cpus.length];
            Thread thread = new Thread(() -> {
                if (cpu >= 0) {
                    LinuxAffinity.bindToCpu(cpu);
                }
                runnable.run();
            });
            thread.setName(namePrefix + "-" + threadIndex + (cpu >= 0 ? "-cpu-" + cpu : ""));
            thread.setDaemon(true);
            return thread;
        }
    }

    private static final class LinuxAffinity {
        private static final LibC LIBC = Native.load("c", LibC.class, functionMapperOptions());
        private static final int LONG_BITS = Long.SIZE;
        private static final int CPU_SET_LONGS = 16;

        private LinuxAffinity() {
        }

        static void bindToCpu(final int cpu) {
            CpuSet cpuSet = new CpuSet();
            cpuSet.set(cpu);
            int rc = LIBC.schedSetAffinity(0, cpuSet.size(), cpuSet);
            if (rc != 0) {
                throw new IllegalStateException(
                        "sched_setaffinity cpu=" + cpu + " failed errno=" + Native.getLastError());
            }
        }

        private static Map<String, Object> functionMapperOptions() {
            Map<String, Object> options = new HashMap<>();
            options.put(Library.OPTION_FUNCTION_MAPPER, (FunctionMapper) (library,
                    method) -> "schedSetAffinity".equals(method.getName()) ? "sched_setaffinity" : method.getName());
            return options;
        }

        public interface LibC extends Library {
            int schedSetAffinity(int pid, int cpusetsize, CpuSet mask) throws LastErrorException;
        }

        public static final class CpuSet extends Structure {
            public long[] bits = new long[CPU_SET_LONGS];

            void set(final int cpu) {
                if (cpu < 0 || cpu >= CPU_SET_LONGS * LONG_BITS) {
                    throw new IllegalArgumentException("cpu out of supported range: " + cpu);
                }
                bits[cpu / LONG_BITS] |= 1L << (cpu % LONG_BITS);
            }

            @Override
            protected List<String> getFieldOrder() {
                return Collections.singletonList("bits");
            }
        }
    }
}
