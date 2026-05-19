package tddy;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.WaitStrategy;

/**
 * Tddy strategy that initially spins, then uses native tddy yields for
 * {@link com.lmax.disruptor.EventProcessor}s
 * waiting on a barrier.
 *
 * <p>
 * This strategy enables native speedup while work is available and disables it
 * while the consumer is idle. Tddy state
 * is kept thread-local because a {@link WaitStrategy} instance may be shared by
 * multiple consumers.
 */
public final class TddyWaitStrategy implements WaitStrategy {
    private static final int DEFAULT_SPIN_TRIES = 100;

    private final int spinTries;

    /**
     * Native tddy initialization state for the current thread.
     */
    private final ThreadLocal<Boolean> initialized = ThreadLocal.withInitial(() -> false);

    /**
     * Native tddy speedup state for the current thread.
     */
    // private final ThreadLocal<Boolean> speedupEnabled =
    // ThreadLocal.withInitial(() -> false);

    /**
     * Provides a tddy wait strategy with the default spin and yield settings.
     */
    public TddyWaitStrategy() {
        this(DEFAULT_SPIN_TRIES);
    }

    /**
     * @param spinTries how many times the strategy should spin before using
     *                  native tddy yields
     */
    public TddyWaitStrategy(final int spinTries) {
        if (spinTries < 0) {
            throw new IllegalArgumentException("spinTries must be >= 0");
        }
        this.spinTries = spinTries;
    }

    @Override
    public long waitFor(
            final long sequence, final Sequence cursor, final Sequence dependentSequence, final SequenceBarrier barrier)
            throws AlertException, InterruptedException {
        initOnce();

        long availableSequence;
        int counter = spinTries;
        long idleCounter = 0;

        while ((availableSequence = dependentSequence.get()) < sequence) {
            final int newCounter = applyWaitMethod(barrier, counter, idleCounter);
            if (0 == counter) {
                idleCounter++;
            }
            counter = newCounter;
        }

        TddyNative.INSTANCE.tddy_java_speedup(1);
        // speedupOnIfNeeded();

        return availableSequence;
    }

    @Override
    public void signalAllWhenBlocking() {
    }

    private int applyWaitMethod(final SequenceBarrier barrier, final int counter, final long idleCounter)
            throws AlertException {
        barrier.checkAlert();
        // speedupOffIfNeeded();

        if (counter > 0) {
            Thread.onSpinWait();
            return counter - 1;
        } else {
            TddyNative.INSTANCE.tddy_java_yield_default();
        }

        return counter;
    }

    private void initOnce() {
        if (!initialized.get()) {
            int rc = TddyNative.INSTANCE.tddy_java_init();
            if (rc != 0) {
                throw new IllegalStateException(
                        "tddy_java_init failed: is scx_tddy running?");
            }
            initialized.set(true);
        }
    }

    // private void speedupOnIfNeeded() {
    // if (!speedupEnabled.get()) {
    // TddyNative.INSTANCE.tddy_java_speedup(1);
    // speedupEnabled.set(true);
    // }
    // }

    // private void speedupOffIfNeeded() {
    // if (speedupEnabled.get()) {
    // TddyNative.INSTANCE.tddy_java_speedup(0);
    // speedupEnabled.set(false);
    // }
    // }
}
