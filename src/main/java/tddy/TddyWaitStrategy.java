package tddy;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.WaitStrategy;

public final class TddyWaitStrategy implements WaitStrategy
{
    private static final int DEFAULT_SPIN_TRIES = 100;
    private static final int DEFAULT_TDDY_YIELD_EVERY = 1;

    private final int spinTries;
    private final int tddyYieldEvery;

    /*
     * WaitStrategy instance may be shared by multiple consumers
     * so speedup status should be thread-local.
     */
    private final ThreadLocal<Boolean> initialized =
            ThreadLocal.withInitial(() -> false);

    private final ThreadLocal<Boolean> speedupEnabled =
            ThreadLocal.withInitial(() -> false);

    public TddyWaitStrategy()
    {
        this(DEFAULT_SPIN_TRIES, DEFAULT_TDDY_YIELD_EVERY);
    }

    public TddyWaitStrategy(final int spinTries, final int tddyYieldEvery)
    {
        if (spinTries < 0)
        {
            throw new IllegalArgumentException("spinTries must be >= 0");
        }
        if (tddyYieldEvery <= 0)
        {
            throw new IllegalArgumentException("tddyYieldEvery must be > 0");
        }

        this.spinTries = spinTries;
        this.tddyYieldEvery = tddyYieldEvery;
    }

    @Override
    public long waitFor(
        final long sequence,
        final Sequence cursor,
        final Sequence dependentSequence,
        final SequenceBarrier barrier)
        throws AlertException, InterruptedException, TimeoutException
    {

        initOnce();

        long availableSequence;
        int counter = spinTries;
        long idleIters = 0;

        while ((availableSequence = dependentSequence.get()) < sequence)
        {
            barrier.checkAlert();

            /*
             * turn off speedup upon entering waiting/idle phase to prevent consumers
             * from redeeming credit while having nothing to do.
             */
            speedupOffIfNeeded();

            if (counter > 0)
            {
                counter--;
                Thread.onSpinWait();
            }
            else
            {
                if ((idleIters++ % tddyYieldEvery) == 0)
                {
                    TddyNative.INSTANCE.tddy_java_yield_default();
                }
                else
                {
                    Thread.onSpinWait();
                }
            }
        }

        /*
         * We have an event so speed up.
         * BatchEventProcessor will take over.
         */
        speedupOnIfNeeded();

        return availableSequence;
    }

    @Override
    public void signalAllWhenBlocking()
    {
    }

    private void initOnce()
    {
        if (!initialized.get())
        {
            int rc = TddyNative.INSTANCE.tddy_java_init();
            if (rc != 0)
            {
                throw new IllegalStateException(
                        "tddy_java_init failed: is scx_tddy running?");
            }
            initialized.set(true);
        }
    }

    private void speedupOnIfNeeded()
    {
        if (!speedupEnabled.get())
        {
            TddyNative.INSTANCE.tddy_java_speedup(1);
            speedupEnabled.set(true);
        }
    }

    private void speedupOffIfNeeded()
    {
        if (speedupEnabled.get())
        {
            TddyNative.INSTANCE.tddy_java_speedup(0);
            speedupEnabled.set(false);
        }
    }
}
