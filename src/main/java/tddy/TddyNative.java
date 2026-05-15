package tddy;

import com.sun.jna.Library;
import com.sun.jna.Native;

/**
 * Native tddy bindings.
 */
public interface TddyNative extends Library
{
    /**
     * Loaded tddy native library.
     */
    TddyNative INSTANCE = Native.load("/home/ajaxli/project/fair-os-scheduling/bin/libtddy_jna.so", TddyNative.class);

    /**
     * Initializes tddy support for the current thread.
     *
     * @return zero on success
     */
    int tddy_java_init();

    /**
     * Yields using the native default duration.
     *
     * @return native yield result
     */
    long tddy_java_yield_default();

    /**
     * Yields for the supplied native duration.
     *
     * @param deltaNs duration in nanoseconds
     * @return native yield result
     */
    long tddy_java_yield_ns(long deltaNs);

    /**
     * Enables or disables native speedup.
     *
     * @param enable non-zero to enable speedup
     * @return native speedup result
     */
    long tddy_java_speedup(int enable);

    /**
     * Synchronizes native credit.
     *
     * @return native sync result
     */
    long tddy_java_sync_credit();

    /**
     * Gets the current native thread id.
     *
     * @return native thread id
     */
    int tddy_java_gettid();

    /**
     * Cleans up tddy support for the current thread.
     */
    void tddy_java_cleanup();
}
