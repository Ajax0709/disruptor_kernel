package com.lmax.disruptor;

import org.junit.jupiter.api.Test;
import tddy.TddyNative;

// import static org.junit.jupiter.api.Assertions.assertEquals;

public class TddyNativeTest
{
    @Test
    public void shouldInitTddy()
    {
        int ret = TddyNative.INSTANCE.tddy_java_init();
        // assertEquals(0, ret);
        TddyNative.INSTANCE.tddy_java_speedup(0);
        TddyNative.INSTANCE.tddy_java_yield_default();
        TddyNative.INSTANCE.tddy_java_speedup(1);
        TddyNative.INSTANCE.tddy_java_speedup(0);
    }
}