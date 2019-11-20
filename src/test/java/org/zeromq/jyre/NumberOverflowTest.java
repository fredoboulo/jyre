package org.zeromq.jyre;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class NumberOverflowTest
{
    @Test
    public void testOverflowByte()
    {
        byte test = (byte) 0xff;
        assertThat(test + 1, is(0));
    }

    @Test
    public void testOverflowShort()
    {
        short test = (short) 0xffff;
        assertThat(test + 1, is(0));
    }

    @Test
    public void testUnderflowShort()
    {
        short test = (short) 0;
        assertThat(test - 1, is(-1));
    }

    @Test
    public void range()
    {
        int c = 56815;
        assertThat(c >= 0xc000, is(true));
        assertThat(c <= 0xffff, is(true));
        short s = (short) c;
        assertThat((0xffff & s) > 0, is(true));
    }
}
