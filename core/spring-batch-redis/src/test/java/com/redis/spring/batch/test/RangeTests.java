package com.redis.spring.batch.test;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.spring.batch.util.Range;

class RangeTests {

    @Test
    void parse() {
        assertRange(Range.of(":100"), 0, 100);
        assertRange(Range.of("0:"), 0, Range.MAX_VALUE);
        assertRange(Range.of("0:*"), 0, Range.MAX_VALUE);
        assertRange(Range.of(":*"), 0, Range.MAX_VALUE);
        assertRange(Range.of(":"), Range.MIN_VALUE, Range.MAX_VALUE);
        assertRange(Range.of("100"), 100, 100);
        assertRange(Range.of("100"), 100, 100);
        assertRange(Range.of("1234567890:1234567890"), 1234567890, 1234567890);
    }

    private void assertRange(Range range, int min, int max) {
        Assertions.assertEquals(min, range.getMin());
        Assertions.assertEquals(max, range.getMax());
    }

}
