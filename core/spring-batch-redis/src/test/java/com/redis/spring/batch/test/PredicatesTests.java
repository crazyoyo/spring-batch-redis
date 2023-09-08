package com.redis.spring.batch.test;

import static com.redis.spring.batch.util.Predicates.and;
import static com.redis.spring.batch.util.Predicates.glob;
import static com.redis.spring.batch.util.Predicates.isFalse;
import static com.redis.spring.batch.util.Predicates.isTrue;
import static com.redis.spring.batch.util.Predicates.or;
import static com.redis.spring.batch.util.Predicates.slots;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

import com.redis.spring.batch.util.LongRange;

class PredicatesTests {

    @SuppressWarnings("unchecked")
    @Test
    void testOr() {
        assertTrue(or().test(null));
        assertFalse(or(isFalse()).test(null));
        assertTrue(or(isTrue()).test(null));
        assertTrue(or(isFalse(), isTrue()).test(null));
        assertFalse(or(isFalse(), isFalse()).test(null));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testAnd() {
        assertTrue(and().test(null));
        assertFalse(and(isFalse()).test(null));
        assertTrue(and(isTrue()).test(null));
        assertTrue(and(isTrue(), isTrue()).test(null));
        assertFalse(and(isFalse(), isTrue()).test(null));
    }

    @Test
    void include() {
        Predicate<String> foo = glob("foo");
        assertTrue(foo.test("foo"));
        assertFalse(foo.test("bar"));
        Predicate<String> fooStar = glob("foo*");
        assertTrue(fooStar.test("foobar"));
        assertFalse(fooStar.test("barfoo"));
    }

    @Test
    void exclude() {
        Predicate<String> foo = glob("foo").negate();
        assertFalse(foo.test("foo"));
        assertTrue(foo.test("foa"));
        Predicate<String> fooStar = glob("foo*").negate();
        assertFalse(fooStar.test("foobar"));
        assertTrue(fooStar.test("barfoo"));
    }

    @Test
    void includeAndExclude() {
        Predicate<String> foo1 = glob("foo1").and(glob("foo").negate());
        assertFalse(foo1.test("foo"));
        assertFalse(foo1.test("bar"));
        assertTrue(foo1.test("foo1"));
        Predicate<String> foo1Star = glob("foo").and(glob("foo1*").negate());
        assertTrue(foo1Star.test("foo"));
        assertFalse(foo1Star.test("bar"));
        assertFalse(foo1Star.test("foo1"));
    }

    @Test
    void slotExact() {
        Predicate<String> predicate = slots(LongRange.is(7638));
        assertTrue(predicate.test("abc"));
        assertFalse(predicate.test("abcd"));
    }

    @Test
    void slotRange() {
        Predicate<String> unbounded = slots(LongRange.unbounded());
        assertTrue(unbounded.test("foo"));
        assertTrue(unbounded.test("foo1"));
        Predicate<String> is999999 = slots(LongRange.is(999999));
        assertFalse(is999999.test("foo"));
    }

    @Test
    void kitchenSink() {
        Predicate<String> predicate = glob("foo").negate().and(glob("foo1")).and(slots(LongRange.unbounded()));
        assertFalse(predicate.test("foo"));
        assertFalse(predicate.test("bar"));
        assertTrue(predicate.test("foo1"));
    }

}
