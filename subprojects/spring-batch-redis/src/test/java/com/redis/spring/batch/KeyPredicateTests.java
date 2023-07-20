package com.redis.spring.batch;

import java.util.function.Predicate;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.spring.batch.common.IntRange;
import com.redis.spring.batch.common.KeyPredicateFactory;

class KeyPredicateTests {

    @Test
    void include() {
        Predicate<String> predicate = KeyPredicateFactory.create().include("foo").build();
        Assertions.assertTrue(predicate.test("foo"));
        Assertions.assertFalse(predicate.test("bar"));
        predicate = KeyPredicateFactory.create().include("foo.*").build();
        Assertions.assertTrue(predicate.test("foobar"));
        Assertions.assertFalse(predicate.test("barfoo"));
        predicate = KeyPredicateFactory.create().include("foo.*", "bar.*").build();
        Assertions.assertTrue(predicate.test("foobar"));
        Assertions.assertTrue(predicate.test("barfoo"));
        Assertions.assertFalse(predicate.test("key"));
    }

    @Test
    void exclude() {
        Predicate<String> predicate = KeyPredicateFactory.create().exclude("foo").build();
        Assertions.assertFalse(predicate.test("foo"));
        Assertions.assertTrue(predicate.test("bar"));
        predicate = KeyPredicateFactory.create().exclude("foo.*").build();
        Assertions.assertFalse(predicate.test("foobar"));
        Assertions.assertTrue(predicate.test("barfoo"));
    }

    @Test
    void includeAndExclude() {
        Predicate<String> predicate = KeyPredicateFactory.create().include("foo1").exclude("foo").build();
        Assertions.assertFalse(predicate.test("foo"));
        Assertions.assertFalse(predicate.test("bar"));
        Assertions.assertTrue(predicate.test("foo1"));
        predicate = KeyPredicateFactory.create().include("foo1.*").exclude("foo.*").build();
        Assertions.assertFalse(predicate.test("foo"));
        Assertions.assertFalse(predicate.test("bar"));
        Assertions.assertTrue(predicate.test("foo1"));
    }

    @Test
    void slotRange() {
        Predicate<String> predicate = KeyPredicateFactory.create().slotRange(IntRange.unbounded()).build();
        Assertions.assertTrue(predicate.test("foo"));
        Assertions.assertTrue(predicate.test("foo1"));
        predicate = KeyPredicateFactory.create().slotRange(IntRange.is(999999)).build();
        Assertions.assertFalse(predicate.test("foo"));
    }

    @Test
    void kitchenSink() {
        Predicate<String> predicate = KeyPredicateFactory.create().include("foo1").exclude("foo")
                .slotRange(IntRange.unbounded()).build();
        Assertions.assertFalse(predicate.test("foo"));
        Assertions.assertFalse(predicate.test("bar"));
        Assertions.assertTrue(predicate.test("foo1"));
    }

}
