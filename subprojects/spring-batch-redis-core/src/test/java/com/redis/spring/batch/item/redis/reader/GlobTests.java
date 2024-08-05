package com.redis.spring.batch.item.redis.reader;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

import com.redis.spring.batch.item.redis.common.GlobPredicate;

public class GlobTests {

	private Predicate<String> globPredicate(String match) {
		return new GlobPredicate(match);
	}

	@Test
	void include() {
		Predicate<String> foo = globPredicate("foo");
		assertTrue(foo.test("foo"));
		assertFalse(foo.test("bar"));
		Predicate<String> fooStar = globPredicate("foo*");
		assertTrue(fooStar.test("foobar"));
		assertFalse(fooStar.test("barfoo"));
	}

	@Test
	void exclude() {
		Predicate<String> foo = globPredicate("foo").negate();
		assertFalse(foo.test("foo"));
		assertTrue(foo.test("foa"));
		Predicate<String> fooStar = globPredicate("foo*").negate();
		assertFalse(fooStar.test("foobar"));
		assertTrue(fooStar.test("barfoo"));
	}

	@Test
	void includeAndExclude() {
		Predicate<String> foo1 = globPredicate("foo1").and(globPredicate("foo").negate());
		assertFalse(foo1.test("foo"));
		assertFalse(foo1.test("bar"));
		assertTrue(foo1.test("foo1"));
		Predicate<String> foo1Star = globPredicate("foo").and(globPredicate("foo1*").negate());
		assertTrue(foo1Star.test("foo"));
		assertFalse(foo1Star.test("bar"));
		assertFalse(foo1Star.test("foo1"));
	}

}
