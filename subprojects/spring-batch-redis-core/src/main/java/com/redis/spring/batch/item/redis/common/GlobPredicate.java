package com.redis.spring.batch.item.redis.common;

import java.util.function.Predicate;

import com.hrakaroo.glob.GlobPattern;
import com.hrakaroo.glob.MatchingEngine;

public class GlobPredicate implements Predicate<String> {

	private final MatchingEngine matchingEngine;

	public GlobPredicate(String pattern) {
		this.matchingEngine = GlobPattern.compile(pattern);
	}

	public GlobPredicate(String pattern, char wildcardChar, char matchOneChar, int flags) {
		this.matchingEngine = GlobPattern.compile(pattern, wildcardChar, matchOneChar, flags);
	}

	@Override
	public boolean test(String t) {
		return matchingEngine.matches(t);
	}
}
