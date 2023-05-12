package com.redis.spring.batch.convert;

import java.util.function.Function;

import io.lettuce.core.ScoredValue;

public class ScoredValueConverter<V, T> implements Function<T, ScoredValue<V>> {

	private final Function<T, V> member;
	private final Function<T, Double> score;

	public ScoredValueConverter(Function<T, V> member, Function<T, Double> score) {
		this.member = member;
		this.score = score;
	}

	@Override
	public ScoredValue<V> apply(T source) {
		Double scoreValue = this.score.apply(source);
		if (scoreValue == null) {
			return null;
		}
		return ScoredValue.just(scoreValue, member.apply(source));
	}

}