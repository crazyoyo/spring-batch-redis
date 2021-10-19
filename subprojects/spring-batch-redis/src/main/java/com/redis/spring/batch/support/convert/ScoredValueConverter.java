package com.redis.spring.batch.support.convert;

import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.ScoredValue;

public class ScoredValueConverter<V, T> implements Converter<T, ScoredValue<V>> {

	private final Converter<T, V> member;
	private final Converter<T, Double> score;

	public ScoredValueConverter(Converter<T, V> member, Converter<T, Double> score) {
		this.member = member;
		this.score = score;
	}

	@Override
	public ScoredValue<V> convert(T source) {
		Double score = this.score.convert(source);
		if (score == null) {
			return null;
		}
		return ScoredValue.just(score, member.convert(source));
	}

}