package com.redis.spring.batch.convert;

import org.springframework.core.convert.converter.Converter;

import com.redis.lettucemod.search.Suggestion;

public class SuggestionConverter<V, T> implements Converter<T, Suggestion<V>> {

	private final Converter<T, V> stringConverter;
	private final Converter<T, Double> scoreConverter;
	private final Converter<T, V> payloadConverter;

	public SuggestionConverter(Converter<T, V> string, Converter<T, Double> score, Converter<T, V> payload) {
		this.stringConverter = string;
		this.scoreConverter = score;
		this.payloadConverter = payload;
	}

	@Override
	public Suggestion<V> convert(T source) {
		Suggestion<V> suggestion = new Suggestion<>();
		suggestion.setString(stringConverter.convert(source));
		suggestion.setScore(scoreConverter.convert(source));
		suggestion.setPayload(payloadConverter.convert(source));
		return suggestion;
	}

}