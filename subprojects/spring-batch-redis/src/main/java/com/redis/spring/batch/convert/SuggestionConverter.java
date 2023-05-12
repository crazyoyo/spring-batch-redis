package com.redis.spring.batch.convert;

import java.util.function.Function;

import com.redis.lettucemod.search.Suggestion;

public class SuggestionConverter<V, T> implements Function<T, Suggestion<V>> {

	private final Function<T, V> string;
	private final Function<T, Double> score;
	private final Function<T, V> payload;

	public SuggestionConverter(Function<T, V> string, Function<T, Double> score, Function<T, V> payload) {
		this.string = string;
		this.score = score;
		this.payload = payload;
	}

	@Override
	public Suggestion<V> apply(T source) {
		Suggestion<V> suggestion = new Suggestion<>();
		suggestion.setString(string.apply(source));
		suggestion.setScore(score.apply(source));
		suggestion.setPayload(payload.apply(source));
		return suggestion;
	}

}