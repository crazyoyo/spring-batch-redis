package com.redis.spring.batch.convert;

import java.util.function.Function;

import com.redis.lettucemod.timeseries.Sample;

public class SampleConverter<T> implements Function<T, Sample> {

	private final Function<T, Long> timestampConverter;
	private final Function<T, Double> valueConverter;

	public SampleConverter(Function<T, Long> timestamp, Function<T, Double> value) {
		this.timestampConverter = timestamp;
		this.valueConverter = value;
	}

	@Override
	public Sample apply(T source) {
		Double value = this.valueConverter.apply(source);
		if (value == null) {
			return null;
		}
		Long timestamp = this.timestampConverter.apply(source);
		if (timestamp == null) {
			timestamp = 0L;
		}
		return Sample.of(timestamp, value);
	}

}