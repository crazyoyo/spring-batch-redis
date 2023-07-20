package com.redis.spring.batch.convert;

import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;

import com.redis.lettucemod.timeseries.Sample;

public class SampleConverter<T> implements Function<T, Sample> {

    private final ToLongFunction<T> timestampConverter;

    private final ToDoubleFunction<T> valueConverter;

    public SampleConverter(ToLongFunction<T> timestamp, ToDoubleFunction<T> value) {
        this.timestampConverter = timestamp;
        this.valueConverter = value;
    }

    @Override
    public Sample apply(T source) {
        double value = this.valueConverter.applyAsDouble(source);
        long timestamp = this.timestampConverter.applyAsLong(source);
        return Sample.of(timestamp, value);
    }

}
