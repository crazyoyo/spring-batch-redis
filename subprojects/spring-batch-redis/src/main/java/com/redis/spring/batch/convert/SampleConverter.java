package com.redis.spring.batch.convert;

import org.springframework.core.convert.converter.Converter;

import com.redis.lettucemod.timeseries.Sample;

public class SampleConverter<T> implements Converter<T, Sample> {

    private final Converter<T, Long> timestampConverter;
    private final Converter<T, Double> valueConverter;

    public SampleConverter(Converter<T, Long> timestamp, Converter<T, Double> value) {
        this.timestampConverter = timestamp;
        this.valueConverter = value;
    }

    @Override
    public Sample convert(T source) {
        Double value = this.valueConverter.convert(source);
        if (value == null) {
            return null;
        }
        Long timestamp = this.timestampConverter.convert(source);
        if (timestamp == null) {
            timestamp = 0L;
        }
        return Sample.of(timestamp, value);
    }

}