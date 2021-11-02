package com.redis.spring.batch.support.convert;

import com.redis.lettucemod.api.timeseries.Sample;
import org.springframework.core.convert.converter.Converter;

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
        return new Sample(timestamp, value);
    }

}