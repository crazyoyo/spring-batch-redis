package com.redis.spring.batch.support.convert;

import com.redis.lettucemod.api.timeseries.Sample;
import org.springframework.core.convert.converter.Converter;

public class SampleConverter<T> implements Converter<T, Sample> {

    private final Converter<T, Long> timestamp;
    private final Converter<T, Double> value;

    public SampleConverter(Converter<T, Long> timestamp, Converter<T, Double> value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    @Override
    public Sample convert(T source) {
        Double value = this.value.convert(source);
        if (value == null) {
            return null;
        }
        Long timestamp = this.timestamp.convert(source);
        if (timestamp == null) {
            timestamp = 0L;
        }
        return new Sample(timestamp, value);
    }

}