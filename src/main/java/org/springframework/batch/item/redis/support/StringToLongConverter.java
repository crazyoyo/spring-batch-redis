package org.springframework.batch.item.redis.support;

import org.springframework.core.convert.converter.Converter;

public class StringToLongConverter implements Converter<String, Long> {
    @Override
    public Long convert(String source) {
        return Long.parseLong(source);
    }
}
