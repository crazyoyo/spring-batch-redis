package org.springframework.batch.item.redis.support;

import org.springframework.core.convert.converter.Converter;

public class StringChannelConverter implements Converter<String, String> {

    @Override
    public String convert(String source) {
	int pos = source.indexOf(":");
	return source.substring(pos + 1);
    }

}
