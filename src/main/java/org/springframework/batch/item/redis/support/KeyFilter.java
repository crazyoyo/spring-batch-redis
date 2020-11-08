package org.springframework.batch.item.redis.support;

import java.util.regex.Pattern;

import lombok.Builder;

public class KeyFilter implements Filter<String> {

    private final Pattern pattern;

    @Builder
    public KeyFilter(String pattern) {
	this.pattern = Pattern.compile(GlobToRegexConverter.convert(pattern));
    }

    @Override
    public boolean accept(String object) {
	return pattern.matcher(object).matches();
    }

}
