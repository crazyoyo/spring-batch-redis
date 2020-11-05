package org.springframework.batch.item.redis.support;

import java.util.regex.Pattern;

public class KeyFilter implements Filter<String> {

    private final Pattern pattern;

    public KeyFilter(Pattern pattern) {
	this.pattern = pattern;
    }

    @Override
    public boolean accept(String object) {
	return pattern.matcher(object).matches();
    }

}
