package com.redis.spring.batch;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.spring.batch.common.KeyBuilder;

class KeyBuilderTests {

	@Test
	void createKeysInKeyspace() {
		KeyBuilder builder = KeyBuilder.of("smartcache");
		Assertions.assertEquals("smartcache:metrics", builder.build("metrics"));
		Assertions.assertEquals("smartcache:cache:123", builder.sub("cache").build("123"));
		Assertions.assertEquals("smartcache:cache:123", builder.sub("cache").build(123));
		Assertions.assertEquals("metrics", builder.noKeyspace().build("metrics"));
		Assertions.assertEquals("metrics:123", builder.noKeyspace().build("metrics", "123"));
	}

}
