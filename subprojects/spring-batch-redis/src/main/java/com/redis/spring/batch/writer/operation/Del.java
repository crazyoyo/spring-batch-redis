package com.redis.spring.batch.writer.operation;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import com.redis.spring.batch.writer.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class Del<K, V, T> implements Operation<K, V, T> {

	private final Converter<T, K> keyConverter;

	public Del(Converter<T, K> key) {
		Assert.notNull(key, "A key converter is required");
		this.keyConverter = key;
	}

	@SuppressWarnings("unchecked")
	@Override
	public RedisFuture<Long> execute(BaseRedisAsyncCommands<K, V> commands, T item) {
		K key = this.keyConverter.convert(item);
		return ((RedisKeyAsyncCommands<K, K>) commands).del(key);
	}

	public static <K, V, T> Del<K, V, T> of(Converter<T, K> key) {
		return new Del<>(key);
	}

}
