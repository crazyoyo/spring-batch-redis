package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.function.Function;

import org.springframework.util.Assert;

import com.redis.spring.batch.writer.WriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class Del<K, V, T> implements WriteOperation<K, V, T> {

	private final Function<T, K> key;

	public Del(Function<T, K> key) {
		Assert.notNull(key, "A key function is required");
		this.key = key;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item) {
		futures.add(((RedisKeyAsyncCommands<K, K>) commands).del(key.apply(item)));
	}

}
