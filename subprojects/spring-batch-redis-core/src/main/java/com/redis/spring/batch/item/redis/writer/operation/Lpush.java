package com.redis.spring.batch.item.redis.writer.operation;

import java.util.Collection;
import java.util.function.Function;

import com.redis.spring.batch.item.redis.writer.AbstractMemberWriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class Lpush<K, V, T> extends AbstractMemberWriteOperation<K, V, T> {

	public Lpush(Function<T, K> keyFunction, Function<T, Collection<V>> valueFunction) {
		super(keyFunction, valueFunction);
	}

	@Override
	protected RedisFuture<Long> execute(RedisAsyncCommands<K, V> commands, K key, V[] values) {
		return commands.lpush(key, values);
	}

}
