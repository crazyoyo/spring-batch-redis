package com.redis.spring.batch.writer.operation;

import java.util.function.Function;
import java.util.function.ToLongFunction;

import org.springframework.batch.item.Chunk;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class ExpireAt<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {

	private ToLongFunction<T> epochFunction = t -> 0;

	public ExpireAt(Function<T, K> keyFunction) {
		super(keyFunction);
	}

	public void setEpoch(long epoch) {
		this.epochFunction = t -> epoch;
	}

	public void setEpochFunction(ToLongFunction<T> epochFunction) {
		this.epochFunction = epochFunction;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, Chunk<RedisFuture<Object>> outputs) {
		long millis = epochFunction.applyAsLong(item);
		if (millis > 0) {
			outputs.add((RedisFuture) ((RedisKeyAsyncCommands<K, V>) commands).pexpireat(key, millis));
		}
	}

}
