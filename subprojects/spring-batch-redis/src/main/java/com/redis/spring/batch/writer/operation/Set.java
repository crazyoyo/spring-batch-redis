package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import org.springframework.util.Assert;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;

public class Set<K, V, T> extends AbstractWriteOperation<K, V, T, String> {

	private static final SetArgs DEFAULT_ARGS = new SetArgs();

	private final Function<T, V> value;
	private final Function<T, SetArgs> args;

	public Set(Function<T, K> key, Function<T, V> value) {
		this(key, value, t -> DEFAULT_ARGS);
	}

	public Set(Function<T, K> key, Function<T, V> value, Function<T, SetArgs> args) {
		super(key);
		Assert.notNull(value, "A value function is required");
		Assert.notNull(args, "A args function is required");
		this.value = value;
		this.args = args;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<String> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		return ((RedisStringAsyncCommands<K, V>) commands).set(key, value.apply(item), args.apply(item));
	}

}
