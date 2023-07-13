package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import com.redis.spring.batch.writer.WriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;

public class ZaddAll<K, V, T> implements WriteOperation<K, V, T> {

	private final Function<T, K> keyFunction;
	private final Function<T, Collection<ScoredValue<V>>> membersFunction;
	private final ZAddArgs args;

	public ZaddAll(Function<T, K> key, Function<T, Collection<ScoredValue<V>>> members) {
		this(key, members, null);
	}

	public ZaddAll(Function<T, K> key, Function<T, Collection<ScoredValue<V>>> members, ZAddArgs args) {
		this.keyFunction = key;
		this.membersFunction = members;
		this.args = args;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<Object>> futures) {
		K key = keyFunction.apply(item);
		ScoredValue<V>[] members = membersFunction.apply(item).toArray(new ScoredValue[0]);
		RedisSortedSetAsyncCommands<K, V> zset = (RedisSortedSetAsyncCommands<K, V>) commands;
		futures.add((RedisFuture) zset.zadd(key, args, members));
	}

}
