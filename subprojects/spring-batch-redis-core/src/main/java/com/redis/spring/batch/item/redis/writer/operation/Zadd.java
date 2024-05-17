package com.redis.spring.batch.item.redis.writer.operation;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.writer.AbstractValueWriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class Zadd<K, V, T> extends AbstractValueWriteOperation<K, V, Collection<ScoredValue<V>>, T> {

	private Function<T, ZAddArgs> argsFunction = t -> null;

	public Zadd(Function<T, K> keyFunction, Function<T, Collection<ScoredValue<V>>> valueFunction) {
		super(keyFunction, valueFunction);
	}

	public void setArgs(ZAddArgs args) {
		this.argsFunction = t -> args;
	}

	public void setArgsFunction(Function<T, ZAddArgs> function) {
		this.argsFunction = function;
	}

	@Override
	public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, Iterable<? extends T> items) {
		return BatchUtils.executeAll(commands, items, this::execute);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private RedisFuture<Long> execute(RedisAsyncCommands<K, V> commands, T item) {
		K key = key(item);
		ZAddArgs args = argsFunction.apply(item);
		Collection<ScoredValue<V>> collection = value(item);
		if (CollectionUtils.isEmpty(collection)) {
			return null;
		}
		ScoredValue[] values = collection.toArray(new ScoredValue[0]);
		return commands.zadd(key, args, values);
	}

}
