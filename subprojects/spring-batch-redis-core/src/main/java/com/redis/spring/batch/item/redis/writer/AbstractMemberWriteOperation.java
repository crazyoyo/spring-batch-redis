package com.redis.spring.batch.item.redis.writer;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.item.redis.common.BatchUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

public abstract class AbstractMemberWriteOperation<K, V, T>
		extends AbstractValueWriteOperation<K, V, Collection<V>, T> {

	protected AbstractMemberWriteOperation(Function<T, K> keyFunction, Function<T, Collection<V>> valueFunction) {
		super(keyFunction, valueFunction);
	}

	@Override
	public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, Iterable<? extends T> items) {
		return BatchUtils.executeAll(commands, items, this::execute);
	}

	@SuppressWarnings("unchecked")
	private RedisFuture<Long> execute(RedisAsyncCommands<K, V> commands, T item) {
		Collection<V> value = value(item);
		if (CollectionUtils.isEmpty(value)) {
			return null;
		}
		return execute(commands, key(item), (V[]) value.toArray());
	}

	protected abstract RedisFuture<Long> execute(RedisAsyncCommands<K, V> commands, K key, V[] values);

}
