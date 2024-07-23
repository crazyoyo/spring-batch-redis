package com.redis.spring.batch.item.redis.writer.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.writer.AbstractValueWriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class Restore<K, V, T> extends AbstractValueWriteOperation<K, V, byte[], T> {

	private final Predicate<T> deletePredicate = t -> ttl(t) == KeyValue.TTL_NO_KEY;
	private ToLongFunction<T> ttlFunction = t -> 0;

	public Restore(Function<T, K> keyFunction, Function<T, byte[]> valueFunction) {
		super(keyFunction, valueFunction);
	}

	public void setTtlFunction(ToLongFunction<T> function) {
		this.ttlFunction = function;
	}

	private long ttl(T item) {
		return ttlFunction.applyAsLong(item);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, List<? extends T> items) {
		List<RedisFuture<?>> futures = new ArrayList<>();
		List<K> toDelete = BatchUtils.stream(items).filter(deletePredicate).map(keyFunction)
				.collect(Collectors.toList());
		if (!toDelete.isEmpty()) {
			futures.add(commands.del((K[]) toDelete.toArray()));
		}
		futures.addAll(BatchUtils.stream(items).filter(deletePredicate.negate()).map(t -> restore(commands, t))
				.collect(Collectors.toList()));
		return (List) futures;
	}

	private RedisFuture<String> restore(RedisAsyncCommands<K, V> commands, T item) {
		RestoreArgs args = new RestoreArgs().replace(true);
		long ttl = ttl(item);
		if (ttl > 0) {
			args.absttl().ttl(ttl);
		}
		byte[] value = value(item);
		if (value == null) {
			return null;
		}
		return commands.restore(key(item), value, args);

	}

}
