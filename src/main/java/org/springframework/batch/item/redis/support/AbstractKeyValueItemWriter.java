package org.springframework.batch.item.redis.support;

import java.util.List;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public abstract class AbstractKeyValueItemWriter<K, V, T extends AbstractKeyValue<K, ?>>
		extends AbstractRedisItemWriter<K, V, T> {

	@Override
	@SuppressWarnings("unchecked")
	protected void write(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item) {
		if (item.getValue() == null || item.getTtl() == AbstractKeyValue.TTL_NOT_EXISTS) {
			futures.add(((RedisKeyAsyncCommands<K, V>) commands).del(item.getKey()));
		} else {
			if (item.getTtl() >= 0) {
				doWrite(commands, futures, item, item.getTtl());
			} else {
				doWrite(commands, futures, item);
			}
		}
	}

	protected abstract void doWrite(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item);

	/**
	 * 
	 * @param commands
	 * @param futures
	 * @param item
	 * @param ttl      time-to-live in seconds
	 */
	protected abstract void doWrite(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item,
			long ttl);

}
