package com.redis.spring.batch.operation;

import org.springframework.batch.item.Chunk;

import com.redis.spring.batch.KeyValue;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class Restore<K, V> extends AbstractKeyWriteOperation<K, V, KeyValue<K>> {

	public Restore() {
		super(KeyValue::getKey);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void execute(BaseRedisAsyncCommands<K, V> commands, KeyValue<K> item, K key,
			Chunk<RedisFuture<Object>> outputs) {
		RedisKeyAsyncCommands<K, V> keyCommands = (RedisKeyAsyncCommands<K, V>) commands;
		if (item.getValue() == null || item.getTtl() == KeyValue.TTL_KEY_DOES_NOT_EXIST) {
			outputs.add((RedisFuture) keyCommands.del(item.getKey()));
		} else {
			RestoreArgs args = new RestoreArgs().replace(true);
			if (item.getTtl() > 0) {
				args.absttl().ttl(item.getTtl());
			}
			outputs.add((RedisFuture) keyCommands.restore(item.getKey(), (byte[]) item.getValue(), args));
		}
	}

}
