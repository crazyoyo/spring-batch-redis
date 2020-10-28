package org.springframework.batch.item.redis;

import java.time.Duration;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractRedisItemWriter;
import org.springframework.batch.item.redis.support.KeyDump;
import org.springframework.batch.item.redis.support.RedisConnectionBuilder;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisDumpItemWriter<K, V> extends AbstractRedisItemWriter<K, V, KeyDump<K>> {

	private final boolean replace;

	public RedisDumpItemWriter(GenericObjectPool<? extends StatefulConnection<K, V>> pool,
			Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout,
			boolean replace) {
		super(pool, commands, commandTimeout);
		this.replace = replace;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, KeyDump<K> item) {
		RedisKeyAsyncCommands<K, V> keyCommands = (RedisKeyAsyncCommands<K, V>) commands;
		if (item.getValue() == null || item.isTtlNoKey()) {
			return keyCommands.del(item.getKey());
		}
		if (item.hasTtl()) {
			long ttl = item.getTtl() * 1000;
			return keyCommands.restore(item.getKey(), item.getValue(), new RestoreArgs().ttl(ttl).replace(replace));
		}
		return keyCommands.restore(item.getKey(), item.getValue(), new RestoreArgs().replace(replace));
	}

	public static RedisKeyDumpItemWriterBuilder<String, String> builder() {
		return new RedisKeyDumpItemWriterBuilder<>(StringCodec.UTF8);
	}

	public static class RedisKeyDumpItemWriterBuilder<K, V>
			extends RedisConnectionBuilder<K, V, RedisKeyDumpItemWriterBuilder<K, V>> {

		public RedisKeyDumpItemWriterBuilder(RedisCodec<K, V> codec) {
			super(codec);
		}

		private boolean replace;

		public RedisKeyDumpItemWriterBuilder<K, V> replace(boolean replace) {
			this.replace = replace;
			return this;
		}

		public RedisDumpItemWriter<K, V> build() {
			return new RedisDumpItemWriter<>(pool(), async(), timeout(), replace);
		}

	}

}
