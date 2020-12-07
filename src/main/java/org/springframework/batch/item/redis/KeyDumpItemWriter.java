package org.springframework.batch.item.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.support.AbstractRedisItemWriter;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.RedisConnectionPoolBuilder;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

public class KeyDumpItemWriter extends AbstractRedisItemWriter<KeyValue<byte[]>> {

	private final boolean replace;

	public KeyDumpItemWriter(AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig, boolean replace) {
		super(client, poolConfig);
		this.replace = replace;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected RedisFuture<?> write(BaseRedisAsyncCommands<String, String> async, KeyValue<byte[]> item) {
		RedisKeyAsyncCommands<String, String> commands = (RedisKeyAsyncCommands<String, String>) async;
		if (item.getValue() == null || item.noKeyTtl()) {
			return commands.del(item.getKey());
		}
		if (item.hasTtl()) {
			long ttlMillis = toMillis(item.getTtl());
			return commands.restore(item.getKey(), item.getValue(), new RestoreArgs().ttl(ttlMillis).replace(replace));
		}
		return commands.restore(item.getKey(), item.getValue(), new RestoreArgs().replace(replace));
	}

	private long toMillis(long ttl) {
		return ttl * 1000;
	}

	public static KeyDumpItemWriterBuilder builder(AbstractRedisClient client) {
		return new KeyDumpItemWriterBuilder(client);
	}

	@Setter
	@Accessors(fluent = true)
	public static class KeyDumpItemWriterBuilder extends RedisConnectionPoolBuilder<KeyDumpItemWriterBuilder> {

		public KeyDumpItemWriterBuilder(AbstractRedisClient client) {
			super(client);
		}

		private boolean replace;

		public KeyDumpItemWriterBuilder replace(boolean replace) {
			this.replace = replace;
			return this;
		}

		public KeyDumpItemWriter build() {
			return new KeyDumpItemWriter(client, poolConfig, replace);
		}

	}

}
