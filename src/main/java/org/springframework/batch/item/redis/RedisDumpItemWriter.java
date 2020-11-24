package org.springframework.batch.item.redis;

import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractRedisItemWriter;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.RedisConnectionBuilder;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

public class RedisDumpItemWriter extends AbstractRedisItemWriter<KeyValue<byte[]>> {

	private final boolean replace;

	public RedisDumpItemWriter(GenericObjectPool<? extends StatefulConnection<String, String>> pool,
			Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> commands,
			long commandTimeout, boolean replace) {
		super(pool, commands, commandTimeout);
		this.replace = replace;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected RedisFuture<?> write(BaseRedisAsyncCommands<String, String> commands, KeyValue<byte[]> item) {
		RedisKeyAsyncCommands<String, String> keyCommands = (RedisKeyAsyncCommands<String, String>) commands;
		if (item.getValue() == null || item.noKeyTtl()) {
			return keyCommands.del(item.getKey());
		}
		if (item.hasTtl()) {
			long ttl = item.getTtl() * 1000;
			return keyCommands.restore(item.getKey(), item.getValue(), new RestoreArgs().ttl(ttl).replace(replace));
		}
		return keyCommands.restore(item.getKey(), item.getValue(), new RestoreArgs().replace(replace));
	}

	public static RedisDumpItemWriterBuilder builder() {
		return new RedisDumpItemWriterBuilder();
	}

	@Setter
	@Accessors(fluent = true)
	public static class RedisDumpItemWriterBuilder extends RedisConnectionBuilder<RedisDumpItemWriterBuilder> {

		private boolean replace;

		public RedisDumpItemWriterBuilder replace(boolean replace) {
			this.replace = replace;
			return this;
		}

		public RedisDumpItemWriter build() {
			return new RedisDumpItemWriter(pool(), async(), timeout(), replace);
		}

	}

}
