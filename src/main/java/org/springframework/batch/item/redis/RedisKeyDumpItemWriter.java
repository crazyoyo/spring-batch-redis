package org.springframework.batch.item.redis;

import java.util.List;

import org.springframework.batch.item.redis.support.AbstractKeyValueItemWriter;
import org.springframework.batch.item.redis.support.KeyDump;
import org.springframework.batch.item.redis.support.RedisConnectionBuilder;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import lombok.Setter;

public class RedisKeyDumpItemWriter<K, V> extends AbstractKeyValueItemWriter<K, V, KeyDump<K>> {

	@Setter
	private boolean replace;

	@Override
	@SuppressWarnings("unchecked")
	protected void doWrite(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, KeyDump<K> item) {
		futures.add(((RedisKeyAsyncCommands<K, V>) commands).restore(item.getKey(), item.getValue(),
				new RestoreArgs().replace(replace)));
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void doWrite(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, KeyDump<K> item,
			long ttl) {
		long ttlInMillis = ttl * 1000;
		futures.add(((RedisKeyAsyncCommands<K, V>) commands).restore(item.getKey(), item.getValue(),
				new RestoreArgs().ttl(ttlInMillis).replace(replace)));
	}

	public static RedisKeyDumpItemWriterBuilder builder() {
		return new RedisKeyDumpItemWriterBuilder();
	}

	public static class RedisKeyDumpItemWriterBuilder extends RedisConnectionBuilder<RedisKeyDumpItemWriterBuilder> {

		private boolean replace;

		public RedisKeyDumpItemWriterBuilder replace(boolean replace) {
			this.replace = replace;
			return this;
		}

		public RedisKeyDumpItemWriter<String, String> build() {
			RedisKeyDumpItemWriter<String, String> writer = new RedisKeyDumpItemWriter<>();
			writer.setPool(pool());
			writer.setCommands(async());
			writer.setCommandTimeout(timeout());
			writer.setReplace(replace);
			return writer;
		}

	}
}
