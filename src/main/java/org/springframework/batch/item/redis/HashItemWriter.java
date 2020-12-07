package org.springframework.batch.item.redis;

import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.support.AbstractKeyCommandItemWriter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

public class HashItemWriter<T> extends AbstractKeyCommandItemWriter<T> {

	private final Converter<T, Map<String, String>> mapConverter;

	public HashItemWriter(AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig, Converter<T, String> keyConverter,
			Converter<T, Map<String, String>> mapConverter) {
		super(client, poolConfig, keyConverter);
		Assert.notNull(mapConverter, "A map converter is required.");
		this.mapConverter = mapConverter;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> write(BaseRedisAsyncCommands<String, String> commands, String key, T item) {
		return ((RedisHashAsyncCommands<String, String>) commands).hmset(key, mapConverter.convert(item));
	}

	public static <T> HashItemWriterBuilder<T> builder(AbstractRedisClient client) {
		return new HashItemWriterBuilder<>(client);
	}

	@Setter
	@Accessors(fluent = true)
	public static class HashItemWriterBuilder<T>
			extends AbstractKeyCommandItemWriterBuilder<T, HashItemWriterBuilder<T>> {

		protected HashItemWriterBuilder(AbstractRedisClient client) {
			super(client);
		}

		private Converter<T, Map<String, String>> mapConverter;

		public HashItemWriter<T> build() {
			return new HashItemWriter<>(client, poolConfig, keyConverter, mapConverter);
		}

	}

}
