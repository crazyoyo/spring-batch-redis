package org.springframework.batch.item.redis.support;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public abstract class AbstractKeyCommandItemWriter<T> extends AbstractRedisItemWriter<T> {

	private final Converter<T, String> keyConverter;

	protected AbstractKeyCommandItemWriter(AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig, Converter<T, String> keyConverter) {
		super(client, poolConfig);
		Assert.notNull(keyConverter, "A key converter is required.");
		this.keyConverter = keyConverter;
	}

	@Override
	protected RedisFuture<?> write(BaseRedisAsyncCommands<String, String> commands, T item) {
		return write(commands, keyConverter.convert(item), item);
	}

	protected abstract RedisFuture<?> write(BaseRedisAsyncCommands<String, String> commands, String key, T item);

	public static abstract class AbstractKeyCommandItemWriterBuilder<T, B extends AbstractKeyCommandItemWriterBuilder<T, B>>
			extends RedisConnectionPoolBuilder<B> {

		protected AbstractKeyCommandItemWriterBuilder(AbstractRedisClient client) {
			super(client);
		}

		protected Converter<T, String> keyConverter;

		@SuppressWarnings("unchecked")
		public B keyConverter(Converter<T, String> keyConverter) {
			this.keyConverter = keyConverter;
			return (B) this;
		}

	}

}
