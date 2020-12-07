package org.springframework.batch.item.redis.support;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public abstract class AbstractCollectionCommandItemWriter<T> extends AbstractKeyCommandItemWriter<T> {

	private final Converter<T, String> memberIdConverter;

	protected AbstractCollectionCommandItemWriter(AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig, Converter<T, String> keyConverter,
			Converter<T, String> memberIdConverter) {
		super(client, poolConfig, keyConverter);
		Assert.notNull(memberIdConverter, "A member id converter is required.");
		this.memberIdConverter = memberIdConverter;
	}

	@Override
	protected RedisFuture<?> write(BaseRedisAsyncCommands<String, String> commands, String key, T item) {
		return write(commands, key, memberIdConverter.convert(item), item);
	}

	protected abstract RedisFuture<?> write(BaseRedisAsyncCommands<String, String> commands, String key,
			String memberId, T item);

	public static abstract class AbstractCollectionCommandItemWriterBuilder<T, B extends AbstractCollectionCommandItemWriterBuilder<T, B>>
			extends AbstractKeyCommandItemWriterBuilder<T, B> {

		protected AbstractCollectionCommandItemWriterBuilder(AbstractRedisClient client) {
			super(client);
		}

		protected Converter<T, String> memberIdConverter;

		@SuppressWarnings("unchecked")
		public B memberIdConverter(Converter<T, String> memberIdConverter) {
			this.memberIdConverter = memberIdConverter;
			return (B) this;
		}

	}
}
