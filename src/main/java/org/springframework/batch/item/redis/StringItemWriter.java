package org.springframework.batch.item.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.support.AbstractKeyCommandItemWriter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

public class StringItemWriter<T> extends AbstractKeyCommandItemWriter<T> {

	private final Converter<T, String> valueConverter;

	public StringItemWriter(AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig, Converter<T, String> keyConverter,
			Converter<T, String> valueConverter) {
		super(client, poolConfig, keyConverter);
		Assert.notNull(valueConverter, "A value converter is required.");
		this.valueConverter = valueConverter;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> write(BaseRedisAsyncCommands<String, String> commands, String key, T item) {
		return ((RedisStringAsyncCommands<String, String>) commands).set(key, valueConverter.convert(item));
	}

	public static <T> StringItemWriterBuilder<T> builder() {
		return new StringItemWriterBuilder<>();
	}

	@Setter
	@Accessors(fluent = true)
	public static class StringItemWriterBuilder<T>
			extends AbstractKeyCommandItemWriterBuilder<T, StringItemWriterBuilder<T>> {

		private Converter<T, String> valueConverter;

		public StringItemWriter<T> build() {
			return new StringItemWriter<>(client, poolConfig, keyConverter, valueConverter);
		}

	}

}
