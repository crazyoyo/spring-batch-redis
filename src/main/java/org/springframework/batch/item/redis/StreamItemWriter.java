package org.springframework.batch.item.redis;

import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.support.AbstractKeyCommandItemWriter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

public class StreamItemWriter<T> extends AbstractKeyCommandItemWriter<T> {

	private final Converter<T, Map<String, String>> bodyConverter;
	private final Converter<T, XAddArgs> argsConverter;

	public StreamItemWriter(AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig, Converter<T, String> keyConverter,
			Converter<T, Map<String, String>> bodyConverter, Converter<T, XAddArgs> argsConverter) {
		super(client, poolConfig, keyConverter);
		Assert.notNull(bodyConverter, "Body converter is required.");
		Assert.notNull(argsConverter, "Args converter is required.");
		this.bodyConverter = bodyConverter;
		this.argsConverter = argsConverter;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> write(BaseRedisAsyncCommands<String, String> commands, String key, T item) {
		RedisStreamAsyncCommands<String, String> streamCommands = (RedisStreamAsyncCommands<String, String>) commands;
		XAddArgs args = argsConverter.convert(item);
		Map<String, String> body = bodyConverter.convert(item);
		return streamCommands.xadd(key, args, body);
	}

	public static <T> StreamItemWriterBuilder<T> builder() {
		return new StreamItemWriterBuilder<>();
	}

	@Setter
	@Accessors(fluent = true)
	public static class StreamItemWriterBuilder<T>
			extends AbstractKeyCommandItemWriterBuilder<T, StreamItemWriterBuilder<T>> {

		private Converter<T, Map<String, String>> bodyConverter;

		private Converter<T, XAddArgs> argsConverter = s -> null;

		public StreamItemWriter<T> build() {
			return new StreamItemWriter<>(client, poolConfig, keyConverter, bodyConverter, argsConverter);
		}

	}

}
