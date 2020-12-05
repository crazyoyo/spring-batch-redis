package org.springframework.batch.item.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.support.AbstractKeyCommandItemWriter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

public class ExpireItemWriter<T> extends AbstractKeyCommandItemWriter<T> {

	private final Converter<T, Long> timeoutConverter;

	protected ExpireItemWriter(AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig, Converter<T, String> keyConverter,
			Converter<T, Long> timeoutConverter) {
		super(client, poolConfig, keyConverter);
		Assert.notNull(timeoutConverter, "A timeout converter is required.");
		this.timeoutConverter = timeoutConverter;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> write(BaseRedisAsyncCommands<String, String> commands, String key, T item) {
		Long millis = timeoutConverter.convert(item);
		if (millis == null) {
			return null;
		}
		return ((RedisKeyAsyncCommands<String, String>) commands).pexpire(key, millis);
	}

	public static <T> ExpireItemWriterBuilder<T> builder() {
		return new ExpireItemWriterBuilder<>();
	}

	@Setter
	@Accessors(fluent = true)
	public static class ExpireItemWriterBuilder<T>
			extends AbstractKeyCommandItemWriterBuilder<T, ExpireItemWriterBuilder<T>> {

		private Converter<T, Long> timeoutConverter;

		public ExpireItemWriter<T> build() {
			return new ExpireItemWriter<>(client, poolConfig, keyConverter, timeoutConverter);
		}

	}

}
