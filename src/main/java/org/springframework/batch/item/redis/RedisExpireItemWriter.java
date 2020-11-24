package org.springframework.batch.item.redis;

import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractKeyCommandItemWriter;
import org.springframework.batch.item.redis.support.AbstractKeyCommandItemWriterBuilder;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

public class RedisExpireItemWriter<T> extends AbstractKeyCommandItemWriter<T> {

	private final Converter<T, Long> timeoutConverter;

	protected RedisExpireItemWriter(GenericObjectPool<? extends StatefulConnection<String, String>> pool,
			Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> commands,
			long commandTimeout, Converter<T, String> keyConverter, Converter<T, Long> timeoutConverter) {
		super(pool, commands, commandTimeout, keyConverter);
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

	public static <T> RedisExpireItemWriterBuilder<T> builder() {
		return new RedisExpireItemWriterBuilder<>();
	}

	@Setter
	@Accessors(fluent = true)
	public static class RedisExpireItemWriterBuilder<T>
			extends AbstractKeyCommandItemWriterBuilder<T, RedisExpireItemWriterBuilder<T>> {

		private Converter<T, Long> timeoutConverter;

		public RedisExpireItemWriter<T> build() {
			return new RedisExpireItemWriter<>(pool(), async(), timeout(), keyConverter, timeoutConverter);
		}

	}

}
