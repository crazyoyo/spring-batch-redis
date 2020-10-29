package org.springframework.batch.item.redis;

import java.time.Duration;
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
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import lombok.Setter;
import lombok.experimental.Accessors;

public class RedisExpireItemWriter<K, V, T> extends AbstractKeyCommandItemWriter<K, V, T> {

	private final Converter<T, Long> timeoutConverter;

	protected RedisExpireItemWriter(GenericObjectPool<? extends StatefulConnection<K, V>> pool,
			Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout,
			Converter<T, K> keyConverter, Converter<T, Long> timeoutConverter) {
		super(pool, commands, commandTimeout, keyConverter);
		Assert.notNull(timeoutConverter, "A timeout converter is required.");
		this.timeoutConverter = timeoutConverter;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, K key, T item) {
		Long millis = timeoutConverter.convert(item);
		if (millis == null) {
			return null;
		}
		return ((RedisKeyAsyncCommands<K, V>) commands).pexpire(key, millis);
	}

	public static <T> RedisExpireItemWriterBuilder<String, String, T> builder() {
		return new RedisExpireItemWriterBuilder<>(StringCodec.UTF8);
	}

	@Setter
	@Accessors(fluent = true)
	public static class RedisExpireItemWriterBuilder<K, V, T>
			extends AbstractKeyCommandItemWriterBuilder<K, V, T, RedisExpireItemWriterBuilder<K, V, T>> {

		private Converter<T, Long> timeoutConverter;

		public RedisExpireItemWriterBuilder(RedisCodec<K, V> codec) {
			super(codec);
		}

		public RedisExpireItemWriter<K, V, T> build() {
			return new RedisExpireItemWriter<>(pool(), async(), timeout(), keyConverter, timeoutConverter);
		}

	}

}