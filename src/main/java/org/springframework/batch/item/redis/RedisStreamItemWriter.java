package org.springframework.batch.item.redis;

import java.time.Duration;
import java.util.Map;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractKeyCommandItemWriter;
import org.springframework.batch.item.redis.support.AbstractKeyCommandItemWriterBuilder;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import lombok.Setter;
import lombok.experimental.Accessors;

public class RedisStreamItemWriter<K, V, T> extends AbstractKeyCommandItemWriter<K, V, T> {

	private final Converter<T, Map<K, V>> bodyConverter;
	private final Converter<T, XAddArgs> argsConverter;

	public RedisStreamItemWriter(GenericObjectPool<? extends StatefulConnection<K, V>> pool,
			Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout,
			Converter<T, K> keyConverter, Converter<T, Map<K, V>> bodyConverter, Converter<T, XAddArgs> argsConverter) {
		super(pool, commands, commandTimeout, keyConverter);
		Assert.notNull(bodyConverter, "A body converter is required.");
		this.bodyConverter = bodyConverter;
		this.argsConverter = argsConverter;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, K key, T item) {
		RedisStreamAsyncCommands<K, V> streamCommands = (RedisStreamAsyncCommands<K, V>) commands;
		Map<K, V> body = bodyConverter.convert(item);
		if (argsConverter == null) {
			return streamCommands.xadd(key, body);
		}
		return streamCommands.xadd(key, argsConverter.convert(item), body);
	}

	public static <T> RedisStreamItemWriterBuilder<String, String, T> builder() {
		return new RedisStreamItemWriterBuilder<>(StringCodec.UTF8);
	}

	@Setter
	@Accessors(fluent = true)
	public static class RedisStreamItemWriterBuilder<K, V, T>
			extends AbstractKeyCommandItemWriterBuilder<K, V, T, RedisStreamItemWriterBuilder<K, V, T>> {

		private Converter<T, Map<K, V>> bodyConverter;
		private Converter<T, XAddArgs> argsConverter;

		public RedisStreamItemWriterBuilder(RedisCodec<K, V> codec) {
			super(codec);
		}

		public RedisStreamItemWriter<K, V, T> build() {
			return new RedisStreamItemWriter<>(pool(), async(), timeout(), keyConverter, bodyConverter, argsConverter);
		}

	}

}
