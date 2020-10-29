package org.springframework.batch.item.redis;

import java.time.Duration;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractCollectionCommandItemWriter;
import org.springframework.batch.item.redis.support.AbstractCollectionCommandItemWriterBuilder;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import lombok.Setter;
import lombok.experimental.Accessors;

public class RedisSortedSetItemWriter<K, V, T> extends AbstractCollectionCommandItemWriter<K, V, T> {

	private final Converter<T, Double> scoreConverter;

	public RedisSortedSetItemWriter(GenericObjectPool<? extends StatefulConnection<K, V>> pool,
			Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout,
			Converter<T, K> keyConverter, Converter<T, V> memberIdConverter, Converter<T, Double> scoreConverter) {
		super(pool, commands, commandTimeout, keyConverter, memberIdConverter);
		Assert.notNull(scoreConverter, "A score converter is required.");
		this.scoreConverter = scoreConverter;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, K key, V memberId, T item) {
		Double score = scoreConverter.convert(item);
		if (score == null) {
			return null;
		}
		return ((RedisSortedSetAsyncCommands<K, V>) commands).zadd(key, score, memberId);
	}

	public static <T> RedisSortedSetItemWriterBuilder<String, String, T> builder() {
		return new RedisSortedSetItemWriterBuilder<>(StringCodec.UTF8);
	}

	@Setter
	@Accessors(fluent = true)
	public static class RedisSortedSetItemWriterBuilder<K, V, T>
			extends AbstractCollectionCommandItemWriterBuilder<K, V, T, RedisSortedSetItemWriterBuilder<K, V, T>> {

		private Converter<T, Double> scoreConverter;

		public RedisSortedSetItemWriterBuilder(RedisCodec<K, V> codec) {
			super(codec);
		}

		public RedisSortedSetItemWriter<K, V, T> build() {
			return new RedisSortedSetItemWriter<>(pool(), async(), timeout(), keyConverter, memberIdConverter,
					scoreConverter);
		}

	}

}
