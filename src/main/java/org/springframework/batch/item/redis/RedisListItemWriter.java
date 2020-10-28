package org.springframework.batch.item.redis;

import java.time.Duration;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractCollectionCommandItemWriter;
import org.springframework.batch.item.redis.support.AbstractCollectionCommandItemWriterBuilder;
import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import lombok.Setter;
import lombok.experimental.Accessors;

public class RedisListItemWriter<K, V, T> extends AbstractCollectionCommandItemWriter<K, V, T> {

	private final boolean lpush;

	public RedisListItemWriter(GenericObjectPool<? extends StatefulConnection<K, V>> pool,
			Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout,
			Converter<T, K> keyConverter, Converter<T, V> memberIdConverter, boolean lpush) {
		super(pool, commands, commandTimeout, keyConverter, memberIdConverter);
		this.lpush = lpush;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, K key, V memberId, T item) {
		RedisListAsyncCommands<K, V> listCommands = (RedisListAsyncCommands<K, V>) commands;
		return lpush ? listCommands.lpush(key, memberId) : listCommands.rpush(key, memberId);
	}

	public static <T> RedisListItemWriterBuilder<String, String, T> builder() {
		return new RedisListItemWriterBuilder<>(StringCodec.UTF8);
	}

	@Setter
	@Accessors(fluent = true)
	public static class RedisListItemWriterBuilder<K, V, T>
			extends AbstractCollectionCommandItemWriterBuilder<K, V, T, RedisListItemWriterBuilder<K, V, T>> {

		public enum PushDirection {
			LEFT, RIGHT
		}

		private PushDirection direction = PushDirection.LEFT;

		public RedisListItemWriterBuilder(RedisCodec<K, V> codec) {
			super(codec);
		}

		public RedisListItemWriter<K, V, T> build() {
			return new RedisListItemWriter<>(pool(), async(), timeout(), keyConverter, memberIdConverter,
					direction == PushDirection.LEFT);
		}

	}

}
