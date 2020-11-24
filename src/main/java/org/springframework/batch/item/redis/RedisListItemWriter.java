package org.springframework.batch.item.redis;

import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractCollectionCommandItemWriter;
import org.springframework.batch.item.redis.support.AbstractCollectionCommandItemWriterBuilder;
import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

public class RedisListItemWriter<T> extends AbstractCollectionCommandItemWriter<T> {

	private final boolean lpush;

	public RedisListItemWriter(GenericObjectPool<? extends StatefulConnection<String, String>> pool,
			Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> commands,
			long commandTimeout, Converter<T, String> keyConverter, Converter<T, String> memberIdConverter,
			boolean lpush) {
		super(pool, commands, commandTimeout, keyConverter, memberIdConverter);
		this.lpush = lpush;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected RedisFuture<?> write(BaseRedisAsyncCommands<String, String> commands, String key, String memberId,
			T item) {
		RedisListAsyncCommands<String, String> listCommands = (RedisListAsyncCommands<String, String>) commands;
		return lpush ? listCommands.lpush(key, memberId) : listCommands.rpush(key, memberId);
	}

	public static <T> RedisListItemWriterBuilder<T> builder() {
		return new RedisListItemWriterBuilder<>();
	}

	@Setter
	@Accessors(fluent = true)
	public static class RedisListItemWriterBuilder<T>
			extends AbstractCollectionCommandItemWriterBuilder<T, RedisListItemWriterBuilder<T>> {

		public enum PushDirection {
			LEFT, RIGHT
		}

		private PushDirection direction = PushDirection.LEFT;

		public RedisListItemWriter<T> build() {
			return new RedisListItemWriter<>(pool(), async(), timeout(), keyConverter, memberIdConverter,
					direction == PushDirection.LEFT);
		}

	}

}
