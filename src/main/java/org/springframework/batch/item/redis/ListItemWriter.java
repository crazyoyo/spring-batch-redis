package org.springframework.batch.item.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.support.AbstractCollectionCommandItemWriter;
import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

public class ListItemWriter<T> extends AbstractCollectionCommandItemWriter<T> {

	private final boolean lpush;

	public ListItemWriter(AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig, Converter<T, String> keyConverter,
			Converter<T, String> memberIdConverter, boolean lpush) {
		super(client, poolConfig, keyConverter, memberIdConverter);
		this.lpush = lpush;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected RedisFuture<?> write(BaseRedisAsyncCommands<String, String> commands, String key, String memberId,
			T item) {
		RedisListAsyncCommands<String, String> listCommands = (RedisListAsyncCommands<String, String>) commands;
		return lpush ? listCommands.lpush(key, memberId) : listCommands.rpush(key, memberId);
	}

	public static <T> ListItemWriterBuilder<T> builder(AbstractRedisClient client) {
		return new ListItemWriterBuilder<>(client);
	}

	@Setter
	@Accessors(fluent = true)
	public static class ListItemWriterBuilder<T>
			extends AbstractCollectionCommandItemWriterBuilder<T, ListItemWriterBuilder<T>> {

		protected ListItemWriterBuilder(AbstractRedisClient client) {
			super(client);
		}

		public enum PushDirection {
			LEFT, RIGHT
		}

		private PushDirection direction = PushDirection.LEFT;

		public ListItemWriter<T> build() {
			return new ListItemWriter<>(client, poolConfig, keyConverter, memberIdConverter,
					direction == PushDirection.LEFT);
		}

	}

}
