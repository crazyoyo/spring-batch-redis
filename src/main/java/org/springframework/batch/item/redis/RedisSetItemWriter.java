package org.springframework.batch.item.redis;

import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractCollectionCommandItemWriter;
import org.springframework.batch.item.redis.support.AbstractCollectionCommandItemWriterBuilder;
import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

public class RedisSetItemWriter<T> extends AbstractCollectionCommandItemWriter<T> {

	public RedisSetItemWriter(GenericObjectPool<? extends StatefulConnection<String, String>> pool,
			Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> commands,
			long commandTimeout, Converter<T, String> keyConverter, Converter<T, String> memberIdConverter) {
		super(pool, commands, commandTimeout, keyConverter, memberIdConverter);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> write(BaseRedisAsyncCommands<String, String> commands, String key, String memberId,
			T item) {
		return ((RedisSetAsyncCommands<String, String>) commands).sadd(key, memberId);
	}

	public static <T> RedisSetItemWriterBuilder<T> builder() {
		return new RedisSetItemWriterBuilder<>();
	}

	@Setter
	@Accessors(fluent = true)
	public static class RedisSetItemWriterBuilder<T>
			extends AbstractCollectionCommandItemWriterBuilder<T, RedisSetItemWriterBuilder<T>> {

		public RedisSetItemWriter<T> build() {
			return new RedisSetItemWriter<>(pool(), async(), timeout(), keyConverter, memberIdConverter);
		}

	}

}
