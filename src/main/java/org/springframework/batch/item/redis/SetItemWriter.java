package org.springframework.batch.item.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.support.AbstractCollectionCommandItemWriter;
import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

public class SetItemWriter<T> extends AbstractCollectionCommandItemWriter<T> {

	public SetItemWriter(AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig, Converter<T, String> keyConverter,
			Converter<T, String> memberIdConverter) {
		super(client, poolConfig, keyConverter, memberIdConverter);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> write(BaseRedisAsyncCommands<String, String> commands, String key, String memberId,
			T item) {
		return ((RedisSetAsyncCommands<String, String>) commands).sadd(key, memberId);
	}

	public static <T> SetItemWriterBuilder<T> builder() {
		return new SetItemWriterBuilder<>();
	}

	@Setter
	@Accessors(fluent = true)
	public static class SetItemWriterBuilder<T>
			extends AbstractCollectionCommandItemWriterBuilder<T, SetItemWriterBuilder<T>> {

		public SetItemWriter<T> build() {
			return new SetItemWriter<>(client, poolConfig, keyConverter, memberIdConverter);
		}

	}

}
