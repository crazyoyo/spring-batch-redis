package org.springframework.batch.item.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.support.AbstractCollectionCommandItemWriter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

public class SortedSetItemWriter<T> extends AbstractCollectionCommandItemWriter<T> {

	private final Converter<T, Double> scoreConverter;

	public SortedSetItemWriter(AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig, Converter<T, String> keyConverter,
			Converter<T, String> memberIdConverter, Converter<T, Double> scoreConverter) {
		super(client, poolConfig, keyConverter, memberIdConverter);
		Assert.notNull(scoreConverter, "A score converter is required.");
		this.scoreConverter = scoreConverter;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> write(BaseRedisAsyncCommands<String, String> commands, String key, String memberId,
			T item) {
		Double score = scoreConverter.convert(item);
		if (score == null) {
			return null;
		}
		return ((RedisSortedSetAsyncCommands<String, String>) commands).zadd(key, score, memberId);
	}

	public static <T> SortedSetItemWriterBuilder<T> builder(AbstractRedisClient client) {
		return new SortedSetItemWriterBuilder<>(client);
	}

	@Setter
	@Accessors(fluent = true)
	public static class SortedSetItemWriterBuilder<T>
			extends AbstractCollectionCommandItemWriterBuilder<T, SortedSetItemWriterBuilder<T>> {

		protected SortedSetItemWriterBuilder(AbstractRedisClient client) {
			super(client);
		}

		private Converter<T, Double> scoreConverter;

		public SortedSetItemWriter<T> build() {
			return new SortedSetItemWriter<>(client, poolConfig, keyConverter, memberIdConverter, scoreConverter);
		}

	}

}
