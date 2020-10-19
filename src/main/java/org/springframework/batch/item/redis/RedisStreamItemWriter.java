package org.springframework.batch.item.redis;

import org.springframework.batch.item.redis.support.AbstractRedisItemWriter;
import org.springframework.batch.item.redis.support.RedisItemWriterBuilder;
import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import lombok.Setter;
import lombok.experimental.Accessors;

public class RedisStreamItemWriter<K, V> extends AbstractRedisItemWriter<K, V, StreamMessage<K, V>> {

	private final Converter<StreamMessage<K, V>, XAddArgs> converter;

	public RedisStreamItemWriter(Converter<StreamMessage<K, V>, XAddArgs> converter) {
		this.converter = converter;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, StreamMessage<K, V> item) {
		RedisStreamAsyncCommands<K, V> streamCommands = (RedisStreamAsyncCommands<K, V>) commands;
		if (converter == null) {
			return streamCommands.xadd(item.getStream(), item.getBody());
		}
		return streamCommands.xadd(item.getStream(), converter.convert(item), item.getBody());
	}

	public static RedisStreamItemWriterBuilder<String, String> builder() {
		return new RedisStreamItemWriterBuilder<>(StringCodec.UTF8);
	}

	@Setter
	@Accessors(fluent = true)
	public static class RedisStreamItemWriterBuilder<K, V>
			extends RedisItemWriterBuilder<K, V, RedisStreamItemWriterBuilder<K, V>> {

		private Converter<StreamMessage<K, V>, XAddArgs> converter;

		public RedisStreamItemWriterBuilder(RedisCodec<K, V> codec) {
			super(codec);
		}

		public RedisStreamItemWriter<K, V> build() {
			return configure(new RedisStreamItemWriter<>(converter));
		}

	}

}
