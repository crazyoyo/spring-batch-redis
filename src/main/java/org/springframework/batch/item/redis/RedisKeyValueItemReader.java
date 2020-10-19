package org.springframework.batch.item.redis;

import java.util.List;
import java.util.function.Function;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.KeyValueItemProcessor;
import org.springframework.batch.item.redis.support.RedisItemReader;
import org.springframework.batch.item.redis.support.RedisItemReaderBuilder;
import org.springframework.batch.item.redis.support.StringChannelConverter;
import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisKeyValueItemReader<K> extends RedisItemReader<K, KeyValue<K>> {

	public RedisKeyValueItemReader(ItemReader<K> keyReader,
			ItemProcessor<List<? extends K>, List<KeyValue<K>>> valueProcessor, int threadCount, int batchSize,
			int queueCapacity, long queuePollingTimeout) {
		super(keyReader, valueProcessor, threadCount, batchSize, queueCapacity, queuePollingTimeout);
	}

	public static RedisKeyValueItemReaderBuilder<String, String> builder() {
		return new RedisKeyValueItemReaderBuilder<>(StringCodec.UTF8,
				RedisItemReaderBuilder.stringPubSubPatternProvider(), new StringChannelConverter());
	}

	public static class RedisKeyValueItemReaderBuilder<K, V>
			extends RedisItemReaderBuilder<K, V, RedisKeyValueItemReaderBuilder<K, V>, KeyValue<K>> {

		private Function<RedisKeyValueItemReaderBuilder<K, V>, K> pubSubPatternProvider;
		private Converter<K, K> keyExtractor;

		public RedisKeyValueItemReaderBuilder(RedisCodec<K, V> codec,
				Function<RedisKeyValueItemReaderBuilder<K, V>, K> pubSubPatternProvider, Converter<K, K> keyExtractor) {
			super(codec);
			this.pubSubPatternProvider = pubSubPatternProvider;
			this.keyExtractor = keyExtractor;
		}

		public RedisKeyValueItemReader<K> build() {
			return new RedisKeyValueItemReader<>(keyReader(pubSubPatternProvider, keyExtractor),
					new KeyValueItemProcessor<>(pool(), async(), uri().getTimeout()), threadCount, batchSize,
					queueCapacity, queuePollingTimeout);
		}

	}
}
