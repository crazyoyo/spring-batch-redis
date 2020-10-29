package org.springframework.batch.item.redis;

import java.util.List;
import java.util.function.Function;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.KeyDump;
import org.springframework.batch.item.redis.support.KeyDumpItemProcessor;
import org.springframework.batch.item.redis.support.KeyValueItemReader;
import org.springframework.batch.item.redis.support.KeyValueItemReaderBuilder;
import org.springframework.batch.item.redis.support.StringChannelConverter;
import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisDumpItemReader<K> extends KeyValueItemReader<K, KeyDump<K>> {

	public RedisDumpItemReader(ItemReader<K> keyReader,
			ItemProcessor<List<? extends K>, List<KeyDump<K>>> valueProcessor, int threadCount, int batchSize,
			int queueCapacity, long queuePollingTimeout) {
		super(keyReader, valueProcessor, threadCount, batchSize, queueCapacity, queuePollingTimeout);
	}

	public static RedisKeyDumpItemReaderBuilder<String, String> builder() {
		return new RedisKeyDumpItemReaderBuilder<>(StringCodec.UTF8,
				KeyValueItemReaderBuilder.stringPubSubPatternProvider(), new StringChannelConverter());
	}

	public static class RedisKeyDumpItemReaderBuilder<K, V>
			extends KeyValueItemReaderBuilder<K, V, RedisKeyDumpItemReaderBuilder<K, V>, KeyDump<String>> {

		private Function<RedisKeyDumpItemReaderBuilder<K, V>, K> pubSubPatternProvider;
		private Converter<K, K> keyExtractor;

		public RedisKeyDumpItemReaderBuilder(RedisCodec<K, V> codec,
				Function<RedisKeyDumpItemReaderBuilder<K, V>, K> pubSubPatternProvider, Converter<K, K> keyExtractor) {
			super(codec);
			this.pubSubPatternProvider = pubSubPatternProvider;
			this.keyExtractor = keyExtractor;
		}

		public RedisDumpItemReader<K> build() {
			return new RedisDumpItemReader<>(keyReader(pubSubPatternProvider, keyExtractor),
					new KeyDumpItemProcessor<>(pool(), async(), uri().getTimeout()), threadCount, batchSize,
					queueCapacity, queuePollingTimeout);
		}
	}
}
