package org.springframework.batch.item.redis;

import java.util.List;
import java.util.function.Function;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.redis.support.DumpItemProcessor;
import org.springframework.batch.item.redis.support.Filter;
import org.springframework.batch.item.redis.support.KeyItemReader;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.KeyValueItemReader;
import org.springframework.batch.item.redis.support.KeyValueItemReaderBuilder;
import org.springframework.batch.item.redis.support.StringChannelConverter;
import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisDumpItemReader<K, V> extends KeyValueItemReader<K, V, KeyValue<K, byte[]>> {

    public RedisDumpItemReader(KeyItemReader<K, V> keyReader,
	    ItemProcessor<List<? extends K>, List<KeyValue<K, byte[]>>> valueReader, int threadCount, int batchSize,
	    int queueCapacity, long queuePollingTimeout) {
	super(keyReader, valueReader, threadCount, batchSize, queueCapacity, queuePollingTimeout);
    }

    public static RedisDumpItemReaderBuilder<String, String> builder() {
	return new RedisDumpItemReaderBuilder<>(StringCodec.UTF8,
		KeyValueItemReaderBuilder.stringPubSubPatternProvider(), KeyValueItemReaderBuilder.keyFilterProvider(),
		new StringChannelConverter());
    }

    public static class RedisDumpItemReaderBuilder<K, V>
	    extends KeyValueItemReaderBuilder<K, V, RedisDumpItemReaderBuilder<K, V>, KeyValue<String, byte[]>> {

	public RedisDumpItemReaderBuilder(RedisCodec<K, V> codec,
		Function<RedisDumpItemReaderBuilder<K, V>, K> pubSubPatternProvider,
		Function<RedisDumpItemReaderBuilder<K, V>, Filter<K>> keyFilterProvider, Converter<K, K> keyExtractor) {
	    super(codec, pubSubPatternProvider, keyFilterProvider, keyExtractor);
	}

	public RedisDumpItemReader<K, V> build() {
	    return new RedisDumpItemReader<>(keyReader(pubSubPatternProvider, keyFilterProvider, keyExtractor),
		    new DumpItemProcessor<>(pool(), async(), uri().getTimeout()), threadCount, batchSize, queueCapacity,
		    queuePollingTimeout);
	}

    }

}
