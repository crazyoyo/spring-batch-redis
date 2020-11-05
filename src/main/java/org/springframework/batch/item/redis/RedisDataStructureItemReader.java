package org.springframework.batch.item.redis;

import java.util.List;
import java.util.function.Function;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.DataStructureItemProcessor;
import org.springframework.batch.item.redis.support.Filter;
import org.springframework.batch.item.redis.support.KeyItemReader;
import org.springframework.batch.item.redis.support.KeyValueItemReader;
import org.springframework.batch.item.redis.support.KeyValueItemReaderBuilder;
import org.springframework.batch.item.redis.support.StringChannelConverter;
import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisDataStructureItemReader<K, V> extends KeyValueItemReader<K, V, DataStructure<K>> {

    public RedisDataStructureItemReader(KeyItemReader<K, V> keyReader,
	    ItemProcessor<List<? extends K>, List<DataStructure<K>>> dataStructureReader, int threadCount,
	    int batchSize, int queueCapacity, long queuePollingTimeout) {
	super(keyReader, dataStructureReader, threadCount, batchSize, queueCapacity, queuePollingTimeout);
    }

    public static RedisDataStructureItemReaderBuilder<String, String> builder() {
	return new RedisDataStructureItemReaderBuilder<>(StringCodec.UTF8,
		KeyValueItemReaderBuilder.stringPubSubPatternProvider(), KeyValueItemReaderBuilder.keyFilterProvider(),
		new StringChannelConverter());
    }

    public static class RedisDataStructureItemReaderBuilder<K, V>
	    extends KeyValueItemReaderBuilder<K, V, RedisDataStructureItemReaderBuilder<K, V>, DataStructure<K>> {

	public RedisDataStructureItemReaderBuilder(RedisCodec<K, V> codec,
		Function<RedisDataStructureItemReaderBuilder<K, V>, K> pubSubPatternProvider,
		Function<RedisDataStructureItemReaderBuilder<K, V>, Filter<K>> keyFilterProvider,
		Converter<K, K> keyExtractor) {
	    super(codec, pubSubPatternProvider, keyFilterProvider, keyExtractor);
	}

	public RedisDataStructureItemReader<K, V> build() {
	    return new RedisDataStructureItemReader<>(keyReader(pubSubPatternProvider, keyFilterProvider, keyExtractor),
		    new DataStructureItemProcessor<>(pool(), async(), uri().getTimeout()), threadCount, batchSize,
		    queueCapacity, queuePollingTimeout);
	}

    }

}
