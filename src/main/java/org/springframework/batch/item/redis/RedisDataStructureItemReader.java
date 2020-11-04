package org.springframework.batch.item.redis;

import java.util.List;
import java.util.function.Function;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.DataStructureItemProcessor;
import org.springframework.batch.item.redis.support.KeyValueItemReader;
import org.springframework.batch.item.redis.support.KeyValueItemReaderBuilder;
import org.springframework.batch.item.redis.support.StringChannelConverter;
import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisDataStructureItemReader<K> extends KeyValueItemReader<K, DataStructure<K>> {

    public RedisDataStructureItemReader(ItemReader<K> keyReader,
	    ItemProcessor<List<? extends K>, List<DataStructure<K>>> valueProcessor, int threadCount, int batchSize,
	    int queueCapacity, long queuePollingTimeout) {
	super(keyReader, valueProcessor, threadCount, batchSize, queueCapacity, queuePollingTimeout);
    }

    public static RedisDataStructureItemReaderBuilder<String, String> builder() {
	return new RedisDataStructureItemReaderBuilder<>(StringCodec.UTF8,
		KeyValueItemReaderBuilder.stringPubSubPatternProvider(), new StringChannelConverter());
    }

    public static class RedisDataStructureItemReaderBuilder<K, V>
	    extends KeyValueItemReaderBuilder<K, V, RedisDataStructureItemReaderBuilder<K, V>, DataStructure<K>> {

	private Function<RedisDataStructureItemReaderBuilder<K, V>, K> pubSubPatternProvider;

	private Converter<K, K> keyExtractor;

	public RedisDataStructureItemReaderBuilder(RedisCodec<K, V> codec,
		Function<RedisDataStructureItemReaderBuilder<K, V>, K> pubSubPatternProvider,
		Converter<K, K> keyExtractor) {
	    super(codec);
	    this.pubSubPatternProvider = pubSubPatternProvider;
	    this.keyExtractor = keyExtractor;
	}

	public RedisDataStructureItemReader<K> build() {
	    return new RedisDataStructureItemReader<>(keyReader(pubSubPatternProvider, keyExtractor),
		    new DataStructureItemProcessor<>(pool(), async(), uri().getTimeout()), threadCount, batchSize,
		    queueCapacity, queuePollingTimeout);
	}

    }

}
