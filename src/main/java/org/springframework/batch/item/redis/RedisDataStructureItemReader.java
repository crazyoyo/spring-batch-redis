package org.springframework.batch.item.redis;

import java.util.List;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.DataStructureReader;
import org.springframework.batch.item.redis.support.KeyValueItemReader;
import org.springframework.batch.item.redis.support.KeyValueItemReaderBuilder;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisDataStructureItemReader<K, V> extends KeyValueItemReader<K, V, DataStructure<K>> {

    public RedisDataStructureItemReader(ItemReader<K> keyReader,
	    ItemProcessor<List<? extends K>, List<DataStructure<K>>> valueReader, int threadCount, int batchSize,
	    int queueCapacity, long queuePollingTimeout) {
	super(keyReader, valueReader, threadCount, batchSize, queueCapacity, queuePollingTimeout);
    }

    public static RedisDataStructureItemReaderBuilder<String, String> builder() {
	return new RedisDataStructureItemReaderBuilder<>(StringCodec.UTF8);
    }

    public static class RedisDataStructureItemReaderBuilder<K, V>
	    extends KeyValueItemReaderBuilder<K, V, RedisDataStructureItemReaderBuilder<K, V>, DataStructure<K>> {

	public RedisDataStructureItemReaderBuilder(RedisCodec<K, V> codec) {
	    super(codec);
	}

	public RedisDataStructureItemReader<K, V> build() {
	    DataStructureReader<K, V> valueReader = new DataStructureReader<>(pool(), async(), timeout());
	    return new RedisDataStructureItemReader<>(keyReader, valueReader, threadCount, batchSize, queueCapacity,
		    queuePollingTimeout);
	}

    }

}
