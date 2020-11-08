package org.springframework.batch.item.redis;

import java.util.List;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.DumpReader;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.KeyValueItemReader;
import org.springframework.batch.item.redis.support.KeyValueItemReaderBuilder;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisDumpItemReader<K, V> extends KeyValueItemReader<K, V, KeyValue<K, byte[]>> {

    public RedisDumpItemReader(ItemReader<K> keyReader,
	    ItemProcessor<List<? extends K>, List<KeyValue<K, byte[]>>> valueReader, int threadCount, int batchSize,
	    int queueCapacity, long queuePollingTimeout) {
	super(keyReader, valueReader, threadCount, batchSize, queueCapacity, queuePollingTimeout);
    }

    public static RedisDumpItemReaderBuilder<String, String> builder() {
	return new RedisDumpItemReaderBuilder<>(StringCodec.UTF8);
    }

    public static class RedisDumpItemReaderBuilder<K, V>
	    extends KeyValueItemReaderBuilder<K, V, RedisDumpItemReaderBuilder<K, V>, KeyValue<String, byte[]>> {

	public RedisDumpItemReaderBuilder(RedisCodec<K, V> codec) {
	    super(codec);
	}

	public RedisDumpItemReader<K, V> build() {
	    DumpReader<K, V> valueReader = new DumpReader<>(pool(), async(), timeout());
	    return new RedisDumpItemReader<>(keyReader, valueReader, threadCount, batchSize, queueCapacity,
		    queuePollingTimeout);
	}

    }

}
