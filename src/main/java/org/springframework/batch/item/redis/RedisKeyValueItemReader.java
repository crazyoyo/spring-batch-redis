package org.springframework.batch.item.redis;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.*;

import java.util.List;

public class RedisKeyValueItemReader<K> extends RedisItemReader<K, KeyValue<K>> {

    public RedisKeyValueItemReader(ItemReader<K> keyReader, ItemProcessor<List<? extends K>, List<KeyValue<K>>> valueProcessor, int threadCount, int batchSize, int queueCapacity, long queuePollingTimeout) {
        super(keyReader, valueProcessor, threadCount, batchSize, queueCapacity, queuePollingTimeout);
    }

    public static RedisKeyValueItemReaderBuilder builder() {
        return new RedisKeyValueItemReaderBuilder();
    }

    public static class RedisKeyValueItemReaderBuilder extends RedisItemReaderBuilder<RedisKeyValueItemReaderBuilder, KeyValue<String>> {

        public KeyValueItemProcessor<String, String> keyValueProcessor() {
            return new KeyValueItemProcessor<>(pool(), async(), getTimeout());
        }

        public RedisKeyValueItemReader<String> build() {
            return new RedisKeyValueItemReader<>(keyReader(), keyValueProcessor(), threadCount, batchSize, queueCapacity, queuePollingTimeout);
        }

    }
}
