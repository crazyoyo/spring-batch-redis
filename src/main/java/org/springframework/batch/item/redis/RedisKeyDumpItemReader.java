package org.springframework.batch.item.redis;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.*;

import java.util.List;

public class RedisKeyDumpItemReader<K> extends RedisItemReader<K, KeyDump<K>> {

    public RedisKeyDumpItemReader(ItemReader<K> keyReader, ItemProcessor<List<? extends K>, List<KeyDump<K>>> valueProcessor, int threadCount, int batchSize, int queueCapacity, long queuePollingTimeout) {
        super(keyReader, valueProcessor, threadCount, batchSize, queueCapacity, queuePollingTimeout);
    }

    public static RedisKeyDumpItemReaderBuilder builder() {
        return new RedisKeyDumpItemReaderBuilder();
    }

    public static class RedisKeyDumpItemReaderBuilder extends RedisItemReaderBuilder<RedisKeyDumpItemReaderBuilder, KeyDump<String>> {

        public RedisKeyDumpItemReader<String> build() {
            return new RedisKeyDumpItemReader<>(keyReader(), new KeyDumpItemProcessor<>(pool(), async(), timeout()), threadCount, batchSize, queueCapacity, queuePollingTimeout);
        }
    }
}
