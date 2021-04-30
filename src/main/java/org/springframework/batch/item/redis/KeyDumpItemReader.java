package org.springframework.batch.item.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.*;

import java.time.Duration;

public class KeyDumpItemReader<K, V> extends KeyValueItemReader<K, KeyValue<K, byte[]>> {

    public KeyDumpItemReader(ItemReader<K> keyReader, KeyDumpValueReader<K, V> valueReader, int threads, int chunkSize, int queueCapacity, Duration queuePollTimeout) {
        super(keyReader, valueReader, threads, chunkSize, queueCapacity, queuePollTimeout);
    }

    public static KeyDumpItemReaderBuilder client(RedisClient client) {
        return new KeyDumpItemReaderBuilder(client, KeyDumpValueReader.client(client).build());
    }

    public static KeyDumpItemReaderBuilder client(RedisClusterClient client) {
        return new KeyDumpItemReaderBuilder(client, KeyDumpValueReader.client(client).build());
    }

    public static class KeyDumpItemReaderBuilder extends KeyValueItemReaderBuilder<KeyValue<String, byte[]>, KeyDumpValueReader<String, String>, KeyDumpItemReaderBuilder> {

        public KeyDumpItemReaderBuilder(RedisClient client, KeyDumpValueReader<String, String> valueReader) {
            super(client, valueReader);
        }

        public KeyDumpItemReaderBuilder(RedisClusterClient client, KeyDumpValueReader<String, String> valueReader) {
            super(client, valueReader);
        }

        public KeyDumpItemReader<String, String> build() {
            return new KeyDumpItemReader<>(keyReader(), valueReader, threads, chunkSize, queueCapacity, queuePollTimeout);
        }

        public LiveKeyDumpItemReaderBuilder live() {
            if (client instanceof RedisClusterClient) {
                return new LiveKeyDumpItemReaderBuilder((RedisClusterClient) client, valueReader);
            }
            return new LiveKeyDumpItemReaderBuilder((RedisClient) client, valueReader);
        }

    }

    public static class LiveKeyDumpItemReaderBuilder extends LiveKeyValueItemReaderBuilder<KeyValue<String, byte[]>, KeyDumpValueReader<String, String>, LiveKeyDumpItemReaderBuilder> {


        public LiveKeyDumpItemReaderBuilder(RedisClient client, KeyDumpValueReader<String, String> valueReader) {
            super(client, valueReader);
        }

        protected LiveKeyDumpItemReaderBuilder(RedisClusterClient client, KeyDumpValueReader<String, String> valueReader) {
            super(client, valueReader);
        }

        public LiveKeyValueItemReader<String, KeyValue<String, byte[]>> build() {
            return new LiveKeyValueItemReader<>(keyReader(), valueReader, threads, chunkSize, queueCapacity, queuePollTimeout, flushingInterval, idleTimeout);
        }
    }

}
