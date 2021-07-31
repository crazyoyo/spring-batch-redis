package org.springframework.batch.item.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.*;

import java.time.Duration;

public class KeyDumpItemReader extends KeyValueItemReader<KeyValue<byte[]>> {

    public KeyDumpItemReader(ItemReader<String> keyReader, KeyDumpValueReader valueReader, int threads, int chunkSize, int queueCapacity, Duration queuePollTimeout) {
        super(keyReader, valueReader, threads, chunkSize, queueCapacity, queuePollTimeout);
    }

    public static KeyDumpItemReaderBuilder client(RedisClient client) {
        return new KeyDumpItemReaderBuilder(client, KeyDumpValueReader.client(client).build());
    }

    public static KeyDumpItemReaderBuilder client(RedisClusterClient client) {
        return new KeyDumpItemReaderBuilder(client, KeyDumpValueReader.client(client).build());
    }

    public static class KeyDumpItemReaderBuilder extends KeyValueItemReaderBuilder<KeyValue<byte[]>, KeyDumpValueReader, KeyDumpItemReaderBuilder> {

        public KeyDumpItemReaderBuilder(RedisClient client, KeyDumpValueReader valueReader) {
            super(client, valueReader);
        }

        public KeyDumpItemReaderBuilder(RedisClusterClient client, KeyDumpValueReader valueReader) {
            super(client, valueReader);
        }

        public KeyDumpItemReader build() {
            return new KeyDumpItemReader(keyReader(), valueReader, threads, chunkSize, queueCapacity, queuePollTimeout);
        }

        public LiveKeyDumpItemReaderBuilder live() {
            if (client instanceof RedisClusterClient) {
                return new LiveKeyDumpItemReaderBuilder((RedisClusterClient) client, valueReader);
            }
            return new LiveKeyDumpItemReaderBuilder((RedisClient) client, valueReader);
        }

    }

    public static class LiveKeyDumpItemReaderBuilder extends LiveKeyValueItemReaderBuilder<KeyValue<byte[]>, KeyDumpValueReader, LiveKeyDumpItemReaderBuilder> {


        public LiveKeyDumpItemReaderBuilder(RedisClient client, KeyDumpValueReader valueReader) {
            super(client, valueReader);
        }

        protected LiveKeyDumpItemReaderBuilder(RedisClusterClient client, KeyDumpValueReader valueReader) {
            super(client, valueReader);
        }

        public LiveKeyValueItemReader<KeyValue<byte[]>> build() {
            return new LiveKeyValueItemReader<>(keyReader(), valueReader, threads, chunkSize, queueCapacity, queuePollTimeout, flushingInterval, idleTimeout);
        }
    }

}
