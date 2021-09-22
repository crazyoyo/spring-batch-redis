package org.springframework.batch.item.redis;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.KeyDumpValueReader;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.KeyValueItemReader;
import org.springframework.batch.item.redis.support.LiveKeyValueItemReader;

import java.time.Duration;

public class KeyDumpItemReader extends KeyValueItemReader<KeyValue<byte[]>> {

    public KeyDumpItemReader(ItemReader<String> keyReader, KeyDumpValueReader valueReader, int threads, int chunkSize, int queueCapacity, Duration queuePollTimeout, SkipPolicy skipPolicy) {
        super(keyReader, valueReader, threads, chunkSize, queueCapacity, queuePollTimeout, skipPolicy);
    }

    public static KeyDumpItemReaderBuilder client(RedisModulesClient client) {
        return new KeyDumpItemReaderBuilder(client, KeyDumpValueReader.client(client).build());
    }

    public static KeyDumpItemReaderBuilder client(RedisModulesClusterClient client) {
        return new KeyDumpItemReaderBuilder(client, KeyDumpValueReader.client(client).build());
    }

    public static class KeyDumpItemReaderBuilder extends KeyValueItemReaderBuilder<KeyValue<byte[]>, KeyDumpValueReader, KeyDumpItemReaderBuilder> {

        public KeyDumpItemReaderBuilder(RedisModulesClient client, KeyDumpValueReader valueReader) {
            super(client, valueReader);
        }

        public KeyDumpItemReaderBuilder(RedisModulesClusterClient client, KeyDumpValueReader valueReader) {
            super(client, valueReader);
        }

        public KeyDumpItemReader build() {
            return new KeyDumpItemReader(keyReader(), valueReader, threads, chunkSize, queueCapacity, queuePollTimeout, skipPolicy);
        }

        public LiveKeyDumpItemReaderBuilder live() {
            if (client instanceof RedisModulesClusterClient) {
                return new LiveKeyDumpItemReaderBuilder((RedisModulesClusterClient) client, valueReader);
            }
            return new LiveKeyDumpItemReaderBuilder((RedisModulesClient) client, valueReader);
        }

    }

    public static class LiveKeyDumpItemReaderBuilder extends LiveKeyValueItemReaderBuilder<KeyValue<byte[]>, KeyDumpValueReader, LiveKeyDumpItemReaderBuilder> {


        public LiveKeyDumpItemReaderBuilder(RedisModulesClient client, KeyDumpValueReader valueReader) {
            super(client, valueReader);
        }

        protected LiveKeyDumpItemReaderBuilder(RedisModulesClusterClient client, KeyDumpValueReader valueReader) {
            super(client, valueReader);
        }

        public LiveKeyValueItemReader<KeyValue<byte[]>> build() {
            return new LiveKeyValueItemReader<>(keyReader(), valueReader, threads, chunkSize, queueCapacity, queuePollTimeout, skipPolicy, flushingInterval, idleTimeout);
        }
    }

}
