package org.springframework.batch.item.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.DataStructureValueReader;
import org.springframework.batch.item.redis.support.LiveKeyValueItemReader;
import org.springframework.batch.item.redis.support.KeyValueItemReader;

import java.time.Duration;

public class DataStructureItemReader extends KeyValueItemReader<DataStructure> {

    public DataStructureItemReader(ItemReader<String> keyReader, DataStructureValueReader valueReader, int threads, int chunkSize, int queueCapacity, Duration queuePollTimeout) {
        super(keyReader, valueReader, threads, chunkSize, queueCapacity, queuePollTimeout);
    }

    public static DataStructureItemReaderBuilder client(RedisClient client) {
        return new DataStructureItemReaderBuilder(client, DataStructureValueReader.client(client).build());
    }

    public static DataStructureItemReaderBuilder client(RedisClusterClient client) {
        return new DataStructureItemReaderBuilder(client, DataStructureValueReader.client(client).build());
    }

    public static class DataStructureItemReaderBuilder extends KeyValueItemReaderBuilder<DataStructure, DataStructureValueReader, DataStructureItemReaderBuilder> {

        public DataStructureItemReaderBuilder(RedisClient client, DataStructureValueReader valueReader) {
            super(client, valueReader);
        }

        public DataStructureItemReaderBuilder(RedisClusterClient client, DataStructureValueReader valueReader) {
            super(client, valueReader);
        }

        public DataStructureItemReader build() {
            return new DataStructureItemReader(keyReader(), valueReader, threads, chunkSize, queueCapacity, queuePollTimeout);
        }

        public LiveDataStructureItemReaderBuilder live() {
            if (client instanceof RedisClusterClient) {
                return new LiveDataStructureItemReaderBuilder((RedisClusterClient) client, valueReader);
            }
            return new LiveDataStructureItemReaderBuilder((RedisClient) client, valueReader);
        }

    }

    public static class LiveDataStructureItemReaderBuilder extends LiveKeyValueItemReaderBuilder<DataStructure, DataStructureValueReader, LiveDataStructureItemReaderBuilder> {

        public LiveDataStructureItemReaderBuilder(RedisClient client, DataStructureValueReader valueReader) {
            super(client, valueReader);
        }

        protected LiveDataStructureItemReaderBuilder(RedisClusterClient client, DataStructureValueReader valueReader) {
            super(client, valueReader);
        }

        public LiveKeyValueItemReader<DataStructure> build() {
            return new LiveKeyValueItemReader<>(keyReader(), valueReader, threads, chunkSize, queueCapacity, queuePollTimeout, flushingInterval, idleTimeout);
        }
    }

}
