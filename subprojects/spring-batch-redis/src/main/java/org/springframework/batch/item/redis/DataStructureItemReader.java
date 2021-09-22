package org.springframework.batch.item.redis;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.DataStructureValueReader;
import org.springframework.batch.item.redis.support.KeyValueItemReader;
import org.springframework.batch.item.redis.support.LiveKeyValueItemReader;

import java.time.Duration;

public class DataStructureItemReader extends KeyValueItemReader<DataStructure> {

    public DataStructureItemReader(ItemReader<String> keyReader, DataStructureValueReader valueReader, int threads, int chunkSize, int queueCapacity, Duration queuePollTimeout, SkipPolicy skipPolicy) {
        super(keyReader, valueReader, threads, chunkSize, queueCapacity, queuePollTimeout, skipPolicy);
    }

    public static DataStructureItemReaderBuilder client(RedisModulesClient client) {
        return new DataStructureItemReaderBuilder(client, DataStructureValueReader.client(client).build());
    }

    public static DataStructureItemReaderBuilder client(RedisModulesClusterClient client) {
        return new DataStructureItemReaderBuilder(client, DataStructureValueReader.client(client).build());
    }

    public static class DataStructureItemReaderBuilder extends KeyValueItemReaderBuilder<DataStructure, DataStructureValueReader, DataStructureItemReaderBuilder> {

        public DataStructureItemReaderBuilder(RedisModulesClient client, DataStructureValueReader valueReader) {
            super(client, valueReader);
        }

        public DataStructureItemReaderBuilder(RedisModulesClusterClient client, DataStructureValueReader valueReader) {
            super(client, valueReader);
        }

        public DataStructureItemReader build() {
            return new DataStructureItemReader(keyReader(), valueReader, threads, chunkSize, queueCapacity, queuePollTimeout, skipPolicy);
        }

        public LiveDataStructureItemReaderBuilder live() {
            if (client instanceof RedisModulesClusterClient) {
                return new LiveDataStructureItemReaderBuilder((RedisModulesClusterClient) client, valueReader);
            }
            return new LiveDataStructureItemReaderBuilder((RedisModulesClient) client, valueReader);
        }

    }

    public static class LiveDataStructureItemReaderBuilder extends LiveKeyValueItemReaderBuilder<DataStructure, DataStructureValueReader, LiveDataStructureItemReaderBuilder> {

        public LiveDataStructureItemReaderBuilder(RedisModulesClient client, DataStructureValueReader valueReader) {
            super(client, valueReader);
        }

        protected LiveDataStructureItemReaderBuilder(RedisModulesClusterClient client, DataStructureValueReader valueReader) {
            super(client, valueReader);
        }

        public LiveKeyValueItemReader<DataStructure> build() {
            return new LiveKeyValueItemReader<>(keyReader(), valueReader, threads, chunkSize, queueCapacity, queuePollTimeout, skipPolicy, flushingInterval, idleTimeout);
        }
    }

}
