package org.springframework.batch.item.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.cluster.RedisClusterClient;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.RedisClusterKeyValueItemProcessor;
import org.springframework.batch.item.redis.support.RedisKeyValueItemProcessor;
import org.springframework.batch.item.redis.support.RedisOptions;

import java.util.List;

public class RedisKeyValueItemReader<K> extends RedisItemReader<K, KeyValue<K>> {

    protected RedisKeyValueItemReader(ItemReader<K> keyReader, ItemProcessor<List<? extends K>, List<? extends KeyValue<K>>> valueReader, Options options) {
        super(keyReader, valueReader, options);
    }

    public static RedisKeyValueItemReaderBuilder builder() {
        return new RedisKeyValueItemReaderBuilder();
    }

    @Accessors(fluent = true)
    @Setter
    public static class RedisKeyValueItemReaderBuilder extends AbstractRedisItemReaderBuilder {

        private RedisOptions redisOptions = DEFAULT_REDIS_OPTIONS;
        private Options options = DEFAULT_OPTIONS;
        private ScanArgs scanArgs = DEFAULT_SCAN_OPTIONS;
        private Mode mode = DEFAULT_MODE;

        public RedisKeyValueItemReader<String> build() {
            if (redisOptions.isCluster()) {
                RedisClusterClient client = redisOptions.redisClusterClient();
                return new RedisKeyValueItemReader<>(keyReader(mode, client, redisOptions, options, scanArgs), new RedisClusterKeyValueItemProcessor<>(redisOptions.redisClusterConnectionPool(client), redisOptions.getCommandTimeout()), options);
            }
            RedisClient client = redisOptions.redisClient();
            return new RedisKeyValueItemReader<>(keyReader(mode, client, redisOptions, options, scanArgs), new RedisKeyValueItemProcessor<>(redisOptions.redisConnectionPool(client), redisOptions.getCommandTimeout()), options);
        }

    }
}
