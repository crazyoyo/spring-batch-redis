package org.springframework.batch.item.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.cluster.RedisClusterClient;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.KeyDump;
import org.springframework.batch.item.redis.support.RedisClusterKeyDumpItemProcessor;
import org.springframework.batch.item.redis.support.RedisKeyDumpItemProcessor;
import org.springframework.batch.item.redis.support.RedisOptions;

import java.util.List;

public class RedisKeyDumpItemReader<K> extends RedisItemReader<K, KeyDump<K>> {

    protected RedisKeyDumpItemReader(ItemReader<K> keyReader, ItemProcessor<List<? extends K>, List<? extends KeyDump<K>>> valueReader, Options options) {
        super(keyReader, valueReader, options);
    }

    public static RedisKeyDumpItemReaderBuilder builder() {
        return new RedisKeyDumpItemReaderBuilder();
    }

    @Accessors(fluent = true)
    @Setter
    public static class RedisKeyDumpItemReaderBuilder extends AbstractRedisItemReaderBuilder {

        private RedisOptions redisOptions = DEFAULT_REDIS_OPTIONS;
        private Options options = DEFAULT_OPTIONS;
        private ScanArgs scanArgs = DEFAULT_SCAN_OPTIONS;
        private Mode mode = DEFAULT_MODE;

        public RedisKeyDumpItemReader<String> build() {
            if (redisOptions.isCluster()) {
                RedisClusterClient client = redisOptions.redisClusterClient();
                return new RedisKeyDumpItemReader<>(keyReader(mode, client, redisOptions, options, scanArgs), new RedisClusterKeyDumpItemProcessor<>(redisOptions.redisClusterConnectionPool(client), redisOptions.getCommandTimeout()), options);
            }
            RedisClient client = redisOptions.redisClient();
            return new RedisKeyDumpItemReader<>(keyReader(mode, client, redisOptions, options, scanArgs), new RedisKeyDumpItemProcessor<>(redisOptions.redisConnectionPool(client), redisOptions.getCommandTimeout()), options);
        }

    }


}
