package org.springframework.batch.item.redis;

import com.redislabs.lettuce.helper.RedisOptions;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.AbstractRedisItemReader;
import org.springframework.batch.item.redis.support.ReaderOptions;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@Slf4j
public class RedisKeyDumpItemReader<K, V> extends AbstractRedisItemReader<K, V, KeyDump<K>> {

    public RedisKeyDumpItemReader(ItemReader<K> keyReader, GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, int threadCount, int batchSize, Duration commandTimeout, int queueCapacity, long queuePollingTimeout) {
        super(keyReader, pool, commands, threadCount, batchSize, commandTimeout, queueCapacity, queuePollingTimeout);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected List<KeyDump<K>> values(List<? extends K> keys, BaseRedisAsyncCommands<K, V> commands) {
        List<RedisFuture<Long>> ttlFutures = new ArrayList<>(keys.size());
        List<RedisFuture<byte[]>> dumpFutures = new ArrayList<>(keys.size());
        for (K key : keys) {
            ttlFutures.add(((RedisKeyAsyncCommands<K, V>) commands).ttl(key));
            dumpFutures.add(((RedisKeyAsyncCommands<K, V>) commands).dump(key));
        }
        commands.flushCommands();
        List<KeyDump<K>> dumps = new ArrayList<>(keys.size());
        for (int index = 0; index < keys.size(); index++) {
            KeyDump<K> dump = new KeyDump<>();
            dump.setKey(keys.get(index));
            try {
                dump.setTtl(getTtl(ttlFutures.get(index)));
                dump.setValue(get(dumpFutures.get(index)));
            } catch (Exception e) {
                log.error("Could not get value", e);
            }
            dumps.add(dump);
        }
        return dumps;
    }

    public static RedisKeyDumpItemReaderBuilder builder() {
        return new RedisKeyDumpItemReaderBuilder();
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisKeyDumpItemReaderBuilder extends AbstractRedisItemReaderBuilder {

        private RedisOptions redisOptions = RedisOptions.builder().build();
        private ReaderOptions readerOptions = ReaderOptions.builder().build();

        public RedisKeyDumpItemReader<String, String> build() {
            Assert.notNull(redisOptions, "Redis options are required.");
            Assert.notNull(readerOptions, "Reader options are required.");
            return new RedisKeyDumpItemReader<>(keyReader(redisOptions, readerOptions), redisOptions.connectionPool(), redisOptions.async(), readerOptions.getThreadCount(), readerOptions.getBatchSize(), redisOptions.getTimeout(), readerOptions.getValueQueueOptions().getCapacity(), readerOptions.getValueQueueOptions().getPollingTimeout());
        }

    }

}
