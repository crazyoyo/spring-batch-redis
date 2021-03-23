package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.AbstractKeyValueItemReader;
import org.springframework.batch.item.redis.support.KeyValue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class KeyDumpItemReader<K, V, C extends StatefulConnection<K, V>> extends AbstractKeyValueItemReader<K, V, C, KeyValue<K, byte[]>> {

    public KeyDumpItemReader(ItemReader<K> keyReader, GenericObjectPool<C> pool, Function<C, BaseRedisAsyncCommands<K, V>> commands, int chunkSize, int threads, int queueCapacity) {
        super(keyReader, pool, commands, chunkSize, threads, queueCapacity);
    }

    public KeyDumpItemReader(ItemReader<K> keyReader, GenericObjectPool<C> pool, Function<C, BaseRedisAsyncCommands<K, V>> commands, int chunkSize, int threads, int queueCapacity, Function<SimpleStepBuilder<K, K>, SimpleStepBuilder<K, K>> stepBuilderProvider) {
        super(keyReader, pool, commands, chunkSize, threads, queueCapacity, stepBuilderProvider);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<KeyValue<K, byte[]>> values(List<? extends K> keys) throws Exception {
        try (C connection = pool.borrowObject()) {
            BaseRedisAsyncCommands<K, V> asyncCommands = commands.apply(connection);
            asyncCommands.setAutoFlushCommands(false);
            List<RedisFuture<Long>> ttlFutures = new ArrayList<>(keys.size());
            List<RedisFuture<byte[]>> dumpFutures = new ArrayList<>(keys.size());
            for (K key : keys) {
                ttlFutures.add(((RedisKeyAsyncCommands<K, V>) asyncCommands).ttl(key));
                dumpFutures.add(((RedisKeyAsyncCommands<K, V>) asyncCommands).dump(key));
            }
            asyncCommands.flushCommands();
            List<KeyValue<K, byte[]>> dumps = new ArrayList<>(keys.size());
            long commandTimeout = connection.getTimeout().toMillis();
            try {
                for (int index = 0; index < keys.size(); index++) {
                    K key = keys.get(index);
                    Long ttl = ttlFutures.get(index).get(commandTimeout, TimeUnit.MILLISECONDS);
                    byte[] dump = dumpFutures.get(index).get(commandTimeout, TimeUnit.MILLISECONDS);
                    dumps.add(new KeyValue<>(key, ttl, dump));
                }
                return dumps;
            } finally {
                asyncCommands.setAutoFlushCommands(true);
            }
        }
    }


}
