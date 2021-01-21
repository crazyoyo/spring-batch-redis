package org.springframework.batch.item.redis;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.AbstractKeyValueItemReader;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.LiveKeyValueItemReaderBuilder;
import org.springframework.batch.item.redis.support.ScanKeyValueItemReaderBuilder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

public class KeyDumpItemReader<K, V> extends AbstractKeyValueItemReader<K, V, KeyValue<K, byte[]>> {

    public KeyDumpItemReader(ItemReader<K> keyReader, GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout, int chunkSize, int threads, int queueCapacity) {
        super(keyReader, pool, commands, commandTimeout, chunkSize, threads, queueCapacity);
    }

    public KeyDumpItemReader(ItemReader<K> keyReader, GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout, int chunkSize, int threads, int queueCapacity, Function<SimpleStepBuilder<K, K>, SimpleStepBuilder<K, K>> stepBuilderProvider) {
        super(keyReader, pool, commands, commandTimeout, chunkSize, threads, queueCapacity, stepBuilderProvider);
    }

    @Override
    protected List<KeyValue<K, byte[]>> values(List<? extends K> keys, BaseRedisAsyncCommands<K, V> commands) throws InterruptedException, ExecutionException, TimeoutException {
        List<RedisFuture<Long>> ttlFutures = new ArrayList<>(keys.size());
        List<RedisFuture<byte[]>> dumpFutures = new ArrayList<>(keys.size());
        for (K key : keys) {
            ttlFutures.add(((RedisKeyAsyncCommands<K, V>) commands).ttl(key));
            dumpFutures.add(((RedisKeyAsyncCommands<K, V>) commands).dump(key));
        }
        commands.flushCommands();
        List<KeyValue<K, byte[]>> dumps = new ArrayList<>(keys.size());
        for (int index = 0; index < keys.size(); index++) {
            K key = keys.get(index);
            Long ttl = ttlFutures.get(index).get(commandTimeout, TimeUnit.SECONDS);
            byte[] dump = dumpFutures.get(index).get(commandTimeout, TimeUnit.SECONDS);
            dumps.add(new KeyValue<>(key, ttl, dump));
        }
        return dumps;
    }


    public static ScanKeyValueItemReaderBuilder<KeyDumpItemReader<String,String>> builder(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisConnection<String, String> connection) {
        return new ScanKeyValueItemReaderBuilder() {
            @Override
            public KeyDumpItemReader<String, String> build() {
                return new KeyDumpItemReader<>(keyReader(connection), pool, c -> ((StatefulRedisConnection<String, String>) c).async(), commandTimeout, chunkSize, threadCount, queueCapacity);
            }
        };
    }

    public static LiveKeyValueItemReaderBuilder<KeyDumpItemReader<String,String>> builder(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisPubSubConnection<String, String> connection) {
        return new LiveKeyValueItemReaderBuilder() {
            @Override
            public KeyDumpItemReader<String, String> build() {
                return new KeyDumpItemReader<>(keyspaceNotificationReader(connection), pool, c -> ((StatefulRedisConnection<String, String>) c).async(), commandTimeout, chunkSize, threadCount, queueCapacity, stepBuilderProvider());
            }
        };
    }

    public static ScanKeyValueItemReaderBuilder<KeyDumpItemReader<String,String>> builder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterConnection<String, String> connection) {
        return new ScanKeyValueItemReaderBuilder() {
            @Override
            public KeyDumpItemReader<String, String> build() {
                return new KeyDumpItemReader<>(keyReader(connection), pool, c -> ((StatefulRedisClusterConnection<String, String>) c).async(), commandTimeout, chunkSize, threadCount, queueCapacity);
            }
        };
    }

    public static LiveKeyValueItemReaderBuilder<KeyDumpItemReader<String,String>> builder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterPubSubConnection<String, String> connection) {
        return new LiveKeyValueItemReaderBuilder() {
            @Override
            public KeyDumpItemReader<String, String> build() {
                return new KeyDumpItemReader<>(keyspaceNotificationReader(connection), pool, c -> ((StatefulRedisClusterConnection<String, String>) c).async(), commandTimeout, chunkSize, threadCount, queueCapacity, stepBuilderProvider());
            }
        };
    }

}
