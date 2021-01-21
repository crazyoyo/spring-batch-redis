package org.springframework.batch.item.redis;

import io.lettuce.core.Range;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.*;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

public class DataStructureItemReader<K, V> extends AbstractKeyValueItemReader<K, V, DataStructure<K>> {

    public DataStructureItemReader(ItemReader<K> keyReader, GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout, int chunkSize, int threads, int queueCapacity) {
        super(keyReader, pool, commands, commandTimeout, chunkSize, threads, queueCapacity);
    }

    public DataStructureItemReader(ItemReader<K> keyReader, GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout, int chunkSize, int threads, int queueCapacity, Function<SimpleStepBuilder<K, K>, SimpleStepBuilder<K, K>> stepBuilderProvider) {
        super(keyReader, pool, commands, commandTimeout, chunkSize, threads, queueCapacity, stepBuilderProvider);
    }

    @Override
    protected List<DataStructure<K>> values(List<? extends K> keys, BaseRedisAsyncCommands<K, V> commands) throws InterruptedException, ExecutionException, TimeoutException {
        List<RedisFuture<String>> typeFutures = new ArrayList<>(keys.size());
        for (K key : keys) {
            typeFutures.add(((RedisKeyAsyncCommands<K, V>) commands).type(key));
        }
        commands.flushCommands();
        List<DataStructure<K>> values = new ArrayList<>(keys.size());
        List<RedisFuture<Long>> ttlFutures = new ArrayList<>(keys.size());
        List<RedisFuture<?>> valueFutures = new ArrayList<>(keys.size());
        for (int index = 0; index < keys.size(); index++) {
            K key = keys.get(index);
            String typeName = typeFutures.get(index).get(commandTimeout, TimeUnit.SECONDS);
            DataType type = DataType.fromCode(typeName);
            valueFutures.add(getValue(commands, key, type));
            ttlFutures.add(((RedisKeyAsyncCommands<K, V>) commands).ttl(key));
            DataStructure dataStructure = new DataStructure();
            dataStructure.setKey(key);
            dataStructure.setType(type);
            values.add(dataStructure);
        }
        commands.flushCommands();
        for (int index = 0; index < values.size(); index++) {
            DataStructure dataStructure = values.get(index);
            dataStructure.setValue(valueFutures.get(index).get(commandTimeout, TimeUnit.SECONDS));
            dataStructure.setTtl(ttlFutures.get(index).get(commandTimeout, TimeUnit.SECONDS));
        }
        return values;
    }

    @SuppressWarnings("unchecked")
    private RedisFuture<?> getValue(BaseRedisAsyncCommands<K, V> commands, K key, DataType type) {
        if (type == null) {
            return null;
        }
        switch (type) {
            case HASH:
                return ((RedisHashAsyncCommands<K, V>) commands).hgetall(key);
            case LIST:
                return ((RedisListAsyncCommands<K, V>) commands).lrange(key, 0, -1);
            case SET:
                return ((RedisSetAsyncCommands<K, V>) commands).smembers(key);
            case STREAM:
                return ((RedisStreamAsyncCommands<K, V>) commands).xrange(key, Range.create("-", "+"));
            case STRING:
                return ((RedisStringAsyncCommands<K, V>) commands).get(key);
            case ZSET:
                return ((RedisSortedSetAsyncCommands<K, V>) commands).zrangeWithScores(key, 0, -1);
            default:
                return null;
        }
    }


    public static ScanKeyValueItemReaderBuilder<DataStructureItemReader<String, String>> builder(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisConnection<String, String> connection) {
        return new ScanKeyValueItemReaderBuilder() {
            @Override
            public DataStructureItemReader<String, String> build() {
                return new DataStructureItemReader<>(keyReader(connection), pool, c -> ((StatefulRedisConnection<String, String>) c).async(), commandTimeout, chunkSize, threadCount, queueCapacity);
            }
        };
    }

    public static LiveKeyValueItemReaderBuilder<DataStructureItemReader<String, String>> builder(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisPubSubConnection<String, String> connection) {
        return new LiveKeyValueItemReaderBuilder() {
            @Override
            public DataStructureItemReader<String, String> build() {
                return new DataStructureItemReader<>(keyspaceNotificationReader(connection), pool, c -> ((StatefulRedisConnection<String, String>) c).async(), commandTimeout, chunkSize, threadCount, queueCapacity, stepBuilderProvider());
            }
        };
    }


    public static ScanKeyValueItemReaderBuilder<DataStructureItemReader<String, String>> builder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterConnection<String, String> connection) {
        return new ScanKeyValueItemReaderBuilder() {
            @Override
            public DataStructureItemReader<String, String> build() {
                return new DataStructureItemReader<>(keyReader(connection), pool, c -> ((StatefulRedisClusterConnection<String, String>) c).async(), commandTimeout, chunkSize, threadCount, queueCapacity);
            }
        };
    }

    public static LiveKeyValueItemReaderBuilder<DataStructureItemReader<String, String>> builder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterPubSubConnection<String, String> connection) {
        return new LiveKeyValueItemReaderBuilder() {
            @Override
            public DataStructureItemReader<String, String> build() {
                return new DataStructureItemReader<>(keyspaceNotificationReader(connection), pool, c -> ((StatefulRedisClusterConnection<String, String>) c).async(), commandTimeout, chunkSize, threadCount, queueCapacity, stepBuilderProvider());
            }
        };
    }

}
