package org.springframework.batch.item.redis.support;

import io.lettuce.core.Range;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.*;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class DataStructureItemReader<K, V, C extends StatefulConnection<K, V>> extends AbstractKeyValueItemReader<K, V, C, DataStructure<K>> {

    public DataStructureItemReader(Duration readTimeout, ItemReader<K> keyReader, GenericObjectPool<C> pool, Function<C, BaseRedisAsyncCommands<K, V>> commands, int chunkSize, int threads, int queueCapacity) {
        super(readTimeout, keyReader, pool, commands, chunkSize, threads, queueCapacity);
    }

    public DataStructureItemReader(Duration readTimeout, ItemReader<K> keyReader, GenericObjectPool<C> pool, Function<C, BaseRedisAsyncCommands<K, V>> commands, int chunkSize, int threads, int queueCapacity, Function<SimpleStepBuilder<K, K>, SimpleStepBuilder<K, K>> stepBuilderProvider) {
        super(readTimeout, keyReader, pool, commands, chunkSize, threads, queueCapacity, stepBuilderProvider);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<DataStructure<K>> values(List<? extends K> keys) throws Exception {
        try (C connection = pool.borrowObject()) {
            BaseRedisAsyncCommands<K, V> commands = this.commands.apply(connection);
            commands.setAutoFlushCommands(false);
            List<RedisFuture<String>> typeFutures = new ArrayList<>(keys.size());
            for (K key : keys) {
                typeFutures.add(((RedisKeyAsyncCommands<K, V>) commands).type(key));
            }
            commands.flushCommands();
            long commandTimeout = connection.getTimeout().toMillis();
            try {
                List<DataStructure<K>> values = new ArrayList<>(keys.size());
                List<RedisFuture<Long>> ttlFutures = new ArrayList<>(keys.size());
                List<RedisFuture<?>> valueFutures = new ArrayList<>(keys.size());
                for (int index = 0; index < keys.size(); index++) {
                    K key = keys.get(index);
                    RedisFuture<String> typeFuture = typeFutures.get(index);
                    DataType type = typeFuture == null ? null : toDataType(typeFuture.get(commandTimeout, TimeUnit.MILLISECONDS));
                    valueFutures.add(getValue(commands, key, type));
                    ttlFutures.add(((RedisKeyAsyncCommands<K, V>) commands).ttl(key));
                    DataStructure<K> dataStructure = new DataStructure<>();
                    dataStructure.setKey(key);
                    dataStructure.setType(type);
                    values.add(dataStructure);
                }
                commands.flushCommands();
                for (int index = 0; index < values.size(); index++) {
                    DataStructure<K> dataStructure = values.get(index);
                    RedisFuture<?> valueFuture = valueFutures.get(index);
                    Object value = valueFuture == null ? null : valueFuture.get(commandTimeout, TimeUnit.MILLISECONDS);
                    dataStructure.setValue(value);
                    RedisFuture<Long> ttlFuture = ttlFutures.get(index);
                    Long ttl = ttlFuture == null ? null : ttlFuture.get(commandTimeout, TimeUnit.MILLISECONDS);
                    dataStructure.setTtl(ttl);
                }
                return values;
            } finally {
                commands.setAutoFlushCommands(true);
            }
        }
    }

    private DataType toDataType(String code) {
        return DataType.fromCode(code);
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

}
