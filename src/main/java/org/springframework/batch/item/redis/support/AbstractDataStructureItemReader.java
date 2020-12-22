package org.springframework.batch.item.redis.support;

import io.lettuce.core.Range;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.*;
import org.springframework.batch.item.ItemReader;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class AbstractDataStructureItemReader<K, V, C extends StatefulConnection<K, V>> extends AbstractKeyValueItemReader<K, V, DataStructure<K>, C> {

    protected AbstractDataStructureItemReader(ItemReader<K> keyReader, Duration commandTimeout, int chunkSize, int threads, int queueCapacity, Duration pollingTimeout) {
        super(keyReader, commandTimeout, chunkSize, threads, queueCapacity, pollingTimeout);
    }

    @Override
    protected List<DataStructure<K>> readValues(List<? extends K> keys, BaseRedisAsyncCommands<K, V> commands, long timeout) throws InterruptedException, ExecutionException, TimeoutException {
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
            String typeName = typeFutures.get(index).get(timeout, TimeUnit.SECONDS);
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
            dataStructure.setValue(valueFutures.get(index).get(timeout, TimeUnit.SECONDS));
            dataStructure.setTtl(ttlFutures.get(index).get(timeout, TimeUnit.SECONDS));
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
}
