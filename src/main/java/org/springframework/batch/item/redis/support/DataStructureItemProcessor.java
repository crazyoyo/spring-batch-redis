package org.springframework.batch.item.redis.support;

import io.lettuce.core.Range;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@Slf4j
public class DataStructureItemProcessor<K, V> extends AbstractKeyValueItemProcessor<K, V, DataStructure<K>> {

    public DataStructureItemProcessor(GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout) {
        super(pool, commands, commandTimeout);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected List<DataStructure<K>> values(List<? extends K> keys, BaseRedisAsyncCommands<K, V> commands) {
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
            String typeName;
            try {
                typeName = get(typeFutures.get(index));
            } catch (Exception e) {
                log.error("Could not get type", e);
                continue;
            }
            DataType type = DataType.fromCode(typeName);
            valueFutures.add(getValue(commands, key, type));
            ttlFutures.add(((RedisKeyAsyncCommands<K, V>) commands).ttl(key));
            DataStructure<K> keyValue = new DataStructure<>();
            keyValue.setKey(key);
            keyValue.setType(type);
            values.add(keyValue);
        }
        commands.flushCommands();
        for (int index = 0; index < values.size(); index++) {
            DataStructure<K> keyValue = values.get(index);
            try {
                keyValue.setValue(get(valueFutures.get(index)));
            } catch (Exception e) {
                log.error("Could not get value", e);
            }
            try {
                keyValue.setTtl(getTtl(ttlFutures.get(index)));
            } catch (Exception e) {
                log.error("Could not get ttl", e);
            }
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
