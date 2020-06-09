package org.springframework.batch.item.redis.support;

import io.lettuce.core.Range;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemReader;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@Slf4j
public class KeyValueItemReader<K, V, C extends StatefulConnection<K, V>> extends AbstractRedisItemReader<K, V, C, TypeKeyValue<K>> {

    public KeyValueItemReader(ItemReader<K> keyReader, GenericObjectPool<C> pool, Function<C, BaseRedisAsyncCommands<K, V>> commands, ReaderOptions options) {
        super(keyReader, pool, commands, options);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected List<TypeKeyValue<K>> values(List<? extends K> keys, BaseRedisAsyncCommands<K, V> commands) throws Exception {
        List<RedisFuture<String>> typeFutures = new ArrayList<>(keys.size());
        for (K key : keys) {
            typeFutures.add(((RedisKeyAsyncCommands<K, V>) commands).type(key));
        }
        commands.flushCommands();
        List<TypeKeyValue<K>> values = new ArrayList<>(keys.size());
        List<RedisFuture<Long>> ttlFutures = new ArrayList<>(keys.size());
        List<RedisFuture<?>> valueFutures = new ArrayList<>(keys.size());
        for (int index = 0; index < keys.size(); index++) {
            K key = keys.get(index);
            DataType type = DataType.fromCode(get(typeFutures.get(index)));
            valueFutures.add(getValue(commands, key, type));
            ttlFutures.add(((RedisKeyAsyncCommands<K, V>) commands).ttl(key));
            TypeKeyValue<K> keyValue = new TypeKeyValue<>();
            keyValue.setKey(key);
            keyValue.setType(type);
            values.add(keyValue);
        }
        commands.flushCommands();
        for (int index = 0; index < values.size(); index++) {
            TypeKeyValue<K> keyValue = values.get(index);
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
            case STRING:
                return ((RedisStringAsyncCommands<K, V>) commands).get(key);
            case LIST:
                return ((RedisListAsyncCommands<K, V>) commands).lrange(key, 0, -1);
            case SET:
                return ((RedisSetAsyncCommands<K, V>) commands).smembers(key);
            case ZSET:
                return ((RedisSortedSetAsyncCommands<K, V>) commands).zrangeWithScores(key, 0, -1);
            case HASH:
                return ((RedisHashAsyncCommands<K, V>) commands).hgetall(key);
            case STREAM:
                return ((RedisStreamAsyncCommands<K, V>) commands).xrange(key, Range.create("-", "+"));
            default:
                return null;
        }
    }


}
