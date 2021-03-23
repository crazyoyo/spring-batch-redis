package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.*;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractKeyValueItemWriter;
import org.springframework.batch.item.redis.support.DataStructure;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class DataStructureItemWriter<K, V, C extends StatefulConnection<K, V>> extends AbstractKeyValueItemWriter<K, V, C, DataStructure<K>> {

    public DataStructureItemWriter(GenericObjectPool<C> pool, Function<C, BaseRedisAsyncCommands<K, V>> commands) {
        super(pool, commands);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected List<RedisFuture<?>> write(List<? extends DataStructure<K>> items, BaseRedisAsyncCommands<K, V> commands) {
        List<RedisFuture<?>> futures = new ArrayList<>();
        for (DataStructure<K> item : items) {
            if (item.getValue() == null || item.noKeyTtl()) {
                futures.add(((RedisKeyAsyncCommands<K, V>) commands).del(item.getKey()));
                continue;
            }
            if (item.getValue() != null) {
                switch (item.getType()) {
                    case STRING:
                        futures.add(((RedisStringAsyncCommands<K, V>) commands).set(item.getKey(), (V) item.getValue()));
                        break;
                    case LIST:
                        futures.add(((RedisListAsyncCommands<K, V>) commands).rpush(item.getKey(), (V[]) ((Collection<V>) item.getValue()).toArray()));
                        break;
                    case SET:
                        futures.add(((RedisSetAsyncCommands<K, V>) commands).sadd(item.getKey(), (V[]) ((Collection<V>) item.getValue()).toArray()));
                        break;
                    case ZSET:
                        futures.add(((RedisSortedSetAsyncCommands<K, V>) commands).zadd(item.getKey(), ((Collection<ScoredValue<V>>) item.getValue()).toArray(new ScoredValue[0])));
                        break;
                    case HASH:
                        futures.add(((RedisHashAsyncCommands<K, V>) commands).hset(item.getKey(), (Map<K, V>) item.getValue()));
                        break;
                    case STREAM:
                        Collection<StreamMessage<K, V>> messages = (Collection<StreamMessage<K, V>>) item.getValue();
                        for (StreamMessage<K, V> message : messages) {
                            futures.add(((RedisStreamAsyncCommands<K, V>) commands).xadd(item.getKey(), new XAddArgs().id(message.getId()), message.getBody()));
                        }
                        break;
                }
            }
            if (item.hasTtl()) {
                futures.add(((RedisKeyAsyncCommands<K, V>) commands).expire(item.getKey(), item.getTtl()));
            }
        }
        return futures;
    }

}
