package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.*;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class KeyValueItemWriter<K, V, C extends StatefulConnection<K, V>> extends AbstractKeyValueItemWriter<K, V, C, TypeKeyValue<K>> {

    public KeyValueItemWriter(GenericObjectPool<C> pool, Function<C, BaseRedisAsyncCommands<K, V>> commands, long commandTimeout) {
        super(pool, commands, commandTimeout);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doWrite(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, TypeKeyValue<K> item) {
        switch (item.getType()) {
            case STRING:
                futures.add(((RedisStringAsyncCommands<K, V>) commands).set(item.getKey(), (V) item.getValue()));
                break;
            case LIST:
                futures.add(((RedisListAsyncCommands<K, V>) commands).lpush(item.getKey(), (V[]) ((List<V>) item.getValue()).toArray()));
                break;
            case SET:
                futures.add(((RedisSetAsyncCommands<K, V>) commands).sadd(item.getKey(), (V[]) ((Set<V>) item.getValue()).toArray()));
                break;
            case ZSET:
                futures.add(((RedisSortedSetAsyncCommands<K, V>) commands).zadd(item.getKey(), (ScoredValue<V>[]) (((List<ScoredValue<V>>) item.getValue()).toArray())));
                break;
            case HASH:
                futures.add(((RedisHashAsyncCommands<K, V>) commands).hmset(item.getKey(), (Map<K, V>) item.getValue()));
                break;
            case STREAM:
                List<StreamMessage<K, V>> messages = (List<StreamMessage<K, V>>) item.getValue();
                for (StreamMessage<K, V> message : messages) {
                    futures.add(((RedisStreamAsyncCommands<K, V>) commands).xadd(item.getKey(), new XAddArgs().id(message.getId()), message.getBody()));
                }
                break;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doWrite(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, TypeKeyValue<K> item, long ttl) {
        doWrite(commands, futures, item);
        futures.add(((RedisKeyAsyncCommands<K, V>) commands).expire(item.getKey(), item.getTtl()));
    }
}
