package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.*;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class KeyValueCommand<K, V> implements Command<K, V, KeyValue<K>> {

    @Override
    public RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, KeyValue<K> args) {
        switch (args.getType()) {
            case STRING:
                return ((RedisStringAsyncCommands<K, V>) commands).set(args.getKey(), (V) args.getValue());
            case LIST:
                return ((RedisListAsyncCommands<K, V>) commands).lpush(args.getKey(), (V[]) ((List<V>) args.getValue()).toArray());
            case SET:
                return ((RedisSetAsyncCommands<K, V>) commands).sadd(args.getKey(), (V[]) ((Set<V>) args.getValue()).toArray());
            case ZSET:
                return ((RedisSortedSetAsyncCommands<K, V>) commands).zadd(args.getKey(), (ScoredValue<V>[]) (((List<ScoredValue<V>>) args.getValue()).toArray()));
            case HASH:
                return ((RedisHashAsyncCommands<K, V>) commands).hmset(args.getKey(), (Map<K, V>) args.getValue());
            case STREAM:
                List<StreamMessage<K, V>> messages = (List<StreamMessage<K, V>>) args.getValue();
                if (messages.size()>1) {
                    log.warn("Multiple stream messages in TypeValue - Some might be lost");
                }
                RedisFuture<?> xaddFuture = null;
                for (StreamMessage<K, V> message : messages) {
                    xaddFuture = ((RedisStreamAsyncCommands<K, V>) commands).xadd(args.getKey(), new XAddArgs().id(message.getId()), message.getBody());
                }
                return xaddFuture;
            default:
                return null;
        }

    }
}
