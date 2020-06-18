package org.springframework.batch.item.redis;

import com.redislabs.lettuce.helper.RedisOptions;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.*;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractKeyValueItemWriter;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class RedisKeyValueItemWriter<K, V> extends AbstractKeyValueItemWriter<K, V, KeyValue<K>> {

    public RedisKeyValueItemWriter(GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout) {
        super(pool, commands, commandTimeout);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doWrite(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, KeyValue<K> item) {
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
    protected void doWrite(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, KeyValue<K> item, long ttl) {
        doWrite(commands, futures, item);
        futures.add(((RedisKeyAsyncCommands<K, V>) commands).expire(item.getKey(), item.getTtl()));
    }

    public static RedisKeyValueItemWriterBuilder builder() {
        return new RedisKeyValueItemWriterBuilder();
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisKeyValueItemWriterBuilder {

        private RedisOptions redisOptions;

        public RedisKeyValueItemWriter<String, String> build() {
            Assert.notNull(redisOptions, "Redis options are required.");
            return new RedisKeyValueItemWriter<>(redisOptions.connectionPool(), redisOptions.async(), redisOptions.getTimeout());
        }

    }
}
