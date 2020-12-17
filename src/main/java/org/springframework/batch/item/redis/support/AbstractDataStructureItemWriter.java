package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.*;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class AbstractDataStructureItemWriter<K, V, C extends StatefulConnection<K, V>> extends AbstractItemStreamItemWriter<DataStructure<K>> {

    private final long commandTimeout;

    public AbstractDataStructureItemWriter(Duration commandTimeout) {
        Assert.notNull(commandTimeout, "Command timeout is required.");
        this.commandTimeout = commandTimeout.getSeconds();
    }

    @Override
    public void write(List<? extends DataStructure<K>> items) throws Exception {
        try (C connection = connection()) {
            BaseRedisAsyncCommands<K, V> commands = commands(connection);
            commands.setAutoFlushCommands(false);
            List<RedisFuture<?>> futures = new ArrayList<>(items.size());
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
                            futures.add(((RedisListAsyncCommands<K, V>) commands).lpush(item.getKey(), (V[]) ((Collection<V>) item.getValue()).toArray()));
                            break;
                        case SET:
                            futures.add(((RedisSetAsyncCommands<K, V>) commands).sadd(item.getKey(), (V[]) ((Collection<V>) item.getValue()).toArray()));
                            break;
                        case ZSET:
                            futures.add(((RedisSortedSetAsyncCommands<K, V>) commands).zadd(item.getKey(), (ScoredValue<V>[]) ((Collection<ScoredValue<V>>) item.getValue()).toArray(new ScoredValue[0])));
                            break;
                        case HASH:
                            futures.add(((RedisHashAsyncCommands<K, V>) commands).hmset(item.getKey(), (Map<K, V>) item.getValue()));
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
            commands.flushCommands();
            for (RedisFuture<?> future : futures) {
                future.get(commandTimeout, TimeUnit.SECONDS);
            }
            commands.setAutoFlushCommands(true);
        }
    }


    protected abstract C connection() throws Exception;

    protected abstract BaseRedisAsyncCommands<K, V> commands(C connection);

}
