package org.springframework.batch.item.redis.support;

import io.lettuce.core.Range;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

@Slf4j
public class KeyValueItemProcessor<K, V, C extends StatefulConnection<K, V>> implements ItemProcessor<List<? extends K>, List<? extends KeyValue<K>>> {

    private final GenericObjectPool<C> pool;
    private final Function<C, BaseRedisAsyncCommands<K, V>> commands;
    private final long commandTimeout;

    public KeyValueItemProcessor(GenericObjectPool<C> pool, Function<C, BaseRedisAsyncCommands<K, V>> commands, long commandTimeout) {
        Assert.notNull(pool, "A connection pool is required.");
        Assert.notNull(commands, "A commands provider is required.");
        Assert.isTrue(commandTimeout > 0, "Command timeout must be positive.");
        this.pool = pool;
        this.commands = commands;
        this.commandTimeout = commandTimeout;
    }

    @Override
    public List<KeyValue<K>> process(List<? extends K> keys) throws Exception {
        try (C connection = pool.borrowObject()) {
            BaseRedisAsyncCommands<K, V> commands = this.commands.apply(connection);
            commands.setAutoFlushCommands(false);
            List<RedisFuture<Long>> ttlFutures = new ArrayList<>(keys.size());
            List<RedisFuture<String>> typeFutures = new ArrayList<>(keys.size());
            for (K key : keys) {
                ttlFutures.add(((RedisKeyAsyncCommands<K, V>) commands).ttl(key));
                typeFutures.add(((RedisKeyAsyncCommands<K, V>) commands).type(key));
            }
            commands.flushCommands();
            List<Long> ttls = new ArrayList<>(ttlFutures.size());
            for (RedisFuture<Long> future : ttlFutures) {
                try {
                    ttls.add(future.get(commandTimeout, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    log.debug("Interrupted while getting type", e);
                } catch (ExecutionException e) {
                    log.error("Could not get type", e);
                } catch (TimeoutException e) {
                    log.error("Timeout in TYPE command", e);
                }
            }
            List<DataType> types = new ArrayList<>(typeFutures.size());
            for (RedisFuture<String> future : typeFutures) {
                try {
                    String type = future.get(commandTimeout, TimeUnit.SECONDS);
                    types.add(DataType.fromCode(type));
                } catch (InterruptedException e) {
                    log.debug("Interrupted while getting type", e);
                } catch (ExecutionException e) {
                    log.error("Could not get type", e);
                } catch (TimeoutException e) {
                    log.error("Timeout in TYPE command", e);
                }
            }
            List<RedisFuture<?>> valueFutures = new ArrayList<>();
            for (int index = 0; index < keys.size(); index++) {
                valueFutures.add(readValue(keys.get(index), types.get(index), commands));
            }
            commands.flushCommands();
            List<KeyValue<K>> keyValues = new ArrayList<>();
            for (int index = 0; index < keys.size(); index++) {
                K key = keys.get(index);
                KeyValue<K> keyValue = new KeyValue<>();
                keyValue.setKey(key);
                keyValue.setTtl(ttls.get(index));
                keyValue.setType(types.get(index));
                try {
                    keyValue.setTtl(ttlFutures.get(index).get(commandTimeout, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    log.debug("Interrupted while getting TTL for key {}", key, e);
                } catch (ExecutionException e) {
                    log.error("Could not get TTL for key {}", key, e);
                } catch (TimeoutException e) {
                    log.error("Timeout in TTL command", e);
                }
                try {
                    keyValue.setValue(valueFutures.get(index).get(commandTimeout, TimeUnit.SECONDS));
                } catch (NullPointerException e) {
                    log.error("Nullpointer for key {}", key, e);
                } catch (InterruptedException e) {
                    log.debug("Interrupted while getting value for key {}", key, e);
                } catch (ExecutionException e) {
                    log.error("Could not get value for key {}", key, e);
                } catch (TimeoutException e) {
                    log.error("Timeout while reading value for key {}", key, e);
                }
                keyValues.add(keyValue);
            }
            commands.setAutoFlushCommands(true);
            return keyValues;
        }
    }

    private RedisFuture<?> readValue(K key, DataType type, BaseRedisAsyncCommands<K, V> commands) {
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
