package org.springframework.batch.item.redis.support;

import io.lettuce.core.Range;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.async.*;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public abstract class AbstractValueReader<K, V> implements ItemProcessor<List<K>, List<KeyValue<K>>> {

    @Getter
    @Setter
    private long timeout;

    protected AbstractValueReader(Duration timeout) {
        this.timeout = timeout == null ? RedisURI.DEFAULT_TIMEOUT : timeout.getSeconds();
    }

    protected List<KeyValue<K>> read(List<K> keys, BaseRedisAsyncCommands<K, V> commands) {
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
                ttls.add(future.get(timeout, TimeUnit.SECONDS));
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
                types.add(DataType.fromCode(future.get(timeout, TimeUnit.SECONDS)));
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
                keyValue.setTtl(ttlFutures.get(index).get(timeout, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                log.debug("Interrupted while getting TTL for key {}", key, e);
            } catch (ExecutionException e) {
                log.error("Could not get TTL for key {}", key, e);
            } catch (TimeoutException e) {
                log.error("Timeout in TTL command", e);
            }
            try {
                keyValue.setValue(valueFutures.get(index).get(timeout, TimeUnit.SECONDS));
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
