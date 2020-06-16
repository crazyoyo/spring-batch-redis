package org.springframework.batch.item.redis;

import com.redislabs.lettuce.helper.RedisOptions;
import io.lettuce.core.Range;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.*;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.AbstractRedisItemReader;
import org.springframework.batch.item.redis.support.DataType;
import org.springframework.batch.item.redis.support.ReaderOptions;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@Slf4j
public class RedisKeyValueItemReader<K, V> extends AbstractRedisItemReader<K, V, KeyValue<K>> {

    public RedisKeyValueItemReader(ItemReader<K> keyReader, GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, int threadCount, int batchSize, Duration commandTimeout, int queueCapacity, long queuePollingTimeout) {
        super(keyReader, pool, commands, threadCount, batchSize, commandTimeout, queueCapacity, queuePollingTimeout);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected List<KeyValue<K>> values(List<? extends K> keys, BaseRedisAsyncCommands<K, V> commands) throws Exception {
        List<RedisFuture<String>> typeFutures = new ArrayList<>(keys.size());
        for (K key : keys) {
            typeFutures.add(((RedisKeyAsyncCommands<K, V>) commands).type(key));
        }
        commands.flushCommands();
        List<KeyValue<K>> values = new ArrayList<>(keys.size());
        List<RedisFuture<Long>> ttlFutures = new ArrayList<>(keys.size());
        List<RedisFuture<?>> valueFutures = new ArrayList<>(keys.size());
        for (int index = 0; index < keys.size(); index++) {
            K key = keys.get(index);
            DataType type = DataType.fromCode(get(typeFutures.get(index)));
            valueFutures.add(getValue(commands, key, type));
            ttlFutures.add(((RedisKeyAsyncCommands<K, V>) commands).ttl(key));
            KeyValue<K> keyValue = new KeyValue<>();
            keyValue.setKey(key);
            keyValue.setType(type);
            values.add(keyValue);
        }
        commands.flushCommands();
        for (int index = 0; index < values.size(); index++) {
            KeyValue<K> keyValue = values.get(index);
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

    public static RedisKeyValueItemReaderBuilder builder() {
        return new RedisKeyValueItemReaderBuilder();
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisKeyValueItemReaderBuilder extends AbstractRedisItemReaderBuilder {

        private RedisOptions redisOptions = RedisOptions.builder().build();
        private ReaderOptions readerOptions = ReaderOptions.builder().build();

        public RedisKeyValueItemReader<String, String> build() {
            Assert.notNull(redisOptions, "Redis options are required.");
            Assert.notNull(readerOptions, "Reader options are required.");
            return new RedisKeyValueItemReader<>(keyReader(redisOptions, readerOptions), redisOptions.connectionPool(), redisOptions.async(), readerOptions.getThreadCount(), readerOptions.getBatchSize(), redisOptions.getTimeout(), readerOptions.getValueQueueOptions().getCapacity(), readerOptions.getValueQueueOptions().getPollingTimeout());
        }

    }

}
