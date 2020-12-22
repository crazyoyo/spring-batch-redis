package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import org.springframework.batch.item.ItemReader;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class AbstractKeyDumpItemReader<K, V, C extends StatefulConnection<K, V>> extends AbstractKeyValueItemReader<K, V, KeyValue<K, byte[]>, C> {

    protected AbstractKeyDumpItemReader(ItemReader<K> keyReader, Duration commandTimeout, int chunkSize, int threads, int queueCapacity, Duration pollingTimeout) {
        super(keyReader, commandTimeout, chunkSize, threads, queueCapacity, pollingTimeout);
    }

    @Override
    protected List<KeyValue<K, byte[]>> readValues(List<? extends K> keys, BaseRedisAsyncCommands<K, V> commands, long timeout) throws InterruptedException, ExecutionException, TimeoutException {
        List<RedisFuture<Long>> ttlFutures = new ArrayList<>(keys.size());
        List<RedisFuture<byte[]>> dumpFutures = new ArrayList<>(keys.size());
        for (K key : keys) {
            ttlFutures.add(((RedisKeyAsyncCommands<K, V>) commands).ttl(key));
            dumpFutures.add(((RedisKeyAsyncCommands<K, V>) commands).dump(key));
        }
        commands.flushCommands();
        List<KeyValue<K, byte[]>> dumps = new ArrayList<>(keys.size());
        for (int index = 0; index < keys.size(); index++) {
            K key = keys.get(index);
            Long ttl = ttlFutures.get(index).get(timeout, TimeUnit.SECONDS);
            byte[] dump = dumpFutures.get(index).get(timeout, TimeUnit.SECONDS);
            dumps.add(new KeyValue<>(key, ttl, dump));
        }
        return dumps;
    }
}
