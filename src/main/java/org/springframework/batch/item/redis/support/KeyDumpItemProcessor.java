package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import lombok.Builder;
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
public class KeyDumpItemProcessor<K, V, C extends StatefulConnection<K, V>> implements ItemProcessor<List<K>, List<KeyDump<K>>> {

    private final GenericObjectPool<C> pool;
    private final Function<C, BaseRedisAsyncCommands<K, V>> commands;
    private final long timeout;

    @Builder
    public KeyDumpItemProcessor(GenericObjectPool<C> pool, Function<C, BaseRedisAsyncCommands<K, V>> commands, long commandTimeout) {
        Assert.notNull(pool, "A connection pool is required.");
        Assert.notNull(commands, "A commands function is required.");
        Assert.isTrue(commandTimeout > 0, "Command timeout must be positive.");
        this.pool = pool;
        this.commands = commands;
        this.timeout = commandTimeout;
    }

    @Override
    public List<KeyDump<K>> process(List<K> keys) throws Exception {
        try (C connection = pool.borrowObject()) {
            BaseRedisAsyncCommands<K, V> commands = this.commands.apply(connection);
            commands.setAutoFlushCommands(false);
            List<RedisFuture<Long>> ttls = new ArrayList<>(keys.size());
            List<RedisFuture<byte[]>> dumps = new ArrayList<>(keys.size());
            for (K key : keys) {
                ttls.add(((RedisKeyAsyncCommands<K, V>) commands).pttl(key));
                dumps.add(((RedisKeyAsyncCommands<K, V>) commands).dump(key));
            }
            commands.flushCommands();
            List<KeyDump<K>> keyDumps = new ArrayList<>();
            for (int index = 0; index < keys.size(); index++) {
                try {
                    Long ttl = ttls.get(index).get(timeout, TimeUnit.SECONDS);
                    byte[] dump = dumps.get(index).get(timeout, TimeUnit.SECONDS);
                    keyDumps.add(new KeyDump<>(keys.get(index), ttl, dump));
                } catch (InterruptedException e) {
                    log.debug("Interrupted while dumping", e);
                } catch (ExecutionException e) {
                    log.error("Could not dump", e);
                } catch (TimeoutException e) {
                    log.error("Timeout in DUMP command", e);
                }
            }
            commands.setAutoFlushCommands(true);
            return keyDumps;
        }
    }

}
