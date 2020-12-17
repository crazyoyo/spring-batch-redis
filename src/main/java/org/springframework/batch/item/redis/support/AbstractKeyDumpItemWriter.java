package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class AbstractKeyDumpItemWriter<K, V, C extends StatefulConnection<K, V>> extends AbstractItemStreamItemWriter<KeyValue<K, byte[]>> {

    private final boolean replace;
    private final long commandTimeout;

    public AbstractKeyDumpItemWriter(boolean replace, Duration commandTimeout) {
        Assert.notNull(commandTimeout, "Command timeout is required.");
        this.replace = replace;
        this.commandTimeout = commandTimeout.getSeconds();
    }

    protected abstract C connection() throws Exception;

    protected abstract BaseRedisAsyncCommands<K, V> commands(C connection);


    @Override
    public void write(List<? extends KeyValue<K, byte[]>> items) throws Exception {
        try (C connection = connection()) {
            BaseRedisAsyncCommands<K, V> commands = commands(connection);
            commands.setAutoFlushCommands(false);
            List<RedisFuture<?>> futures = new ArrayList<>(items.size());
            for (KeyValue<K, byte[]> item : items) {
                futures.add(restore((RedisKeyAsyncCommands<K, V>) commands, item));
            }
            commands.flushCommands();
            for (RedisFuture<?> future : futures) {
                future.get(commandTimeout, TimeUnit.SECONDS);
            }
            commands.setAutoFlushCommands(true);
        }

    }

    private RedisFuture<?> restore(RedisKeyAsyncCommands<K, V> keyCommands, KeyValue<K, byte[]> item) {
        if (item.getValue() == null || item.noKeyTtl()) {
            return keyCommands.del(item.getKey());
        }
        RestoreArgs restoreArgs = new RestoreArgs().replace(replace);
        if (item.hasTtl()) {
            restoreArgs.ttl(item.getTtl() * 1000);
        }
        return keyCommands.restore(item.getKey(), item.getValue(), restoreArgs);
    }

}
