package org.springframework.batch.item.redis;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.RedisClusterItemWriterBuilder;
import org.springframework.batch.item.redis.support.RedisItemWriterBuilder;
import org.springframework.batch.item.redis.support.WriteCommand;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
public class RedisItemWriter<K, V, C extends StatefulConnection<K,V>, T> extends AbstractItemStreamItemWriter<T> {

    private final GenericObjectPool<C> pool;
    private final Function<C, BaseRedisAsyncCommands<K, V>> commands;
    private final WriteCommand<K, V, T> command;
    private final long timeout;

    public RedisItemWriter(GenericObjectPool<C> connectionPool, Function<C, BaseRedisAsyncCommands<K, V>> commands, WriteCommand<K, V, T> writeCommand, long commandTimeout) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(connectionPool, "A connection pool is required.");
        Assert.notNull(commands, "A commands provider is required.");
        Assert.notNull(writeCommand, "A write command is required.");
        Assert.isTrue(commandTimeout > 0, "Command timeout must be positive.");
        this.pool = connectionPool;
        this.commands = commands;
        this.command = writeCommand;
        this.timeout = commandTimeout;
    }

    @Override
    public void write(List<? extends T> items) throws Exception {
        try (C connection = pool.borrowObject()) {
            BaseRedisAsyncCommands<K, V> commands = this.commands.apply(connection);
            commands.setAutoFlushCommands(false);
            List<RedisFuture<?>> futures = new ArrayList<>();
            for (T item : items) {
                RedisFuture<?> future = null;
                try {
                    future = command.write(commands, item);
                } catch (Exception e) {
                    log.error("Could not execute command", e);
                }
                if (future == null) {
                    continue;
                }
                futures.add(future);
            }
            commands.flushCommands();
            for (RedisFuture<?> future : futures) {
                try {
                    future.get(timeout, TimeUnit.SECONDS);
                } catch (Exception e) {
                    log.error("Could not write record", e);
                }
            }
            commands.setAutoFlushCommands(true);
        }
    }

    public static <T> RedisItemWriterBuilder<T> builder() {
        return new RedisItemWriterBuilder<>();
    }

    public static <T> RedisClusterItemWriterBuilder<T> clusterBuilder() {
        return new RedisClusterItemWriterBuilder<>();
    }

}
