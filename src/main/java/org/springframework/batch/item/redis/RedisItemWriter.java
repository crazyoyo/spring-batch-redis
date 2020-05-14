package org.springframework.batch.item.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.support.Command;
import org.springframework.batch.item.redis.support.RedisBuilder;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
public class RedisItemWriter<K, V, T> extends AbstractItemStreamItemWriter<T> implements InitializingBean {

    private final GenericObjectPool<? extends StatefulConnection<K, V>> pool;
    private final Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands;
    private final Command<K, V, T> command;
    private final long timeout;

    public RedisItemWriter(GenericObjectPool<? extends StatefulConnection<K, V>> connectionPool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Command<K, V, T> command, long commandTimeout) {
        Assert.notNull(connectionPool, "A connection pool is required.");
        Assert.notNull(commands, "A commands function is required.");
        Assert.notNull(command, "A command instance is required.");
        Assert.isTrue(commandTimeout > 0, "Command timeout must be positive.");
        this.pool = connectionPool;
        this.commands = commands;
        this.command = command;
        this.timeout = commandTimeout;
    }

    @Override
    public void afterPropertiesSet() {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(pool, "A connection pool is required.");
        Assert.notNull(commands, "A commands function is required.");
        Assert.notNull(command, "A write command is required.");
    }

    @Override
    public void write(List<? extends T> items) throws Exception {
        try (StatefulConnection<K, V> connection = pool.borrowObject()) {
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

    @Setter
    @Accessors(fluent = true)
    public static class RedisItemWriterBuilder<T> extends RedisBuilder {

        private RedisURI redisURI;
        private boolean cluster;
        private int maxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
        private int minIdle = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;
        private int maxIdle = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
        private Command<String, String, T> command;
        private long commandTimeout = RedisURI.DEFAULT_TIMEOUT;


        public RedisItemWriter<String, String, T> build() {
            if (cluster) {
                return new RedisItemWriter<>(connectionPool(RedisClusterClient.create(redisURI), maxTotal, minIdle, maxIdle), c -> ((StatefulRedisClusterConnection<String, String>) c).async(), command, commandTimeout);
            }
            return new RedisItemWriter<>(connectionPool(RedisClient.create(redisURI), maxTotal, minIdle, maxIdle), c -> ((StatefulRedisConnection<String, String>) c).async(), command, commandTimeout);
        }

    }
}
