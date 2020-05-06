package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractRedisItemWriter<K, V, T> extends AbstractItemStreamItemWriter<T> {

    @Getter
    @Setter
    private Command<K, V, T> command;
    @Getter
    @Setter
    private long timeout;

    protected AbstractRedisItemWriter(Command<K, V, T> command, Duration timeout) {
        setName(ClassUtils.getShortName(getClass()));
        this.command = command;
        this.timeout = timeout == null ? RedisURI.DEFAULT_TIMEOUT : timeout.getSeconds();
    }

    protected void write(List<? extends T> items, BaseRedisAsyncCommands<K, V> commands) {
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
