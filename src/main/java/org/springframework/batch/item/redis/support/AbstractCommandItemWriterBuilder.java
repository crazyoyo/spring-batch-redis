package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.function.BiFunction;

public class AbstractCommandItemWriterBuilder<K, V, T, B extends AbstractCommandItemWriterBuilder<K, V, T, B>> {

    private BiFunction<BaseRedisAsyncCommands<K, V>, T, RedisFuture<?>> command;
    protected Duration timeout = RedisURI.DEFAULT_TIMEOUT_DURATION;

    protected BiFunction<BaseRedisAsyncCommands<K, V>, T, RedisFuture<?>> getCommand() {
        Assert.notNull(command, "A command is required.");
        return command;
    }

    public B command(BiFunction<?, T, RedisFuture<?>> command) {
        this.command = (BiFunction) command;
        return (B) this;
    }

    public B timeout(Duration timeout) {
        this.timeout = timeout;
        return (B) this;
    }

}