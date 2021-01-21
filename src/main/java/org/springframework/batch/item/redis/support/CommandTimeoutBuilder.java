package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisURI;

import java.time.Duration;

public class CommandTimeoutBuilder<B extends CommandTimeoutBuilder<B>> {

    protected Duration commandTimeout = RedisURI.DEFAULT_TIMEOUT_DURATION;

    public B commandTimeout(Duration commandTimeout) {
        this.commandTimeout = commandTimeout;
        return (B) this;
    }

}
