package org.springframework.batch.item.redis.support;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.batch.item.redis.RedisItemWriter;
import org.springframework.util.Assert;

@Setter
@Accessors(fluent = true)
public class RedisItemWriterBuilder<T> {

    private RedisOptions redisOptions = RedisOptions.builder().build();
    private ClientOptions clientOptions;
    private WriteCommand<String, String, T> writeCommand;

    public RedisItemWriter<String, String, StatefulRedisConnection<String, String>, T> build() {
        Assert.notNull(redisOptions, "A RedisOptions instance is required.");
        Assert.notNull(writeCommand, "A write command is required.");
        return new RedisItemWriter<String, String, StatefulRedisConnection<String, String>, T>(redisOptions.connectionPool(redisOptions.client(clientOptions)), StatefulRedisConnection::async, writeCommand, redisOptions.getRedisURI().getTimeout().getSeconds());
    }

}