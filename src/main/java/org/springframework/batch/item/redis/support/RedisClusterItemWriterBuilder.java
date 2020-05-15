package org.springframework.batch.item.redis.support;

import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.batch.item.redis.RedisItemWriter;
import org.springframework.util.Assert;

@Setter
@Accessors(fluent = true)
public class RedisClusterItemWriterBuilder<T> {

    private RedisOptions redisOptions = RedisOptions.builder().build();
    private ClusterClientOptions clientOptions;
    private WriteCommand<String, String, T> writeCommand;

    public RedisItemWriter<String, String, StatefulRedisClusterConnection<String, String>, T> build() {
        Assert.notNull(redisOptions, "Redis options are required.");
        Assert.notNull(writeCommand, "A write command is required.");
        return new RedisItemWriter<String, String, StatefulRedisClusterConnection<String, String>, T>(redisOptions.connectionPool(redisOptions.client(clientOptions)), StatefulRedisClusterConnection::async, writeCommand, redisOptions.getRedisURI().getTimeout().getSeconds());
    }

}