package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class ConnectionPoolBuilder {

    private Integer maxTotal;
    private Integer maxIdle;
    private Integer minIdle;

    public ConnectionPoolBuilder maxTotal(Integer maxTotal) {
        this.maxTotal = maxTotal;
        return this;
    }

    public ConnectionPoolBuilder maxIdle(Integer maxIdle) {
        this.maxIdle = maxIdle;
        return this;
    }

    public ConnectionPoolBuilder minIdle(Integer minIdle) {
        this.minIdle = minIdle;
        return this;
    }

    public GenericObjectPool<StatefulRedisConnection<String, String>> build(RedisClient client) {
        return ConnectionPoolSupport.createGenericObjectPool(client::connect, poolConfig());
    }

    public GenericObjectPool<StatefulRedisClusterConnection<String, String>> build(RedisClusterClient client) {
        return ConnectionPoolSupport.createGenericObjectPool(client::connect, poolConfig());
    }

    private <T extends StatefulConnection<String, String>> GenericObjectPoolConfig<T> poolConfig() {
        GenericObjectPoolConfig<T> config = new GenericObjectPoolConfig<>();
        if (maxTotal != null) {
            config.setMaxTotal(maxTotal);
        }
        if (maxIdle != null) {
            config.setMaxIdle(maxIdle);
        }
        if (minIdle != null) {
            config.setMaxIdle(minIdle);
        }
        return config;
    }
}
