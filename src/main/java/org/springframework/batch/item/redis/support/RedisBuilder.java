package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.support.ConnectionPoolSupport;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

@Setter
@Accessors(fluent = true)
public class RedisBuilder {

    public GenericObjectPool<StatefulRedisConnection<String, String>> connectionPool(RedisClient client, int maxTotal, int minIdle, int maxIdle) {
        return ConnectionPoolSupport.createGenericObjectPool(client::connect, poolConfig(maxTotal, minIdle, maxIdle));
    }

    public GenericObjectPool<StatefulRedisClusterConnection<String, String>> connectionPool(RedisClusterClient client, int maxTotal, int minIdle, int maxIdle) {
        return ConnectionPoolSupport.createGenericObjectPool(client::connect, poolConfig(maxTotal, minIdle, maxIdle));
    }

    public RedisClient client(RedisURI redisURI) {
        return RedisClient.create(redisURI);
    }

    public RedisClusterClient clusterClient(RedisURI redisURI) {
        return RedisClusterClient.create(redisURI);
    }

    private <T extends StatefulConnection<String, String>> GenericObjectPoolConfig<T> poolConfig(int maxTotal, int minIdle, int maxIdle) {
        GenericObjectPoolConfig<T> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(maxTotal);
        config.setMinIdle(minIdle);
        config.setMaxIdle(maxIdle);
        return config;
    }


}
