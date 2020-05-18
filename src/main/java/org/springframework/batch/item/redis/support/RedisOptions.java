package org.springframework.batch.item.redis.support;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.support.ConnectionPoolSupport;
import lombok.Builder;
import lombok.Getter;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

@Builder
public class RedisOptions {

    private final static RedisURI DEFAULT_REDIS_URI = RedisURI.create("localhost", RedisURI.DEFAULT_REDIS_PORT);

    @Getter
    @Builder.Default
    private final RedisURI redisURI = DEFAULT_REDIS_URI;
    private final ClientResources clientResources;
    @Builder.Default
    private final PoolOptions poolOptions = PoolOptions.builder().build();
    @Getter
    private final boolean cluster;
    @Builder.Default
    private final ClientOptions clientOptions = ClientOptions.builder().build();
    @Builder.Default
    private final ClusterClientOptions clusterClientOptions = ClusterClientOptions.builder().build();

    private <K, V, T extends StatefulConnection<K, V>> GenericObjectPoolConfig<T> poolConfig() {
        GenericObjectPoolConfig<T> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(poolOptions.getMaxTotal());
        config.setMinIdle(poolOptions.getMinIdle());
        config.setMaxIdle(poolOptions.getMaxIdle());
        return config;
    }

    public long getCommandTimeout() {
        return redisURI.getTimeout().getSeconds();
    }

    public RedisClient redisClient() {
        RedisClient client = clientResources == null ? RedisClient.create(redisURI) : RedisClient.create(clientResources, redisURI);
        client.setOptions(clientOptions);
        return client;
    }

    public GenericObjectPool<StatefulRedisConnection<String, String>> redisConnectionPool() {
        return redisConnectionPool(redisClient());
    }

    public GenericObjectPool<StatefulRedisConnection<String, String>> redisConnectionPool(RedisClient client) {
        return ConnectionPoolSupport.createGenericObjectPool(client::connect, poolConfig());
    }

    public RedisClusterClient redisClusterClient() {
        RedisClusterClient client = clientResources == null ? RedisClusterClient.create(redisURI) : RedisClusterClient.create(clientResources, redisURI);
        client.setOptions(clusterClientOptions);
        return client;
    }

    public GenericObjectPool<StatefulRedisClusterConnection<String, String>> redisClusterConnectionPool() {
        return redisClusterConnectionPool(redisClusterClient());
    }

    public GenericObjectPool<StatefulRedisClusterConnection<String, String>> redisClusterConnectionPool(RedisClusterClient client) {
        return ConnectionPoolSupport.createGenericObjectPool(client::connect, poolConfig());
    }


}
