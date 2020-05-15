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
import lombok.NonNull;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

@Builder
public class RedisOptions {

    @Getter
    @NonNull
    @Builder.Default
    private RedisURI redisURI = RedisURI.create("localhost", RedisURI.DEFAULT_REDIS_PORT);
    @NonNull
    @Builder.Default
    private ClientResources clientResources = ClientResources.builder().build();
    @NonNull
    @Builder.Default
    private PoolOptions poolOptions = PoolOptions.builder().build();

    public RedisClient client(ClientOptions options) {
        if (clientResources == null) {
            return setOptions(RedisClient.create(redisURI), options);
        }
        return setOptions(RedisClient.create(clientResources, redisURI), options);
    }

    private RedisClient setOptions(RedisClient client, ClientOptions options) {
        if (options != null) {
            client.setOptions(options);
        }
        return client;
    }

    public RedisClusterClient client(ClusterClientOptions options) {
        if (clientResources == null) {
            return setOptions(RedisClusterClient.create(redisURI), options);
        }
        return setOptions(RedisClusterClient.create(clientResources, redisURI), options);
    }

    private RedisClusterClient setOptions(RedisClusterClient client, ClusterClientOptions options) {
        if (options != null) {
            client.setOptions(options);
        }
        return client;
    }

    public GenericObjectPool<StatefulRedisConnection<String, String>> connectionPool(RedisClient client) {
        return ConnectionPoolSupport.createGenericObjectPool(client::connect, config());
    }

    public GenericObjectPool<StatefulRedisClusterConnection<String, String>> connectionPool(RedisClusterClient client) {
        return ConnectionPoolSupport.createGenericObjectPool(client::connect, config());
    }

    private <T extends StatefulConnection<String, String>> GenericObjectPoolConfig<T> config() {
        GenericObjectPoolConfig<T> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(poolOptions.getMaxTotal());
        config.setMinIdle(poolOptions.getMinIdle());
        config.setMaxIdle(poolOptions.getMaxIdle());
        return config;
    }


}
