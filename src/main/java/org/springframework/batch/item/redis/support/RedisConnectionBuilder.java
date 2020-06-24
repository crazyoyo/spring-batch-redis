package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.time.Duration;
import java.util.function.Function;
import java.util.function.Supplier;

@SuppressWarnings("unchecked")
public class RedisConnectionBuilder<B extends RedisConnectionBuilder<B>> {

    private RedisURI redisURI;
    private ClientResources clientResources;
    private ClusterClientOptions clientOptions;
    private ConnectionPoolConfig poolConfig = ConnectionPoolConfig.builder().build();
    private boolean cluster;

    public RedisURI redisURI() {
        return redisURI;
    }

    public Duration timeout() {
        return redisURI.getTimeout();
    }

    public B redisURI(RedisURI redisURI) {
        this.redisURI = redisURI;
        return (B) this;
    }

    public B clientResources(ClientResources clientResources) {
        this.clientResources = clientResources;
        return (B) this;
    }

    public B poolConfig(ConnectionPoolConfig poolConfig) {
        this.poolConfig = poolConfig;
        return (B) this;
    }

    public B clientOptions(ClusterClientOptions clientOptions) {
        this.clientOptions = clientOptions;
        return (B) this;
    }

    public B cluster(boolean cluster) {
        this.cluster = cluster;
        return (B) this;
    }

    public Supplier<StatefulConnection<String, String>> connectionSupplier() {
        if (cluster) {
            return redisClusterClient()::connect;
        }
        return redisClient()::connect;
    }

    public StatefulConnection<String, String> connection() {
        if (cluster) {
            return redisClusterClient().connect();
        }
        return redisClient().connect();
    }

    public StatefulRedisPubSubConnection<String, String> pubSubConnection() {
        if (cluster) {
            return redisClusterClient().connectPubSub();
        }
        return redisClient().connectPubSub();
    }

    public RedisClient redisClient() {
        RedisClient client = createRedisClient(redisURI, clientResources);
        if (clientOptions != null) {
            client.setOptions(clientOptions);
        }
        return client;
    }

    public RedisClusterClient redisClusterClient() {
        RedisClusterClient client = createRedisClusterClient(redisURI, clientResources);
        if (clientOptions != null) {
            client.setOptions(clientOptions);
        }
        return client;
    }

    private RedisClient createRedisClient(RedisURI redisURI, ClientResources clientResources) {
        if (clientResources == null) {
            return RedisClient.create(redisURI);
        }
        return RedisClient.create(clientResources, redisURI);
    }

    private RedisClusterClient createRedisClusterClient(RedisURI redisURI, ClientResources clientResources) {
        if (clientResources == null) {
            return RedisClusterClient.create(redisURI);
        }
        return RedisClusterClient.create(clientResources, redisURI);
    }

    public Function<StatefulConnection<String, String>, BaseRedisCommands<String, String>> sync() {
        if (cluster) {
            return c -> ((StatefulRedisClusterConnection<String, String>) c).sync();
        }
        return c -> ((StatefulRedisConnection<String, String>) c).sync();
    }

    public Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> async() {
        if (cluster) {
            return c -> ((StatefulRedisClusterConnection<String, String>) c).async();
        }
        return c -> ((StatefulRedisConnection<String, String>) c).async();
    }

    public GenericObjectPool<StatefulConnection<String, String>> pool() {
        return ConnectionPoolSupport.createGenericObjectPool(connectionSupplier(), poolConfig.config());
    }

}
