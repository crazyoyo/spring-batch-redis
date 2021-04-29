package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.function.Function;
import java.util.function.Supplier;

public class CommandBuilder<B extends CommandBuilder<B>> {

    protected GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig = new GenericObjectPoolConfig<>();
    protected final Supplier<StatefulConnection<String, String>> connectionSupplier;
    protected final Supplier<StatefulRedisPubSubConnection<String, String>> pubSubConnectionSupplier;
    protected final Function<StatefulConnection<String, String>, BaseRedisCommands<String, String>> sync;
    protected final Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> async;

    public CommandBuilder(RedisClient client) {
        this.connectionSupplier = client::connect;
        this.pubSubConnectionSupplier = client::connectPubSub;
        this.async = c -> ((StatefulRedisConnection<String, String>) c).async();
        this.sync = c -> ((StatefulRedisConnection<String, String>) c).sync();
    }

    public CommandBuilder(RedisClusterClient client) {
        this.connectionSupplier = client::connect;
        this.pubSubConnectionSupplier = client::connectPubSub;
        this.async = c -> ((StatefulRedisClusterConnection<String, String>) c).async();
        this.sync = c -> ((StatefulRedisClusterConnection<String, String>) c).sync();
    }

    @SuppressWarnings("unchecked")
    public B poolConfig(GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig) {
        this.poolConfig = poolConfig;
        return (B) this;
    }

}