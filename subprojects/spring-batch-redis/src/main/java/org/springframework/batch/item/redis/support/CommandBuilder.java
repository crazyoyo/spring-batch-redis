package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.function.Function;
import java.util.function.Supplier;

public class CommandBuilder<K, V, B extends CommandBuilder<K, V, B>> {

    protected GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig = new GenericObjectPoolConfig<>();
    protected final Supplier<StatefulConnection<K, V>> connectionSupplier;
    protected final Supplier<StatefulRedisPubSubConnection<K, V>> pubSubConnectionSupplier;
    protected final Function<StatefulConnection<K, V>, BaseRedisCommands<K, V>> sync;
    protected final Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> async;

    public CommandBuilder(RedisClient client, RedisCodec<K, V> codec) {
        this.connectionSupplier = () -> client.connect(codec);
        this.pubSubConnectionSupplier = () -> client.connectPubSub(codec);
        this.async = c -> ((StatefulRedisConnection<K, V>) c).async();
        this.sync = c -> ((StatefulRedisConnection<K, V>) c).sync();
    }

    public CommandBuilder(RedisClusterClient client, RedisCodec<K, V> codec) {
        this.connectionSupplier = () -> client.connect(codec);
        this.pubSubConnectionSupplier = () -> client.connectPubSub(codec);
        this.async = c -> ((StatefulRedisClusterConnection<K, V>) c).async();
        this.sync = c -> ((StatefulRedisClusterConnection<K, V>) c).sync();
    }

    @SuppressWarnings("unchecked")
    public B poolConfig(GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig) {
        this.poolConfig = poolConfig;
        return (B) this;
    }

}
