package com.redis.spring.batch.support;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.cluster.api.StatefulRedisModulesClusterConnection;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.function.Function;
import java.util.function.Supplier;

public class CommandBuilder<K, V, B extends CommandBuilder<K, V, B>> {

    protected final AbstractRedisClient client;
    protected final RedisCodec<K, V> codec;
    protected GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig = new GenericObjectPoolConfig<>();

    public CommandBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
        this.client = client;
        this.codec = codec;
    }

    @SuppressWarnings("unchecked")
    public B poolConfig(GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig) {
        this.poolConfig = poolConfig;
        return (B) this;
    }

    protected Supplier<StatefulConnection<K, V>> connectionSupplier() {
        if (client instanceof RedisModulesClusterClient) {
            return () -> ((RedisModulesClusterClient) client).connect(codec);
        }
        return () -> ((RedisModulesClient) client).connect(codec);
    }

    protected Supplier<StatefulRedisPubSubConnection<K, V>> pubSubConnectionSupplier() {
        if (client instanceof RedisModulesClusterClient) {
            return () -> ((RedisModulesClusterClient) client).connectPubSub(codec);
        }
        return () -> ((RedisModulesClient) client).connectPubSub(codec);
    }

    protected Function<StatefulConnection<K, V>, RedisModulesCommands<K, V>> sync() {
        if (client instanceof RedisModulesClusterClient) {
            return c -> ((StatefulRedisModulesClusterConnection<K, V>) c).sync();
        }
        return c -> ((StatefulRedisModulesConnection<K, V>) c).sync();
    }

    protected Function<StatefulConnection<K, V>, RedisModulesAsyncCommands<K, V>> async() {
        if (client instanceof RedisModulesClusterClient) {
            return c -> ((StatefulRedisModulesClusterConnection<K, V>) c).async();
        }
        return c -> ((StatefulRedisModulesConnection<K, V>) c).async();
    }

}
