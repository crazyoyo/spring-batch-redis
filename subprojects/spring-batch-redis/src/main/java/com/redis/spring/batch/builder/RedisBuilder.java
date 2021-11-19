package com.redis.spring.batch.builder;

import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.RedisCodec;

public class RedisBuilder<K, V, B extends RedisBuilder<K, V, B>> {

	protected final AbstractRedisClient client;
	protected final RedisCodec<K, V> codec;
	protected GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig = new GenericObjectPoolConfig<>();

	public RedisBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
		this.client = client;
		this.codec = codec;
	}
	
	public AbstractRedisClient getClient() {
		return client;
	}
	
	public RedisCodec<K, V> getCodec() {
		return codec;
	}

	@SuppressWarnings("unchecked")
	public B poolConfig(GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig) {
		this.poolConfig = poolConfig;
		return (B) this;
	}

	protected Supplier<StatefulConnection<K, V>> connectionSupplier() {
		if (client instanceof RedisClusterClient) {
			return () -> ((RedisClusterClient) client).connect(codec);
		}
		return () -> ((RedisClient) client).connect(codec);
	}

	protected Function<StatefulConnection<K, V>, BaseRedisCommands<K, V>> sync() {
		if (client instanceof RedisClusterClient) {
			return c -> ((StatefulRedisClusterConnection<K, V>) c).sync();
		}
		return c -> ((StatefulRedisConnection<K, V>) c).sync();
	}

	protected Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> async() {
		if (client instanceof RedisClusterClient) {
			return c -> ((StatefulRedisClusterConnection<K, V>) c).async();
		}
		return c -> ((StatefulRedisConnection<K, V>) c).async();
	}

}
