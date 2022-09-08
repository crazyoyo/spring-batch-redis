package com.redis.spring.batch.common;

import java.util.function.Supplier;

import org.apache.commons.pool2.impl.EvictionPolicy;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.support.ConnectionPoolSupport;

public class RedisConnectionPoolBuilder {

	private final RedisConnectionPoolOptions options;

	public RedisConnectionPoolBuilder() {
		this.options = RedisConnectionPoolOptions.builder().build();
	}

	public RedisConnectionPoolBuilder(RedisConnectionPoolOptions options) {
		this.options = options;
	}

	public RedisConnectionPoolOptions getOptions() {
		return options;
	}

	@SuppressWarnings("unchecked")
	public <K, V> GenericObjectPoolConfig<StatefulConnection<K, V>> config() {
		GenericObjectPoolConfig<StatefulConnection<K, V>> config = new GenericObjectPoolConfig<>();
		config.setBlockWhenExhausted(options.isBlockWhenExhausted());
		config.setEvictionPolicy((EvictionPolicy<StatefulConnection<K, V>>) options.getEvictionPolicy());
		config.setEvictionPolicyClassName(options.getEvictionPolicyClassName());
		config.setEvictorShutdownTimeout(options.getEvictorShutdownTimeoutDuration());
		config.setFairness(options.isFairness());
		config.setJmxEnabled(options.isJmxEnabled());
		config.setJmxNameBase(options.getJmxNameBase());
		config.setJmxNamePrefix(options.getJmxNamePrefix());
		config.setLifo(options.isLifo());
		config.setMaxIdle(options.getMaxIdle());
		config.setMaxTotal(options.getMaxTotal());
		config.setMaxWait(options.getMaxWaitDuration());
		config.setMinEvictableIdleTime(options.getMinEvictableIdleDuration());
		config.setMinIdle(options.getMinIdle());
		config.setNumTestsPerEvictionRun(options.getNumTestsPerEvictionRun());
		config.setSoftMinEvictableIdleTime(options.getSoftMinEvictableIdleDuration());
		config.setTestOnBorrow(options.isTestOnBorrow());
		config.setTestOnCreate(options.isTestOnCreate());
		config.setTestOnReturn(options.isTestOnReturn());
		config.setTestWhileIdle(options.isTestWhileIdle());
		config.setTimeBetweenEvictionRuns(options.getDurationBetweenEvictionRuns());
		return config;
	}

	public GenericObjectPool<StatefulConnection<String, String>> pool(AbstractRedisClient client) {
		return pool(client, StringCodec.UTF8);
	}

	public <K, V> GenericObjectPool<StatefulConnection<K, V>> pool(AbstractRedisClient client, RedisCodec<K, V> codec) {
		return ConnectionPoolSupport.createGenericObjectPool(connectionSupplier(client, codec), config());
	}

	public <K, V> Supplier<StatefulConnection<K, V>> connectionSupplier(AbstractRedisClient client,
			RedisCodec<K, V> codec) {
		if (client instanceof RedisClusterClient) {
			return () -> {
				StatefulRedisClusterConnection<K, V> connection = ((RedisClusterClient) client).connect(codec);
				options.getReadFrom().ifPresent(connection::setReadFrom);
				return connection;
			};
		}
		return () -> ((RedisClient) client).connect(codec);
	}

	public static RedisConnectionPoolBuilder create() {
		return new RedisConnectionPoolBuilder();
	}

	public static RedisConnectionPoolBuilder create(RedisConnectionPoolOptions options) {
		return new RedisConnectionPoolBuilder(options);
	}
}
