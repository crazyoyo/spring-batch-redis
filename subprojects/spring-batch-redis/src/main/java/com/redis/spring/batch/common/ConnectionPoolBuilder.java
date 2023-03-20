package com.redis.spring.batch.common;

import java.util.Optional;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.EvictionPolicy;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.support.ConnectionPoolSupport;

public class ConnectionPoolBuilder {

	private final AbstractRedisClient client;
	private Optional<ReadFrom> readFrom = Optional.empty();
	private PoolOptions options = PoolOptions.builder().build();

	public ConnectionPoolBuilder(AbstractRedisClient client) {
		this.client = client;
	}

	public ConnectionPoolBuilder readFrom(ReadFrom readFrom) {
		return readFrom(Optional.of(readFrom));
	}

	public ConnectionPoolBuilder readFrom(Optional<ReadFrom> readFrom) {
		this.readFrom = readFrom;
		return this;
	}

	public ConnectionPoolBuilder options(PoolOptions options) {
		this.options = options;
		return this;
	}

	@SuppressWarnings("unchecked")
	public <K, V> GenericObjectPoolConfig<StatefulConnection<K, V>> config() {
		GenericObjectPoolConfig<StatefulConnection<K, V>> config = new GenericObjectPoolConfig<>();
		config.setBlockWhenExhausted(options.isBlockWhenExhausted());
		options.getEvictionPolicy()
				.ifPresent(p -> config.setEvictionPolicy((EvictionPolicy<StatefulConnection<K, V>>) p));
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

	public GenericObjectPool<StatefulConnection<String, String>> build() {
		return codec(StringCodec.UTF8);
	}

	public <K, V> GenericObjectPool<StatefulConnection<K, V>> codec(RedisCodec<K, V> codec) {
		return ConnectionPoolSupport.createGenericObjectPool(connectionSupplier(codec), config());
	}

	private <K, V> Supplier<StatefulConnection<K, V>> connectionSupplier(RedisCodec<K, V> codec) {
		if (client instanceof RedisClusterClient) {
			return () -> {
				StatefulRedisClusterConnection<K, V> connection = ((RedisClusterClient) client).connect(codec);
				readFrom.ifPresent(connection::setReadFrom);
				return connection;
			};
		}
		return () -> ((RedisClient) client).connect(codec);
	}

	public static ConnectionPoolBuilder client(AbstractRedisClient client) {
		return new ConnectionPoolBuilder(client);
	}

}