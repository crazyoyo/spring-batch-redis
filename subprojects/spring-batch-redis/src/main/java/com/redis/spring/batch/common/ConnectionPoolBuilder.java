package com.redis.spring.batch.common;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.BaseObjectPoolConfig;
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

public final class ConnectionPoolBuilder {

	public static final boolean DEFAULT_LIFO = BaseObjectPoolConfig.DEFAULT_LIFO;
	public static final boolean DEFAULT_FAIRNESS = BaseObjectPoolConfig.DEFAULT_FAIRNESS;
	public static final Duration DEFAULT_MAX_WAIT = BaseObjectPoolConfig.DEFAULT_MAX_WAIT;
	public static final Duration DEFAULT_MIN_EVICTABLE_IDLE_DURATION = BaseObjectPoolConfig.DEFAULT_MIN_EVICTABLE_IDLE_DURATION;
	public static final Duration DEFAULT_EVICTOR_SHUTDOWN_TIMEOUT = BaseObjectPoolConfig.DEFAULT_EVICTOR_SHUTDOWN_TIMEOUT;
	public static final Duration DEFAULT_SOFT_MIN_EVICTABLE_IDLE_DURATION = BaseObjectPoolConfig.DEFAULT_SOFT_MIN_EVICTABLE_IDLE_DURATION;
	public static final int DEFAULT_NUM_TESTS_PER_EVICTION_RUN = BaseObjectPoolConfig.DEFAULT_NUM_TESTS_PER_EVICTION_RUN;
	public static final String DEFAULT_EVICTION_POLICY_CLASS_NAME = BaseObjectPoolConfig.DEFAULT_EVICTION_POLICY_CLASS_NAME;
	public static final boolean DEFAULT_TEST_ON_CREATE = BaseObjectPoolConfig.DEFAULT_TEST_ON_CREATE;
	public static final boolean DEFAULT_TEST_ON_BORROW = BaseObjectPoolConfig.DEFAULT_TEST_ON_BORROW;
	public static final boolean DEFAULT_TEST_ON_RETURN = BaseObjectPoolConfig.DEFAULT_TEST_ON_RETURN;
	public static final boolean DEFAULT_TEST_WHILE_IDLE = BaseObjectPoolConfig.DEFAULT_TEST_WHILE_IDLE;
	public static final Duration DEFAULT_TIME_BETWEEN_EVICTION_RUNS = BaseObjectPoolConfig.DEFAULT_TIME_BETWEEN_EVICTION_RUNS;
	public static final boolean DEFAULT_BLOCK_WHEN_EXHAUSTED = BaseObjectPoolConfig.DEFAULT_BLOCK_WHEN_EXHAUSTED;
	public static final boolean DEFAULT_JMX_ENABLE = BaseObjectPoolConfig.DEFAULT_JMX_ENABLE;
	public static final String DEFAULT_JMX_NAME_PREFIX = BaseObjectPoolConfig.DEFAULT_JMX_NAME_PREFIX;
	public static final String DEFAULT_JMX_NAME_BASE = BaseObjectPoolConfig.DEFAULT_JMX_NAME_BASE;
	public static final int DEFAULT_MAX_TOTAL = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
	public static final int DEFAULT_MAX_IDLE = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
	public static final int DEFAULT_MIN_IDLE = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;

	Optional<ReadFrom> readFrom = Optional.empty();
	boolean lifo = DEFAULT_LIFO;
	boolean fairness = DEFAULT_FAIRNESS;
	Duration maxWaitDuration = DEFAULT_MAX_WAIT;
	Duration minEvictableIdleDuration = DEFAULT_MIN_EVICTABLE_IDLE_DURATION;
	Duration evictorShutdownTimeoutDuration = DEFAULT_EVICTOR_SHUTDOWN_TIMEOUT;
	Duration softMinEvictableIdleDuration = DEFAULT_SOFT_MIN_EVICTABLE_IDLE_DURATION;
	int numTestsPerEvictionRun = DEFAULT_NUM_TESTS_PER_EVICTION_RUN;
	Optional<EvictionPolicy<?>> evictionPolicy = Optional.empty();
	String evictionPolicyClassName = DEFAULT_EVICTION_POLICY_CLASS_NAME;
	boolean testOnCreate = DEFAULT_TEST_ON_CREATE;
	boolean testOnBorrow = DEFAULT_TEST_ON_BORROW;
	boolean testOnReturn = DEFAULT_TEST_ON_RETURN;
	boolean testWhileIdle = DEFAULT_TEST_WHILE_IDLE;
	Duration durationBetweenEvictionRuns = DEFAULT_TIME_BETWEEN_EVICTION_RUNS;
	boolean blockWhenExhausted = DEFAULT_BLOCK_WHEN_EXHAUSTED;
	boolean jmxEnabled = DEFAULT_JMX_ENABLE;
	String jmxNamePrefix = DEFAULT_JMX_NAME_PREFIX;
	String jmxNameBase = DEFAULT_JMX_NAME_BASE;
	int maxTotal = DEFAULT_MAX_TOTAL;
	int maxIdle = DEFAULT_MAX_IDLE;
	int minIdle = DEFAULT_MIN_IDLE;

	private final AbstractRedisClient client;

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

	public ConnectionPoolBuilder lifo(boolean lifo) {
		this.lifo = lifo;
		return this;
	}

	public ConnectionPoolBuilder fairness(boolean fairness) {
		this.fairness = fairness;
		return this;
	}

	public ConnectionPoolBuilder maxWaitDuration(Duration maxWaitDuration) {
		this.maxWaitDuration = maxWaitDuration;
		return this;
	}

	public ConnectionPoolBuilder minEvictableIdleDuration(Duration minEvictableIdleDuration) {
		this.minEvictableIdleDuration = minEvictableIdleDuration;
		return this;
	}

	public ConnectionPoolBuilder evictorShutdownTimeoutDuration(Duration evictorShutdownTimeoutDuration) {
		this.evictorShutdownTimeoutDuration = evictorShutdownTimeoutDuration;
		return this;
	}

	public ConnectionPoolBuilder softMinEvictableIdleDuration(Duration softMinEvictableIdleDuration) {
		this.softMinEvictableIdleDuration = softMinEvictableIdleDuration;
		return this;
	}

	public ConnectionPoolBuilder numTestsPerEvictionRun(int numTestsPerEvictionRun) {
		this.numTestsPerEvictionRun = numTestsPerEvictionRun;
		return this;
	}

	public ConnectionPoolBuilder evictionPolicy(EvictionPolicy<?> evictionPolicy) {
		return evictionPolicy(Optional.of(evictionPolicy));
	}

	public ConnectionPoolBuilder evictionPolicy(Optional<EvictionPolicy<?>> evictionPolicy) {
		this.evictionPolicy = evictionPolicy;
		return this;
	}

	public ConnectionPoolBuilder evictionPolicyClassName(String evictionPolicyClassName) {
		this.evictionPolicyClassName = evictionPolicyClassName;
		return this;
	}

	public ConnectionPoolBuilder testOnCreate(boolean testOnCreate) {
		this.testOnCreate = testOnCreate;
		return this;
	}

	public ConnectionPoolBuilder testOnBorrow(boolean testOnBorrow) {
		this.testOnBorrow = testOnBorrow;
		return this;
	}

	public ConnectionPoolBuilder testOnReturn(boolean testOnReturn) {
		this.testOnReturn = testOnReturn;
		return this;
	}

	public ConnectionPoolBuilder testWhileIdle(boolean testWhileIdle) {
		this.testWhileIdle = testWhileIdle;
		return this;
	}

	public ConnectionPoolBuilder durationBetweenEvictionRuns(Duration durationBetweenEvictionRuns) {
		this.durationBetweenEvictionRuns = durationBetweenEvictionRuns;
		return this;
	}

	public ConnectionPoolBuilder blockWhenExhausted(boolean blockWhenExhausted) {
		this.blockWhenExhausted = blockWhenExhausted;
		return this;
	}

	public ConnectionPoolBuilder jmxEnabled(boolean jmxEnabled) {
		this.jmxEnabled = jmxEnabled;
		return this;
	}

	public ConnectionPoolBuilder jmxNamePrefix(String jmxNamePrefix) {
		this.jmxNamePrefix = jmxNamePrefix;
		return this;
	}

	public ConnectionPoolBuilder jmxNameBase(String jmxNameBase) {
		this.jmxNameBase = jmxNameBase;
		return this;
	}

	public ConnectionPoolBuilder maxTotal(int maxTotal) {
		this.maxTotal = maxTotal;
		return this;
	}

	public ConnectionPoolBuilder maxIdle(int maxIdle) {
		this.maxIdle = maxIdle;
		return this;
	}

	public ConnectionPoolBuilder minIdle(int minIdle) {
		this.minIdle = minIdle;
		return this;
	}

	@SuppressWarnings("unchecked")
	public <K, V> GenericObjectPoolConfig<StatefulConnection<K, V>> config() {
		GenericObjectPoolConfig<StatefulConnection<K, V>> config = new GenericObjectPoolConfig<>();
		config.setBlockWhenExhausted(blockWhenExhausted);
		evictionPolicy.ifPresent(p -> config.setEvictionPolicy((EvictionPolicy<StatefulConnection<K, V>>) p));
		config.setEvictionPolicyClassName(evictionPolicyClassName);
		config.setEvictorShutdownTimeout(evictorShutdownTimeoutDuration);
		config.setFairness(fairness);
		config.setJmxEnabled(jmxEnabled);
		config.setJmxNameBase(jmxNameBase);
		config.setJmxNamePrefix(jmxNamePrefix);
		config.setLifo(lifo);
		config.setMaxIdle(maxIdle);
		config.setMaxTotal(maxTotal);
		config.setMaxWait(maxWaitDuration);
		config.setMinEvictableIdleTime(minEvictableIdleDuration);
		config.setMinIdle(minIdle);
		config.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
		config.setSoftMinEvictableIdleTime(softMinEvictableIdleDuration);
		config.setTestOnBorrow(testOnBorrow);
		config.setTestOnCreate(testOnCreate);
		config.setTestOnReturn(testOnReturn);
		config.setTestWhileIdle(testWhileIdle);
		config.setTimeBetweenEvictionRuns(durationBetweenEvictionRuns);
		return config;
	}

	public GenericObjectPool<StatefulConnection<String, String>> build() {
		return build(StringCodec.UTF8);
	}

	public <K, V> GenericObjectPool<StatefulConnection<K, V>> build(RedisCodec<K, V> codec) {
		return ConnectionPoolSupport.createGenericObjectPool(connectionSupplier(client, codec), config());
	}

	public <K, V> Supplier<StatefulConnection<K, V>> connectionSupplier(AbstractRedisClient client,
			RedisCodec<K, V> codec) {
		if (client instanceof RedisClusterClient) {
			return () -> {
				StatefulRedisClusterConnection<K, V> connection = ((RedisClusterClient) client).connect(codec);
				readFrom.ifPresent(connection::setReadFrom);
				return connection;
			};
		}
		return () -> ((RedisClient) client).connect(codec);
	}

	public static ConnectionPoolBuilder create(AbstractRedisClient client) {
		return new ConnectionPoolBuilder(client);
	}

}