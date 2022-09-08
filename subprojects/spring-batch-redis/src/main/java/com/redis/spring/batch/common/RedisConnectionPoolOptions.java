package com.redis.spring.batch.common;

import static org.apache.commons.pool2.impl.BaseObjectPoolConfig.DEFAULT_BLOCK_WHEN_EXHAUSTED;
import static org.apache.commons.pool2.impl.BaseObjectPoolConfig.DEFAULT_EVICTION_POLICY_CLASS_NAME;
import static org.apache.commons.pool2.impl.BaseObjectPoolConfig.DEFAULT_EVICTOR_SHUTDOWN_TIMEOUT;
import static org.apache.commons.pool2.impl.BaseObjectPoolConfig.DEFAULT_FAIRNESS;
import static org.apache.commons.pool2.impl.BaseObjectPoolConfig.DEFAULT_JMX_ENABLE;
import static org.apache.commons.pool2.impl.BaseObjectPoolConfig.DEFAULT_JMX_NAME_BASE;
import static org.apache.commons.pool2.impl.BaseObjectPoolConfig.DEFAULT_JMX_NAME_PREFIX;
import static org.apache.commons.pool2.impl.BaseObjectPoolConfig.DEFAULT_LIFO;
import static org.apache.commons.pool2.impl.BaseObjectPoolConfig.DEFAULT_MAX_WAIT;
import static org.apache.commons.pool2.impl.BaseObjectPoolConfig.DEFAULT_MIN_EVICTABLE_IDLE_DURATION;
import static org.apache.commons.pool2.impl.BaseObjectPoolConfig.DEFAULT_NUM_TESTS_PER_EVICTION_RUN;
import static org.apache.commons.pool2.impl.BaseObjectPoolConfig.DEFAULT_SOFT_MIN_EVICTABLE_IDLE_DURATION;
import static org.apache.commons.pool2.impl.BaseObjectPoolConfig.DEFAULT_TEST_ON_BORROW;
import static org.apache.commons.pool2.impl.BaseObjectPoolConfig.DEFAULT_TEST_ON_CREATE;
import static org.apache.commons.pool2.impl.BaseObjectPoolConfig.DEFAULT_TEST_ON_RETURN;
import static org.apache.commons.pool2.impl.BaseObjectPoolConfig.DEFAULT_TEST_WHILE_IDLE;
import static org.apache.commons.pool2.impl.BaseObjectPoolConfig.DEFAULT_TIME_BETWEEN_EVICTION_RUNS;
import static org.apache.commons.pool2.impl.GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
import static org.apache.commons.pool2.impl.GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
import static org.apache.commons.pool2.impl.GenericObjectPoolConfig.DEFAULT_MIN_IDLE;

import java.time.Duration;
import java.util.Optional;

import org.apache.commons.pool2.impl.EvictionPolicy;

import io.lettuce.core.ReadFrom;

public class RedisConnectionPoolOptions {

	private Optional<ReadFrom> readFrom = Optional.empty();
	private boolean lifo = DEFAULT_LIFO;
	private boolean fairness = DEFAULT_FAIRNESS;
	private Duration maxWaitDuration = DEFAULT_MAX_WAIT;
	private Duration minEvictableIdleDuration = DEFAULT_MIN_EVICTABLE_IDLE_DURATION;
	private Duration evictorShutdownTimeoutDuration = DEFAULT_EVICTOR_SHUTDOWN_TIMEOUT;
	private Duration softMinEvictableIdleDuration = DEFAULT_SOFT_MIN_EVICTABLE_IDLE_DURATION;
	private int numTestsPerEvictionRun = DEFAULT_NUM_TESTS_PER_EVICTION_RUN;
	private EvictionPolicy<?> evictionPolicy;
	private String evictionPolicyClassName = DEFAULT_EVICTION_POLICY_CLASS_NAME;
	private boolean testOnCreate = DEFAULT_TEST_ON_CREATE;
	private boolean testOnBorrow = DEFAULT_TEST_ON_BORROW;
	private boolean testOnReturn = DEFAULT_TEST_ON_RETURN;
	private boolean testWhileIdle = DEFAULT_TEST_WHILE_IDLE;
	private Duration durationBetweenEvictionRuns = DEFAULT_TIME_BETWEEN_EVICTION_RUNS;
	private boolean blockWhenExhausted = DEFAULT_BLOCK_WHEN_EXHAUSTED;
	private boolean jmxEnabled = DEFAULT_JMX_ENABLE;
	private String jmxNamePrefix = DEFAULT_JMX_NAME_PREFIX;
	private String jmxNameBase = DEFAULT_JMX_NAME_BASE;

	private int maxTotal = DEFAULT_MAX_TOTAL;
	private int maxIdle = DEFAULT_MAX_IDLE;
	private int minIdle = DEFAULT_MIN_IDLE;

	public RedisConnectionPoolOptions() {
	}

	private RedisConnectionPoolOptions(Builder builder) {
		this.readFrom = builder.readFrom;
		this.lifo = builder.lifo;
		this.fairness = builder.fairness;
		this.maxWaitDuration = builder.maxWaitDuration;
		this.minEvictableIdleDuration = builder.minEvictableIdleDuration;
		this.evictorShutdownTimeoutDuration = builder.evictorShutdownTimeoutDuration;
		this.softMinEvictableIdleDuration = builder.softMinEvictableIdleDuration;
		this.numTestsPerEvictionRun = builder.numTestsPerEvictionRun;
		this.evictionPolicy = builder.evictionPolicy;
		this.evictionPolicyClassName = builder.evictionPolicyClassName;
		this.testOnCreate = builder.testOnCreate;
		this.testOnBorrow = builder.testOnBorrow;
		this.testOnReturn = builder.testOnReturn;
		this.testWhileIdle = builder.testWhileIdle;
		this.durationBetweenEvictionRuns = builder.durationBetweenEvictionRuns;
		this.blockWhenExhausted = builder.blockWhenExhausted;
		this.jmxEnabled = builder.jmxEnabled;
		this.jmxNamePrefix = builder.jmxNamePrefix;
		this.jmxNameBase = builder.jmxNameBase;
		this.maxTotal = builder.maxTotal;
		this.maxIdle = builder.maxIdle;
		this.minIdle = builder.minIdle;
	}

	public Optional<ReadFrom> getReadFrom() {
		return readFrom;
	}

	public void setReadFrom(ReadFrom readFrom) {
		setReadFrom(Optional.of(readFrom));
	}

	public void setReadFrom(Optional<ReadFrom> readFrom) {
		this.readFrom = readFrom;
	}

	public boolean isLifo() {
		return lifo;
	}

	public void setLifo(boolean lifo) {
		this.lifo = lifo;
	}

	public boolean isFairness() {
		return fairness;
	}

	public void setFairness(boolean fairness) {
		this.fairness = fairness;
	}

	public Duration getMaxWaitDuration() {
		return maxWaitDuration;
	}

	public void setMaxWaitDuration(Duration maxWaitDuration) {
		this.maxWaitDuration = maxWaitDuration;
	}

	public Duration getMinEvictableIdleDuration() {
		return minEvictableIdleDuration;
	}

	public void setMinEvictableIdleDuration(Duration minEvictableIdleDuration) {
		this.minEvictableIdleDuration = minEvictableIdleDuration;
	}

	public Duration getEvictorShutdownTimeoutDuration() {
		return evictorShutdownTimeoutDuration;
	}

	public void setEvictorShutdownTimeoutDuration(Duration evictorShutdownTimeoutDuration) {
		this.evictorShutdownTimeoutDuration = evictorShutdownTimeoutDuration;
	}

	public Duration getSoftMinEvictableIdleDuration() {
		return softMinEvictableIdleDuration;
	}

	public void setSoftMinEvictableIdleDuration(Duration softMinEvictableIdleDuration) {
		this.softMinEvictableIdleDuration = softMinEvictableIdleDuration;
	}

	public int getNumTestsPerEvictionRun() {
		return numTestsPerEvictionRun;
	}

	public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
		this.numTestsPerEvictionRun = numTestsPerEvictionRun;
	}

	public EvictionPolicy<?> getEvictionPolicy() {
		return evictionPolicy;
	}

	public void setEvictionPolicy(EvictionPolicy<?> evictionPolicy) {
		this.evictionPolicy = evictionPolicy;
	}

	public String getEvictionPolicyClassName() {
		return evictionPolicyClassName;
	}

	public void setEvictionPolicyClassName(String evictionPolicyClassName) {
		this.evictionPolicyClassName = evictionPolicyClassName;
	}

	public boolean isTestOnCreate() {
		return testOnCreate;
	}

	public void setTestOnCreate(boolean testOnCreate) {
		this.testOnCreate = testOnCreate;
	}

	public boolean isTestOnBorrow() {
		return testOnBorrow;
	}

	public void setTestOnBorrow(boolean testOnBorrow) {
		this.testOnBorrow = testOnBorrow;
	}

	public boolean isTestOnReturn() {
		return testOnReturn;
	}

	public void setTestOnReturn(boolean testOnReturn) {
		this.testOnReturn = testOnReturn;
	}

	public boolean isTestWhileIdle() {
		return testWhileIdle;
	}

	public void setTestWhileIdle(boolean testWhileIdle) {
		this.testWhileIdle = testWhileIdle;
	}

	public Duration getDurationBetweenEvictionRuns() {
		return durationBetweenEvictionRuns;
	}

	public void setDurationBetweenEvictionRuns(Duration durationBetweenEvictionRuns) {
		this.durationBetweenEvictionRuns = durationBetweenEvictionRuns;
	}

	public boolean isBlockWhenExhausted() {
		return blockWhenExhausted;
	}

	public void setBlockWhenExhausted(boolean blockWhenExhausted) {
		this.blockWhenExhausted = blockWhenExhausted;
	}

	public boolean isJmxEnabled() {
		return jmxEnabled;
	}

	public void setJmxEnabled(boolean jmxEnabled) {
		this.jmxEnabled = jmxEnabled;
	}

	public String getJmxNamePrefix() {
		return jmxNamePrefix;
	}

	public void setJmxNamePrefix(String jmxNamePrefix) {
		this.jmxNamePrefix = jmxNamePrefix;
	}

	public String getJmxNameBase() {
		return jmxNameBase;
	}

	public void setJmxNameBase(String jmxNameBase) {
		this.jmxNameBase = jmxNameBase;
	}

	public int getMaxTotal() {
		return maxTotal;
	}

	public void setMaxTotal(int maxTotal) {
		this.maxTotal = maxTotal;
	}

	public int getMaxIdle() {
		return maxIdle;
	}

	public void setMaxIdle(int maxIdle) {
		this.maxIdle = maxIdle;
	}

	public int getMinIdle() {
		return minIdle;
	}

	public void setMinIdle(int minIdle) {
		this.minIdle = minIdle;
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		private Optional<ReadFrom> readFrom = Optional.empty();
		private boolean lifo = DEFAULT_LIFO;
		private boolean fairness = DEFAULT_FAIRNESS;
		private Duration maxWaitDuration = DEFAULT_MAX_WAIT;
		private Duration minEvictableIdleDuration = DEFAULT_MIN_EVICTABLE_IDLE_DURATION;
		private Duration evictorShutdownTimeoutDuration = DEFAULT_EVICTOR_SHUTDOWN_TIMEOUT;
		private Duration softMinEvictableIdleDuration = DEFAULT_SOFT_MIN_EVICTABLE_IDLE_DURATION;
		private int numTestsPerEvictionRun = DEFAULT_NUM_TESTS_PER_EVICTION_RUN;
		private EvictionPolicy<?> evictionPolicy;
		private String evictionPolicyClassName = DEFAULT_EVICTION_POLICY_CLASS_NAME;
		private boolean testOnCreate = DEFAULT_TEST_ON_CREATE;
		private boolean testOnBorrow = DEFAULT_TEST_ON_BORROW;
		private boolean testOnReturn = DEFAULT_TEST_ON_RETURN;
		private boolean testWhileIdle = DEFAULT_TEST_WHILE_IDLE;
		private Duration durationBetweenEvictionRuns = DEFAULT_TIME_BETWEEN_EVICTION_RUNS;
		private boolean blockWhenExhausted = DEFAULT_BLOCK_WHEN_EXHAUSTED;
		private boolean jmxEnabled = DEFAULT_JMX_ENABLE;
		private String jmxNamePrefix = DEFAULT_JMX_NAME_PREFIX;
		private String jmxNameBase = DEFAULT_JMX_NAME_BASE;
		private int maxTotal = DEFAULT_MAX_TOTAL;
		private int maxIdle = DEFAULT_MAX_IDLE;
		private int minIdle = DEFAULT_MIN_IDLE;

		private Builder() {
		}

		public Builder readFrom(ReadFrom readFrom) {
			return readFrom(Optional.of(readFrom));
		}

		public Builder readFrom(Optional<ReadFrom> readFrom) {
			this.readFrom = readFrom;
			return this;
		}

		public Builder lifo(boolean lifo) {
			this.lifo = lifo;
			return this;
		}

		public Builder fairness(boolean fairness) {
			this.fairness = fairness;
			return this;
		}

		public Builder maxWaitDuration(Duration maxWaitDuration) {
			this.maxWaitDuration = maxWaitDuration;
			return this;
		}

		public Builder minEvictableIdleDuration(Duration minEvictableIdleDuration) {
			this.minEvictableIdleDuration = minEvictableIdleDuration;
			return this;
		}

		public Builder evictorShutdownTimeoutDuration(Duration evictorShutdownTimeoutDuration) {
			this.evictorShutdownTimeoutDuration = evictorShutdownTimeoutDuration;
			return this;
		}

		public Builder softMinEvictableIdleDuration(Duration softMinEvictableIdleDuration) {
			this.softMinEvictableIdleDuration = softMinEvictableIdleDuration;
			return this;
		}

		public Builder numTestsPerEvictionRun(int numTestsPerEvictionRun) {
			this.numTestsPerEvictionRun = numTestsPerEvictionRun;
			return this;
		}

		public Builder evictionPolicy(EvictionPolicy<?> evictionPolicy) {
			this.evictionPolicy = evictionPolicy;
			return this;
		}

		public Builder evictionPolicyClassName(String evictionPolicyClassName) {
			this.evictionPolicyClassName = evictionPolicyClassName;
			return this;
		}

		public Builder testOnCreate(boolean testOnCreate) {
			this.testOnCreate = testOnCreate;
			return this;
		}

		public Builder testOnBorrow(boolean testOnBorrow) {
			this.testOnBorrow = testOnBorrow;
			return this;
		}

		public Builder testOnReturn(boolean testOnReturn) {
			this.testOnReturn = testOnReturn;
			return this;
		}

		public Builder testWhileIdle(boolean testWhileIdle) {
			this.testWhileIdle = testWhileIdle;
			return this;
		}

		public Builder durationBetweenEvictionRuns(Duration durationBetweenEvictionRuns) {
			this.durationBetweenEvictionRuns = durationBetweenEvictionRuns;
			return this;
		}

		public Builder blockWhenExhausted(boolean blockWhenExhausted) {
			this.blockWhenExhausted = blockWhenExhausted;
			return this;
		}

		public Builder jmxEnabled(boolean jmxEnabled) {
			this.jmxEnabled = jmxEnabled;
			return this;
		}

		public Builder jmxNamePrefix(String jmxNamePrefix) {
			this.jmxNamePrefix = jmxNamePrefix;
			return this;
		}

		public Builder jmxNameBase(String jmxNameBase) {
			this.jmxNameBase = jmxNameBase;
			return this;
		}

		public Builder maxTotal(int maxTotal) {
			this.maxTotal = maxTotal;
			return this;
		}

		public Builder maxIdle(int maxIdle) {
			this.maxIdle = maxIdle;
			return this;
		}

		public Builder minIdle(int minIdle) {
			this.minIdle = minIdle;
			return this;
		}

		public RedisConnectionPoolOptions build() {
			return new RedisConnectionPoolOptions(this);
		}
	}

}
