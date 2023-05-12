package com.redis.spring.batch.common;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.BackOffPolicy;

public class StepOptions {

	public static final int DEFAULT_RETRY_LIMIT = 0;
	public static final int DEFAULT_SKIP_LIMIT = 0;
	public static final int DEFAULT_THREADS = 1;
	public static final int DEFAULT_CHUNK_SIZE = 50;

	private int chunkSize = DEFAULT_CHUNK_SIZE;
	private int threads = DEFAULT_THREADS;
	private Optional<SkipPolicy> skipPolicy = Optional.empty();
	private Optional<BackOffPolicy> backOffPolicy = Optional.empty();
	private Optional<RetryPolicy> retryPolicy = Optional.empty();
	private int retryLimit = DEFAULT_RETRY_LIMIT;
	private int skipLimit = DEFAULT_SKIP_LIMIT;
	private List<Class<? extends Throwable>> skip = new ArrayList<>();
	private List<Class<? extends Throwable>> noSkip = new ArrayList<>();
	private Optional<Duration> flushingInterval = Optional.empty();
	private Optional<Duration> idleTimeout = Optional.empty(); // no idle stream detection by default
	private boolean faultTolerant;

	private StepOptions(Builder builder) {
		this.chunkSize = builder.chunkSize;
		this.threads = builder.threads;
		this.skip = builder.skip;
		this.noSkip = builder.noSkip;
		this.retryLimit = builder.retryLimit;
		this.retryPolicy = builder.retryPolicy;
		this.backOffPolicy = builder.backOffPolicy;
		this.skipLimit = builder.skipLimit;
		this.skipPolicy = builder.skipPolicy;
		this.flushingInterval = builder.flushingInterval;
		this.idleTimeout = builder.idleTimeout;
		this.faultTolerant = builder.faultTolerant;
	}

	public boolean isFaultTolerant() {
		return faultTolerant;
	}

	public void setFaultTolerant(boolean faultTolerant) {
		this.faultTolerant = faultTolerant;
	}

	public Optional<BackOffPolicy> getBackOffPolicy() {
		return backOffPolicy;
	}

	public void setBackOffPolicy(Optional<BackOffPolicy> backOffPolicy) {
		this.backOffPolicy = backOffPolicy;
	}

	public Optional<RetryPolicy> getRetryPolicy() {
		return retryPolicy;
	}

	public void setRetryPolicy(Optional<RetryPolicy> retryPolicy) {
		this.retryPolicy = retryPolicy;
	}

	public int getChunkSize() {
		return chunkSize;
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	public int getThreads() {
		return threads;
	}

	public void setThreads(int threads) {
		this.threads = threads;
	}

	public List<Class<? extends Throwable>> getSkip() {
		return skip;
	}

	public void setSkip(List<Class<? extends Throwable>> skip) {
		this.skip = skip;
	}

	public List<Class<? extends Throwable>> getNoSkip() {
		return noSkip;
	}

	public void setNoSkip(List<Class<? extends Throwable>> noSkip) {
		this.noSkip = noSkip;
	}

	public int getRetryLimit() {
		return retryLimit;
	}

	public void setRetryLimit(int retryLimit) {
		this.retryLimit = retryLimit;
	}

	public int getSkipLimit() {
		return skipLimit;
	}

	public void setSkipLimit(int skipLimit) {
		this.skipLimit = skipLimit;
	}

	public Optional<SkipPolicy> getSkipPolicy() {
		return skipPolicy;
	}

	public void setSkipPolicy(Optional<SkipPolicy> skipPolicy) {
		this.skipPolicy = skipPolicy;
	}

	public Optional<Duration> getFlushingInterval() {
		return flushingInterval;
	}

	public void setFlushingInterval(Optional<Duration> interval) {
		this.flushingInterval = interval;
	}

	public void setFlushingInterval(Duration interval) {
		setFlushingInterval(Optional.of(interval));
	}

	public Optional<Duration> getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(Duration timeout) {
		setIdleTimeout(Optional.of(timeout));
	}

	public void setIdleTimeout(Optional<Duration> timeout) {
		this.idleTimeout = timeout;
	}

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {

		private boolean faultTolerant;
		private int chunkSize = DEFAULT_CHUNK_SIZE;
		private int threads = DEFAULT_THREADS;
		private int retryLimit = DEFAULT_RETRY_LIMIT;
		private int skipLimit = DEFAULT_SKIP_LIMIT;
		private List<Class<? extends Throwable>> skip = new ArrayList<>();
		private List<Class<? extends Throwable>> noSkip = new ArrayList<>();
		private Optional<RetryPolicy> retryPolicy = Optional.empty();
		private Optional<BackOffPolicy> backOffPolicy = Optional.empty();
		private Optional<SkipPolicy> skipPolicy = Optional.empty();
		private Optional<Duration> flushingInterval = Optional.empty();
		private Optional<Duration> idleTimeout = Optional.empty(); // no idle stream detection by default

		private Builder() {
		}

		public StepOptions build() {
			return new StepOptions(this);
		}

		public Builder faultTolerant(boolean faultTolerant) {
			this.faultTolerant = faultTolerant;
			return this;
		}

		public Builder chunkSize(int size) {
			this.chunkSize = size;
			return this;
		}

		public Builder threads(int threads) {
			this.threads = threads;
			return this;
		}

		public Builder backOffPolicy(BackOffPolicy policy) {
			return backOffPolicy(Optional.of(policy));
		}

		public Builder backOffPolicy(Optional<BackOffPolicy> policy) {
			this.backOffPolicy = policy;
			return this;
		}

		public Builder retryPolicy(RetryPolicy policy) {
			return retryPolicy(Optional.of(policy));
		}

		public Builder retryPolicy(Optional<RetryPolicy> policy) {
			this.retryPolicy = policy;
			return this;
		}

		@SuppressWarnings("unchecked")
		public Builder skip(Class<? extends Throwable>... types) {
			return skip(Arrays.asList(types));
		}

		public Builder skip(List<Class<? extends Throwable>> types) {
			this.skip.addAll(types);
			return this;
		}

		public Builder noSkip(List<Class<? extends Throwable>> types) {
			this.noSkip.addAll(types);
			return this;
		}

		@SuppressWarnings("unchecked")
		public Builder noSkip(Class<? extends Throwable>... types) {
			return noSkip(Arrays.asList(types));
		}

		public Builder retryLimit(int limit) {
			this.retryLimit = limit;
			return this;
		}

		public Builder skipLimit(int skipLimit) {
			this.skipLimit = skipLimit;
			return this;
		}

		public Builder skipPolicy(SkipPolicy skipPolicy) {
			return skipPolicy(Optional.of(skipPolicy));
		}

		public Builder skipPolicy(Optional<SkipPolicy> skipPolicy) {
			this.skipPolicy = skipPolicy;
			return this;
		}

		public Builder flushingInterval(Duration interval) {
			return flushingInterval(Optional.of(interval));
		}

		public Builder flushingInterval(Optional<Duration> interval) {
			this.flushingInterval = interval;
			return this;
		}

		public Builder idleTimeout(Duration timeout) {
			return idleTimeout(Optional.of(timeout));
		}

		public Builder idleTimeout(Optional<Duration> timeout) {
			this.idleTimeout = timeout;
			return this;
		}
	}

}