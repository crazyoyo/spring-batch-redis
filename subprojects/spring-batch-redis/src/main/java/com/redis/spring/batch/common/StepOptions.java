package com.redis.spring.batch.common;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.springframework.batch.core.step.skip.SkipPolicy;

public class StepOptions {

	public static final int DEFAULT_SKIP_LIMIT = 0;
	public static final int DEFAULT_THREADS = 1;
	public static final int DEFAULT_CHUNK_SIZE = 50;

	private int chunkSize = DEFAULT_CHUNK_SIZE;
	private int threads = DEFAULT_THREADS;
	private Optional<SkipPolicy> skipPolicy = Optional.empty();
	private int skipLimit = DEFAULT_SKIP_LIMIT;
	private List<Class<? extends Throwable>> skip = new ArrayList<>();
	private List<Class<? extends Throwable>> noSkip = new ArrayList<>();
	private Optional<Duration> flushingInterval = Optional.empty(); // no flushing by default
	private Optional<Duration> idleTimeout = Optional.empty(); // no idle stream detection by default
	private boolean faultTolerant;

	private StepOptions(Builder builder) {
		this.chunkSize = builder.chunkSize;
		this.skip = builder.skip;
		this.noSkip = builder.noSkip;
		this.skipLimit = builder.skipLimit;
		this.skipPolicy = builder.skipPolicy;
		this.threads = builder.threads;
		this.flushingInterval = builder.flushingInterval;
		this.idleTimeout = builder.idleTimeout;
		this.faultTolerant = builder.faultTolerant;
	}

	public boolean isFaultTolerant() {
		return faultTolerant;
	}

	public Optional<Duration> getFlushingInterval() {
		return flushingInterval;
	}

	public void setFlushingInterval(Duration interval) {
		this.flushingInterval = Optional.of(interval);
	}

	public Optional<Duration> getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(Optional<Duration> timeout) {
		this.idleTimeout = timeout;
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

	public int getChunkSize() {
		return chunkSize;
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
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

	public int getThreads() {
		return threads;
	}

	public void setThreads(int threads) {
		this.threads = threads;
	}

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {

		private int chunkSize = DEFAULT_CHUNK_SIZE;
		private int skipLimit = DEFAULT_SKIP_LIMIT;
		private List<Class<? extends Throwable>> skip = new ArrayList<>();
		private List<Class<? extends Throwable>> noSkip = new ArrayList<>();
		private Optional<SkipPolicy> skipPolicy = Optional.empty();
		private int threads = DEFAULT_THREADS;
		private Optional<Duration> flushingInterval = Optional.empty();
		private Optional<Duration> idleTimeout = Optional.empty();
		private boolean faultTolerant;

		public Builder flushingInterval(Duration interval) {
			Utils.assertPositive(interval, "Flushing interval");
			this.flushingInterval = Optional.of(interval);
			return this;
		}

		public Builder faultTolerant(boolean ft) {
			this.faultTolerant = ft;
			return this;
		}

		public Builder idleTimeout(Duration timeout) {
			return idleTimeout(Optional.of(timeout));
		}

		public Builder idleTimeout(Optional<Duration> timeout) {
			this.idleTimeout = timeout;
			return this;
		}

		public StepOptions build() {
			return new StepOptions(this);
		}

		private Builder() {
		}

		public Builder chunkSize(int chunkSize) {
			this.chunkSize = chunkSize;
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

		public Builder threads(int threads) {
			this.threads = threads;
			return this;
		}

	}

}