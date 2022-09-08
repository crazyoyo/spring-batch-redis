package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.springframework.batch.core.step.skip.SkipPolicy;

public class ReaderOptions {

	public static final int DEFAULT_SKIP_LIMIT = 0;
	public static final int DEFAULT_THREADS = 1;
	public static final int DEFAULT_CHUNK_SIZE = 50;
	public static final int DEFAULT_QUEUE_CAPACITY = 10000;
	public static final Duration DEFAULT_QUEUE_POLL_TIMEOUT = Duration.ofMillis(100);

	private int chunkSize = DEFAULT_CHUNK_SIZE;
	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
	private Duration queuePollTimeout = DEFAULT_QUEUE_POLL_TIMEOUT;
	private int skipLimit = DEFAULT_SKIP_LIMIT;
	private Optional<SkipPolicy> skipPolicy = Optional.empty();
	private int threads = DEFAULT_THREADS;
	private List<Class<? extends Throwable>> skip = new ArrayList<>();
	private List<Class<? extends Throwable>> noSkip = new ArrayList<>();

	public ReaderOptions() {
	}

	protected ReaderOptions(Builder<?> builder) {
		this.chunkSize = builder.chunkSize;
		this.queueCapacity = builder.queueCapacity;
		this.queuePollTimeout = builder.queuePollTimeout;
		this.skip = builder.skip;
		this.noSkip = builder.noSkip;
		this.skipLimit = builder.skipLimit;
		this.skipPolicy = builder.skipPolicy;
		this.threads = builder.threads;
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

	public int getQueueCapacity() {
		return queueCapacity;
	}

	public void setQueueCapacity(int queueCapacity) {
		this.queueCapacity = queueCapacity;
	}

	public Duration getQueuePollTimeout() {
		return queuePollTimeout;
	}

	public void setQueuePollTimeout(Duration queuePollTimeout) {
		this.queuePollTimeout = queuePollTimeout;
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

	@Override
	public String toString() {
		return "ReaderOptions [chunkSize=" + chunkSize + ", queueCapacity=" + queueCapacity + ", queuePollTimeout="
				+ queuePollTimeout + ", skipLimit=" + skipLimit + ", skipPolicy=" + skipPolicy + ", threads=" + threads
				+ ", skip=" + skip + ", noSkip=" + noSkip + "]";
	}

	public static class Builder<B extends Builder<B>> {

		private int chunkSize = DEFAULT_CHUNK_SIZE;
		private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
		private Duration queuePollTimeout = DEFAULT_QUEUE_POLL_TIMEOUT;
		private int skipLimit = DEFAULT_SKIP_LIMIT;
		private List<Class<? extends Throwable>> skip = new ArrayList<>();
		private List<Class<? extends Throwable>> noSkip = new ArrayList<>();
		private Optional<SkipPolicy> skipPolicy = Optional.empty();
		private int threads = DEFAULT_THREADS;

		protected Builder() {
		}

		@SuppressWarnings("unchecked")
		public B chunkSize(int chunkSize) {
			this.chunkSize = chunkSize;
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B queueCapacity(int queueCapacity) {
			this.queueCapacity = queueCapacity;
			return (B) this;
		}

		public B queuePollTimeoutInSeconds(long queuePollTimeout) {
			return queuePollTimeout(Duration.ofSeconds(queuePollTimeout));
		}

		@SuppressWarnings("unchecked")
		public B queuePollTimeout(Duration queuePollTimeout) {
			this.queuePollTimeout = queuePollTimeout;
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B skip(Class<? extends Throwable>... types) {
			return skip(Arrays.asList(types));
		}

		@SuppressWarnings("unchecked")
		public B skip(List<Class<? extends Throwable>> types) {
			this.skip.addAll(types);
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B noSkip(List<Class<? extends Throwable>> types) {
			this.noSkip.addAll(types);
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B noSkip(Class<? extends Throwable>... types) {
			return noSkip(Arrays.asList(types));
		}

		@SuppressWarnings("unchecked")
		public B skipLimit(int skipLimit) {
			this.skipLimit = skipLimit;
			return (B) this;
		}

		public B skipPolicy(SkipPolicy skipPolicy) {
			return skipPolicy(Optional.of(skipPolicy));
		}

		@SuppressWarnings("unchecked")
		public B skipPolicy(Optional<SkipPolicy> skipPolicy) {
			this.skipPolicy = skipPolicy;
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B threads(int threads) {
			this.threads = threads;
			return (B) this;
		}

	}

}