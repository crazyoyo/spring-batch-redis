package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.springframework.batch.core.step.skip.SkipPolicy;

public class ReaderOptions {

	public static final int DEFAULT_SKIP_LIMIT = 0;
	public static final int DEFAULT_THREADS = 1;
	public static final int DEFAULT_CHUNK_SIZE = 50;

	private int chunkSize = DEFAULT_CHUNK_SIZE;
	private QueueOptions queueOptions = QueueOptions.builder().build();
	private int skipLimit = DEFAULT_SKIP_LIMIT;
	private Optional<SkipPolicy> skipPolicy = Optional.empty();
	private int threads = DEFAULT_THREADS;
	private List<Class<? extends Throwable>> skip = new ArrayList<>();
	private List<Class<? extends Throwable>> noSkip = new ArrayList<>();

	public ReaderOptions() {
	}

	protected ReaderOptions(Builder<?> builder) {
		this.chunkSize = builder.chunkSize;
		this.queueOptions = builder.queueOptions;
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

	public QueueOptions getQueueOptions() {
		return queueOptions;
	}

	public void setQueueOptions(QueueOptions queueOptions) {
		this.queueOptions = queueOptions;
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

	@Override
	public String toString() {
		return "ReaderOptions [chunkSize=" + chunkSize + ", queueOptions=" + queueOptions + ", skipLimit=" + skipLimit
				+ ", skipPolicy=" + skipPolicy + ", threads=" + threads + ", skip=" + skip + ", noSkip=" + noSkip + "]";
	}

	public static class Builder<B extends Builder<B>> {

		private int chunkSize = DEFAULT_CHUNK_SIZE;
		private QueueOptions queueOptions = QueueOptions.builder().build();
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
		public B queueOptions(QueueOptions queueOptions) {
			this.queueOptions = queueOptions;
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