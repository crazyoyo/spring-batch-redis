package com.redis.spring.batch.reader;

import com.redis.spring.batch.common.FaultToleranceOptions;
import com.redis.spring.batch.common.PoolOptions;

public class ReaderOptions {

	public static final int DEFAULT_THREADS = 1;
	public static final int DEFAULT_CHUNK_SIZE = 50;

	private int chunkSize = DEFAULT_CHUNK_SIZE;
	private int threads = DEFAULT_THREADS;
	private PoolOptions poolOptions = PoolOptions.builder().build();
	private QueueOptions queueOptions = QueueOptions.builder().build();
	private FaultToleranceOptions faultToleranceOptions = FaultToleranceOptions.builder().build();

	public ReaderOptions() {
	}

	private ReaderOptions(Builder builder) {
		this.chunkSize = builder.chunkSize;
		this.threads = builder.threads;
		this.poolOptions = builder.poolOptions;
		this.queueOptions = builder.queueOptions;
		this.faultToleranceOptions = builder.faultToleranceOptions;
	}

	public boolean isMultiThreaded() {
		return threads > 1;
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

	public PoolOptions getPoolOptions() {
		return poolOptions;
	}

	public void setPoolOptions(PoolOptions poolOptions) {
		this.poolOptions = poolOptions;
	}

	public QueueOptions getQueueOptions() {
		return queueOptions;
	}

	public void setQueueOptions(QueueOptions queueOptions) {
		this.queueOptions = queueOptions;
	}

	public FaultToleranceOptions getFaultToleranceOptions() {
		return faultToleranceOptions;
	}

	public void setFaultToleranceOptions(FaultToleranceOptions faultToleranceOptions) {
		this.faultToleranceOptions = faultToleranceOptions;
	}

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {

		private int chunkSize = DEFAULT_CHUNK_SIZE;
		private int threads = DEFAULT_THREADS;
		private PoolOptions poolOptions = PoolOptions.builder().build();
		private QueueOptions queueOptions = QueueOptions.builder().build();
		private FaultToleranceOptions faultToleranceOptions = FaultToleranceOptions.builder().build();

		private Builder() {
		}

		public Builder chunkSize(int chunkSize) {
			this.chunkSize = chunkSize;
			return this;
		}

		public Builder threads(int threads) {
			this.threads = threads;
			return this;
		}

		public Builder poolOptions(PoolOptions options) {
			this.poolOptions = options;
			return this;
		}

		public Builder queueOptions(QueueOptions options) {
			this.queueOptions = options;
			return this;
		}

		public Builder faultToleranceOptions(FaultToleranceOptions options) {
			this.faultToleranceOptions = options;
			return this;
		}

		public ReaderOptions build() {
			return new ReaderOptions(this);
		}

	}

}
