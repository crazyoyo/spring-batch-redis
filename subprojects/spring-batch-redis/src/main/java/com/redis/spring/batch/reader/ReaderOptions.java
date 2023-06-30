package com.redis.spring.batch.reader;

import java.util.Optional;

import com.redis.spring.batch.common.PoolOptions;

import io.lettuce.core.ReadFrom;

public class ReaderOptions {

	public static final int DEFAULT_THREADS = 1;
	public static final int DEFAULT_CHUNK_SIZE = 50;

	private int threads = DEFAULT_THREADS;
	private int chunkSize = DEFAULT_CHUNK_SIZE;
	private PoolOptions poolOptions = PoolOptions.builder().build();
	private Optional<ReadFrom> readFrom = Optional.empty();
	private QueueOptions queueOptions = QueueOptions.builder().build();

	private ReaderOptions(Builder builder) {
		this.threads = builder.threads;
		this.chunkSize = builder.chunkSize;
		this.poolOptions = builder.poolOptions;
		this.readFrom = builder.readFrom;
		this.queueOptions = builder.queueOptions;
	}

	public int getThreads() {
		return threads;
	}

	public void setThreads(int threads) {
		this.threads = threads;
	}

	public int getChunkSize() {
		return chunkSize;
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	public PoolOptions getPoolOptions() {
		return poolOptions;
	}

	public void setPoolOptions(PoolOptions poolOptions) {
		this.poolOptions = poolOptions;
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

	public QueueOptions getQueueOptions() {
		return queueOptions;
	}

	public void setQueueOptions(QueueOptions queueOptions) {
		this.queueOptions = queueOptions;
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		private int threads = DEFAULT_THREADS;
		private int chunkSize = DEFAULT_CHUNK_SIZE;
		private PoolOptions poolOptions = PoolOptions.builder().build();
		private Optional<ReadFrom> readFrom = Optional.empty();
		private QueueOptions queueOptions = QueueOptions.builder().build();

		private Builder() {
		}

		public Builder threads(int threads) {
			this.threads = threads;
			return this;
		}

		public Builder chunkSize(int chunkSize) {
			this.chunkSize = chunkSize;
			return this;
		}

		public Builder poolOptions(PoolOptions poolOptions) {
			this.poolOptions = poolOptions;
			return this;
		}

		public Builder readFrom(ReadFrom readFrom) {
			return readFrom(Optional.of(readFrom));
		}

		public Builder readFrom(Optional<ReadFrom> readFrom) {
			this.readFrom = readFrom;
			return this;
		}

		public Builder queueOptions(QueueOptions queueOptions) {
			this.queueOptions = queueOptions;
			return this;
		}

		public ReaderOptions build() {
			return new ReaderOptions(this);
		}
	}

}
