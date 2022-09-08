package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.Optional;

import com.redis.spring.batch.step.FlushingSimpleStepBuilder;

public class LiveReaderOptions extends ReaderOptions {

	public static final int DEFAULT_NOTIFICATION_QUEUE_CAPACITY = 10000;
	public static final Duration DEFAULT_FLUSHING_INTERVAL = FlushingSimpleStepBuilder.DEFAULT_FLUSHING_INTERVAL;

	private int notificationQueueCapacity = DEFAULT_NOTIFICATION_QUEUE_CAPACITY;
	private Duration flushingInterval = DEFAULT_FLUSHING_INTERVAL;
	private Optional<Duration> idleTimeout = Optional.empty();

	public LiveReaderOptions() {
	}

	private LiveReaderOptions(Builder builder) {
		super(builder);
		this.notificationQueueCapacity = builder.notificationQueueCapacity;
		this.flushingInterval = builder.flushingInterval;
		this.idleTimeout = builder.idleTimeout;
	}

	public int getNotificationQueueCapacity() {
		return notificationQueueCapacity;
	}

	public void setNotificationQueueCapacity(int capacity) {
		this.notificationQueueCapacity = capacity;
	}

	public Duration getFlushingInterval() {
		return flushingInterval;
	}

	public void setFlushingInterval(Duration flushingInterval) {
		this.flushingInterval = flushingInterval;
	}

	public Optional<Duration> getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(Duration idleTimeout) {
		setIdleTimeout(Optional.of(idleTimeout));
	}

	public void setIdleTimeout(Optional<Duration> idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public String toString() {
		return "LiveReaderOptions [notificationQueueCapacity=" + notificationQueueCapacity + ", flushingInterval="
				+ flushingInterval + ", idleTimeout=" + idleTimeout + ", getSkip()=" + getSkip() + ", getNoSkip()="
				+ getNoSkip() + ", getChunkSize()=" + getChunkSize() + ", getQueueCapacity()=" + getQueueCapacity()
				+ ", getQueuePollTimeout()=" + getQueuePollTimeout() + ", getSkipLimit()=" + getSkipLimit()
				+ ", getSkipPolicy()=" + getSkipPolicy() + ", getThreads()=" + getThreads() + "]";
	}

	public static Builder builder(ReaderOptions options) {
		return builder().chunkSize(options.getChunkSize()).noSkip(options.getNoSkip())
				.queueCapacity(options.getQueueCapacity()).threads(options.getThreads())
				.skipPolicy(options.getSkipPolicy()).skipLimit(options.getSkipLimit()).skip(options.getSkip())
				.queuePollTimeout(options.getQueuePollTimeout());
	}

	public static final class Builder extends ReaderOptions.Builder<Builder> {

		private int notificationQueueCapacity = DEFAULT_NOTIFICATION_QUEUE_CAPACITY;
		private Duration flushingInterval = DEFAULT_FLUSHING_INTERVAL;
		private Optional<Duration> idleTimeout = Optional.empty();

		private Builder() {
		}

		public Builder notificationQueueCapacity(int capacity) {
			this.notificationQueueCapacity = capacity;
			return this;
		}

		public Builder flushingIntervalInSeconds(long flushingInterval) {
			return flushingInterval(Duration.ofSeconds(flushingInterval));
		}

		public Builder flushingInterval(Duration flushingInterval) {
			this.flushingInterval = flushingInterval;
			return this;
		}

		public Builder idleTimeoutInSeconds(long idleTimeout) {
			return idleTimeout(Duration.ofSeconds(idleTimeout));
		}

		public Builder idleTimeout(Duration idleTimeout) {
			return idleTimeout(Optional.of(idleTimeout));
		}

		public Builder idleTimeout(Optional<Duration> idleTimeout) {
			this.idleTimeout = idleTimeout;
			return this;
		}

		public LiveReaderOptions build() {
			return new LiveReaderOptions(this);
		}
	}
}