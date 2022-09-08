package com.redis.spring.batch.reader;

import com.redis.spring.batch.step.FlushingOptions;

public class LiveReaderOptions extends ReaderOptions {

	private QueueOptions notificationQueueOptions = QueueOptions.builder().build();
	private FlushingOptions flushingOptions = FlushingOptions.builder().build();

	public LiveReaderOptions() {
	}

	private LiveReaderOptions(Builder builder) {
		super(builder);
		this.notificationQueueOptions = builder.notificationQueueOptions;
		this.flushingOptions = builder.flushingOptions;
	}

	public QueueOptions getNotificationQueueOptions() {
		return notificationQueueOptions;
	}

	public void setNotificationQueueOptions(QueueOptions options) {
		this.notificationQueueOptions = options;
	}

	public FlushingOptions getFlushingOptions() {
		return flushingOptions;
	}

	public void setFlushingOptions(FlushingOptions flushingOptions) {
		this.flushingOptions = flushingOptions;
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public String toString() {
		return "LiveReaderOptions [notificationQueueOptions=" + notificationQueueOptions + ", flushingOptions="
				+ flushingOptions + ", getSkip()=" + getSkip() + ", getNoSkip()=" + getNoSkip() + ", getChunkSize()="
				+ getChunkSize() + ", getQueueOptions()=" + getQueueOptions() + ", getSkipLimit()=" + getSkipLimit()
				+ ", getSkipPolicy()=" + getSkipPolicy() + ", getThreads()=" + getThreads() + "]";
	}

	public static Builder builder(ReaderOptions options) {
		return builder().chunkSize(options.getChunkSize()).noSkip(options.getNoSkip())
				.queueOptions(options.getQueueOptions()).threads(options.getThreads())
				.skipPolicy(options.getSkipPolicy()).skipLimit(options.getSkipLimit()).skip(options.getSkip());
	}

	public static final class Builder extends ReaderOptions.Builder<Builder> {

		private QueueOptions notificationQueueOptions = QueueOptions.builder().build();
		private FlushingOptions flushingOptions = FlushingOptions.builder().build();

		private Builder() {
		}

		public Builder notificationQueueOptions(QueueOptions options) {
			this.notificationQueueOptions = options;
			return this;
		}

		public Builder flushingOptions(FlushingOptions flushingOptions) {
			this.flushingOptions = flushingOptions;
			return this;
		}

		public LiveReaderOptions build() {
			return new LiveReaderOptions(this);
		}
	}
}