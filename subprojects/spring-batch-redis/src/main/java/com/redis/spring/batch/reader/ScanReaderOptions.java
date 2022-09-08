package com.redis.spring.batch.reader;

import java.util.Optional;

public class ScanReaderOptions extends ReaderOptions {

	public static final String DEFAULT_MATCH = "*";
	public static final long DEFAULT_COUNT = 1000;

	private String match = DEFAULT_MATCH;
	private long count = DEFAULT_COUNT;
	private Optional<String> type = Optional.empty();

	public ScanReaderOptions() {
	}

	private ScanReaderOptions(Builder builder) {
		super(builder);
		this.match = builder.match;
		this.count = builder.count;
		this.type = builder.type;
	}

	public String getMatch() {
		return match;
	}

	public void setMatch(String match) {
		this.match = match;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public Optional<String> getType() {
		return type;
	}

	public void setType(String type) {
		setType(Optional.of(type));
	}

	public void setType(Optional<String> type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return "ScanReaderOptions [match=" + match + ", count=" + count + ", type=" + type + ", getSkip()=" + getSkip()
				+ ", getNoSkip()=" + getNoSkip() + ", getChunkSize()=" + getChunkSize() + ", getQueueCapacity()="
				+ getQueueCapacity() + ", getQueuePollTimeout()=" + getQueuePollTimeout() + ", getSkipLimit()="
				+ getSkipLimit() + ", getSkipPolicy()=" + getSkipPolicy() + ", getThreads()=" + getThreads()
				+ ", toString()=" + super.toString() + ", getClass()=" + getClass() + ", hashCode()=" + hashCode()
				+ "]";
	}

	public static Builder builder() {
		return new Builder();
	}

	public static Builder builder(ReaderOptions options) {
		return builder().chunkSize(options.getChunkSize()).noSkip(options.getNoSkip())
				.queueCapacity(options.getQueueCapacity()).threads(options.getThreads())
				.skipPolicy(options.getSkipPolicy()).skipLimit(options.getSkipLimit()).skip(options.getSkip())
				.queuePollTimeout(options.getQueuePollTimeout());
	}

	public static final class Builder extends ReaderOptions.Builder<Builder> {

		private String match = DEFAULT_MATCH;
		private long count = DEFAULT_COUNT;
		private Optional<String> type = Optional.empty();

		protected Builder() {

		}

		public Builder match(String match) {
			this.match = match;
			return this;
		}

		public Builder count(long count) {
			this.count = count;
			return this;
		}

		public Builder type(String type) {
			return type(Optional.of(type));
		}

		public Builder type(Optional<String> type) {
			this.type = type;
			return this;
		}

		public ScanReaderOptions build() {
			return new ScanReaderOptions(this);
		}
	}

}
