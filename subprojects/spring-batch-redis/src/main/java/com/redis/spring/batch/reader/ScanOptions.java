package com.redis.spring.batch.reader;

import java.util.Optional;

public class ScanOptions {

	public static final String DEFAULT_MATCH = "*";
	public static final long DEFAULT_COUNT = 1000;

	private String match = DEFAULT_MATCH;
	private long count = DEFAULT_COUNT;
	private Optional<String> type = Optional.empty();

	public ScanOptions() {
	}

	private ScanOptions(Builder builder) {
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
		return "ScanReaderOptions [match=" + match + ", count=" + count + ", type=" + type + "]";
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

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

		public ScanOptions build() {
			return new ScanOptions(this);
		}
	}

}
