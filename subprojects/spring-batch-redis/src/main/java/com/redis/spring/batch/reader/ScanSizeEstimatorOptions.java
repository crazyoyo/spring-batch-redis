package com.redis.spring.batch.reader;

import java.util.Optional;

public class ScanSizeEstimatorOptions {

	public static final long DEFAULT_SAMPLE_SIZE = 100;
	public static final String DEFAULT_MATCH = ScanOptions.DEFAULT_MATCH;

	private String match = DEFAULT_MATCH;
	private long sampleSize = DEFAULT_SAMPLE_SIZE;
	private Optional<String> type = Optional.empty();

	public ScanSizeEstimatorOptions() {

	}

	public ScanSizeEstimatorOptions(Builder builder) {
		this.match = builder.match;
		this.sampleSize = builder.sampleSize;
		this.type = builder.type;
	}

	public String getMatch() {
		return match;
	}

	public void setMatch(String match) {
		this.match = match;
	}

	public long getSampleSize() {
		return sampleSize;
	}

	public void setSampleSize(long sampleSize) {
		this.sampleSize = sampleSize;
	}

	public Optional<String> getType() {
		return type;
	}

	public void setType(Optional<String> type) {
		this.type = type;
	}

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {

		private String match = ScanOptions.DEFAULT_MATCH;
		private long sampleSize = DEFAULT_SAMPLE_SIZE;
		private Optional<String> type = Optional.empty();

		public Builder match(String match) {
			this.match = match;
			return this;
		}

		public Builder sampleSize(long sampleSize) {
			this.sampleSize = sampleSize;
			return this;
		}

		public Builder type(String type) {
			return type(Optional.of(type));
		}

		public Builder type(Optional<String> type) {
			this.type = type;
			return this;
		}

		public ScanSizeEstimatorOptions build() {
			return new ScanSizeEstimatorOptions(this);
		}

	}

}