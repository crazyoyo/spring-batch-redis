package com.redis.spring.batch.reader;

import org.springframework.util.unit.DataSize;

public class MemoryUsageOptions {

	private static final DataSize DEFAULT_LIMIT = DataSize.ofMegabytes(100);
	private static final int DEFAULT_SAMPLES = 5;

	private boolean enabled;
	private DataSize limit = DEFAULT_LIMIT;
	private int samples = DEFAULT_SAMPLES;

	private MemoryUsageOptions(Builder builder) {
		this.enabled = builder.enabled;
		this.limit = builder.limit;
		this.samples = builder.samples;
	}

	public boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	public DataSize getLimit() {
		return limit;
	}

	public void setLimit(DataSize limit) {
		this.limit = limit;
	}

	public int getSamples() {
		return samples;
	}

	public void setSamples(int samples) {
		this.samples = samples;
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		private boolean enabled;
		private DataSize limit = DEFAULT_LIMIT;
		private int samples = DEFAULT_SAMPLES;

		private Builder() {
		}

		public Builder enabled(boolean enabled) {
			this.enabled = enabled;
			return this;
		}

		public Builder limit(DataSize limit) {
			this.limit = limit;
			return this;
		}

		public Builder samples(int samples) {
			this.samples = samples;
			return this;
		}

		public MemoryUsageOptions build() {
			return new MemoryUsageOptions(this);
		}
	}

}
