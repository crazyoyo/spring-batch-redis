package com.redis.spring.batch.reader;

import java.time.Duration;

import com.redis.spring.batch.common.PoolOptions;

public class KeyComparatorOptions {

	public static final Duration DEFAULT_TTL_TOLERANCE = Duration.ofMillis(100);

	private PoolOptions leftPoolOptions = PoolOptions.builder().build();
	private PoolOptions rightPoolOptions = PoolOptions.builder().build();
	private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;
	private ScanOptions scanOptions = ScanOptions.builder().build();

	private KeyComparatorOptions(Builder builder) {
		this.leftPoolOptions = builder.leftPoolOptions;
		this.rightPoolOptions = builder.rightPoolOptions;
		this.ttlTolerance = builder.ttlTolerance;
		this.scanOptions = builder.scanOptions;
	}

	public PoolOptions getLeftPoolOptions() {
		return leftPoolOptions;
	}

	public void setLeftPoolOptions(PoolOptions leftPoolOptions) {
		this.leftPoolOptions = leftPoolOptions;
	}

	public PoolOptions getRightPoolOptions() {
		return rightPoolOptions;
	}

	public void setRightPoolOptions(PoolOptions rightPoolOptions) {
		this.rightPoolOptions = rightPoolOptions;
	}

	public Duration getTtlTolerance() {
		return ttlTolerance;
	}

	public void setTtlTolerance(Duration ttlTolerance) {
		this.ttlTolerance = ttlTolerance;
	}

	public ScanOptions getScanOptions() {
		return scanOptions;
	}

	public void setScanOptions(ScanOptions scanOptions) {
		this.scanOptions = scanOptions;
	}

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {

		private PoolOptions leftPoolOptions = PoolOptions.builder().build();
		private PoolOptions rightPoolOptions = PoolOptions.builder().build();
		private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;
		private ScanOptions scanOptions = ScanOptions.builder().build();

		public Builder ttlTolerance(Duration ttlTolerance) {
			this.ttlTolerance = ttlTolerance;
			return this;
		}

		public Builder leftPoolOptions(PoolOptions options) {
			this.leftPoolOptions = options;
			return this;
		}

		public Builder rightPoolOptions(PoolOptions options) {
			this.rightPoolOptions = options;
			return this;
		}

		public Builder scanOptions(ScanOptions options) {
			this.scanOptions = options;
			return this;
		}

		public KeyComparatorOptions build() {
			return new KeyComparatorOptions(this);
		}

	}
}
