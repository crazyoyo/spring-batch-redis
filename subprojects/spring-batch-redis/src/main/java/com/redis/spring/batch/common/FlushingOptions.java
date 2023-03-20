package com.redis.spring.batch.common;

import java.time.Duration;
import java.util.Optional;

public class FlushingOptions {

	public static final Duration DEFAULT_FLUSHING_INTERVAL = Duration.ofMillis(50);

	private Duration flushingInterval = DEFAULT_FLUSHING_INTERVAL;
	private Optional<Duration> idleTimeout = Optional.empty(); // no idle stream detection by default

	private FlushingOptions(Builder builder) {
		this.flushingInterval = builder.flushingInterval;
		this.idleTimeout = builder.idleTimeout;
	}

	public Duration getFlushingInterval() {
		return flushingInterval;
	}

	public void setFlushingInterval(Duration interval) {
		this.flushingInterval = interval;
	}

	public Optional<Duration> getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(Optional<Duration> timeout) {
		this.idleTimeout = timeout;
	}

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {

		private Duration flushingInterval = DEFAULT_FLUSHING_INTERVAL;
		private Optional<Duration> idleTimeout = Optional.empty();

		private Builder() {
		}

		public Builder flushingInterval(Duration interval) {
			Utils.assertPositive(interval, "Flushing interval");
			this.flushingInterval = interval;
			return this;
		}

		public Builder idleTimeout(Duration timeout) {
			return idleTimeout(Optional.of(timeout));
		}

		public Builder idleTimeout(Optional<Duration> timeout) {
			this.idleTimeout = timeout;
			return this;
		}

		public FlushingOptions build() {
			return new FlushingOptions(this);
		}

	}

}
