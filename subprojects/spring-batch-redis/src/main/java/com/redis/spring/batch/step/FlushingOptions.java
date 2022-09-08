package com.redis.spring.batch.step;

import java.time.Duration;
import java.util.Optional;

import com.redis.spring.batch.common.Utils;

public class FlushingOptions {

	public static final Duration DEFAULT_FLUSHING_INTERVAL = Duration.ofMillis(50);

	private Duration interval = DEFAULT_FLUSHING_INTERVAL;
	private Optional<Duration> timeout = Optional.empty(); // no idle stream detection by default

	private FlushingOptions(Builder builder) {
		this.interval = builder.interval;
		this.timeout = builder.timeout;
	}

	public Duration getInterval() {
		return interval;
	}

	public void setInterval(Duration interval) {
		this.interval = interval;
	}

	public Optional<Duration> getTimeout() {
		return timeout;
	}

	public void setTimeout(Optional<Duration> timeout) {
		this.timeout = timeout;
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		private Duration interval = DEFAULT_FLUSHING_INTERVAL;
		private Optional<Duration> timeout = Optional.empty();

		private Builder() {
		}

		public Builder interval(Duration interval) {
			Utils.assertPositive(interval, "Interval");
			this.interval = interval;
			return this;
		}

		public Builder timeout(Duration timeout) {
			return timeout(Optional.of(timeout));
		}

		public Builder timeout(Optional<Duration> timeout) {
			this.timeout = timeout;
			return this;
		}

		public FlushingOptions build() {
			return new FlushingOptions(this);
		}
	}

}
