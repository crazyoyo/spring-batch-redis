package com.redis.spring.batch.common;

import java.time.Duration;
import java.util.Optional;

public class FlushingStepOptions extends StepOptions {

	public static final Duration DEFAULT_FLUSHING_INTERVAL = Duration.ofMillis(50);

	private Duration interval = DEFAULT_FLUSHING_INTERVAL;
	private Optional<Duration> idleTimeout = Optional.empty(); // no idle stream detection by default

	public FlushingStepOptions() {
	}

	private FlushingStepOptions(Builder builder) {
		super(builder);
		this.interval = builder.interval;
		this.idleTimeout = builder.idleTimeout;
	}

	public Duration getInterval() {
		return interval;
	}

	public void setInterval(Duration interval) {
		this.interval = interval;
	}

	public Optional<Duration> getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(Optional<Duration> timeout) {
		this.idleTimeout = timeout;
	}

	public static final class Builder extends BaseBuilder<Builder> {

		private Duration interval = DEFAULT_FLUSHING_INTERVAL;
		private Optional<Duration> idleTimeout = Optional.empty();

		public Builder(BaseBuilder<?> builder) {
			super(builder);
		}

		public Builder interval(Duration interval) {
			Utils.assertPositive(interval, "Interval");
			this.interval = interval;
			return this;
		}

		public Builder idleTimeout(Duration timeout) {
			return idleTimeout(Optional.of(timeout));
		}

		public Builder idleTimeout(Optional<Duration> timeout) {
			this.idleTimeout = timeout;
			return this;
		}

		public FlushingStepOptions build() {
			return new FlushingStepOptions(this);
		}

	}

}
