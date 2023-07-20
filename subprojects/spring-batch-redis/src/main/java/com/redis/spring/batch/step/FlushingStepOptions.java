package com.redis.spring.batch.step;

import java.time.Duration;
import java.util.Optional;

public class FlushingStepOptions {

    public static final Duration DEFAULT_FLUSHING_INTERVAL = Duration.ofMillis(50);

    public static final long NO_IDLE_TIMEOUT = -1;

    private Duration interval = DEFAULT_FLUSHING_INTERVAL;

    private Optional<Duration> idleTimeout = Optional.empty(); // no idle stream detection by default

    private FlushingStepOptions(Builder builder) {
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

    public void setIdleTimeout(Duration timeout) {
        setIdleTimeout(Optional.of(timeout));
    }

    public void setIdleTimeout(Optional<Duration> timeout) {
        this.idleTimeout = timeout;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private Duration interval = DEFAULT_FLUSHING_INTERVAL;

        private Optional<Duration> idleTimeout = Optional.empty();

        private Builder() {
        }

        public Builder interval(Duration interval) {
            this.interval = interval;
            return this;
        }

        public Builder idleTimeout(Duration idleTimeout) {
            return idleTimeout(Optional.of(idleTimeout));
        }

        public Builder idleTimeout(Optional<Duration> idleTimeout) {
            this.idleTimeout = idleTimeout;
            return this;
        }

        public FlushingStepOptions build() {
            return new FlushingStepOptions(this);
        }

    }

}
