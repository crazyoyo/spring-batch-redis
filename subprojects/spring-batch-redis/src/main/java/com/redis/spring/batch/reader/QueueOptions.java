package com.redis.spring.batch.reader;

import java.time.Duration;

public class QueueOptions {

    public static final int DEFAULT_CAPACITY = 10000;

    public static final long DEFAULT_POLL_TIMEOUT_MILLIS = 100;

    public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(DEFAULT_POLL_TIMEOUT_MILLIS);

    private int capacity = DEFAULT_CAPACITY;

    private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;

    private QueueOptions(Builder builder) {
        this.capacity = builder.capacity;
        this.pollTimeout = builder.pollTimeout;
    }

    public int getCapacity() {
        return capacity;
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    public Duration getPollTimeout() {
        return pollTimeout;
    }

    public void setPollTimeout(Duration pollTimeout) {
        this.pollTimeout = pollTimeout;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private int capacity = DEFAULT_CAPACITY;

        private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;

        private Builder() {
        }

        public Builder capacity(int capacity) {
            this.capacity = capacity;
            return this;
        }

        public Builder pollTimeout(Duration timeout) {
            this.pollTimeout = timeout;
            return this;
        }

        public Builder pollTimeoutInSeconds(long timeout) {
            return pollTimeout(Duration.ofSeconds(timeout));
        }

        public QueueOptions build() {
            return new QueueOptions(this);
        }

    }

}
