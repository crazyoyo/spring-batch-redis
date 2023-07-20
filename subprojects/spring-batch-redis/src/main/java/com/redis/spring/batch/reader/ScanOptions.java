package com.redis.spring.batch.reader;

import java.util.Optional;

import org.springframework.util.Assert;

import io.lettuce.core.ReadFrom;

public class ScanOptions {

    public static final String MATCH_ALL = "*";

    public static final String DEFAULT_MATCH = MATCH_ALL;

    public static final long DEFAULT_COUNT = 1000;

    private String match = DEFAULT_MATCH;

    private long count = DEFAULT_COUNT;

    private Optional<String> type = Optional.empty();

    private Optional<ReadFrom> readFrom = Optional.empty();

    public ScanOptions() {

    }

    private ScanOptions(Builder builder) {
        this.match = builder.match;
        this.count = builder.count;
        this.type = builder.type;
        this.readFrom = builder.readFrom;
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

    public Optional<ReadFrom> getReadFrom() {
        return readFrom;
    }

    public void setReadFrom(ReadFrom readFrom) {
        setReadFrom(Optional.of(readFrom));
    }

    public void setReadFrom(Optional<ReadFrom> readFrom) {
        this.readFrom = readFrom;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private String match = DEFAULT_MATCH;

        private long count = DEFAULT_COUNT;

        private Optional<String> type = Optional.empty();

        private Optional<ReadFrom> readFrom = Optional.empty();

        private Builder() {
        }

        public Builder match(String match) {
            Assert.notNull(match, "Match must be null");
            Assert.isTrue(!match.trim().isEmpty(), "Match must not be empty");
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

        public Builder readFrom(ReadFrom readFrom) {
            return readFrom(Optional.of(readFrom));
        }

        public Builder readFrom(Optional<ReadFrom> readFrom) {
            this.readFrom = readFrom;
            return this;
        }

        public ScanOptions build() {
            return new ScanOptions(this);
        }

    }

}
