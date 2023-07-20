package com.redis.spring.batch.reader;

import org.springframework.util.unit.DataSize;

public class MemoryUsageOptions {

    public static final DataSize DEFAULT_LIMIT = DataSize.ofBytes(0);

    public static final int DEFAULT_SAMPLES = 5;

    private DataSize limit = DEFAULT_LIMIT;

    private int samples = DEFAULT_SAMPLES;

    private MemoryUsageOptions(Builder builder) {
        this.limit = builder.limit;
        this.samples = builder.samples;
    }

    public DataSize getLimit() {
        return limit;
    }

    /**
     * 
     * @param limit maximum memory usage for a given key. Use 0 to disable memory usage checks, -1 for no limit.
     */
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

        private DataSize limit = DEFAULT_LIMIT;

        private int samples = DEFAULT_SAMPLES;

        private Builder() {
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
