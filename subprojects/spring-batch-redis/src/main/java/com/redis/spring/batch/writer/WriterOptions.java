package com.redis.spring.batch.writer;

public class WriterOptions {

    public static final MergePolicy DEFAULT_MERGE_POLICY = MergePolicy.OVERWRITE;

    public static final StreamIdPolicy DEFAULT_STREAM_ID_POLICY = StreamIdPolicy.PROPAGATE;

    public static final TtlPolicy DEFAULT_TTL_POLICY = TtlPolicy.PROPAGATE;

    private TtlPolicy ttlPolicy = DEFAULT_TTL_POLICY;

    private MergePolicy mergePolicy = DEFAULT_MERGE_POLICY;

    private StreamIdPolicy streamIdPolicy = DEFAULT_STREAM_ID_POLICY;

    private WriterOptions(Builder builder) {
        this.mergePolicy = builder.mergePolicy;
        this.streamIdPolicy = builder.streamIdPolicy;
        this.ttlPolicy = builder.ttlPolicy;
    }

    public TtlPolicy getTtlPolicy() {
        return ttlPolicy;
    }

    public void setTtlPolicy(TtlPolicy policy) {
        this.ttlPolicy = policy;
    }

    public MergePolicy getMergePolicy() {
        return mergePolicy;
    }

    public void setMergePolicy(MergePolicy policy) {
        this.mergePolicy = policy;
    }

    public StreamIdPolicy getStreamIdPolicy() {
        return streamIdPolicy;
    }

    public void setStreamIdPolicy(StreamIdPolicy policy) {
        this.streamIdPolicy = policy;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private TtlPolicy ttlPolicy = DEFAULT_TTL_POLICY;

        private MergePolicy mergePolicy = DEFAULT_MERGE_POLICY;

        private StreamIdPolicy streamIdPolicy = DEFAULT_STREAM_ID_POLICY;

        private Builder() {
        }

        public Builder ttlPolicy(TtlPolicy policy) {
            this.ttlPolicy = policy;
            return this;
        }

        public Builder mergePolicy(MergePolicy policy) {
            this.mergePolicy = policy;
            return this;
        }

        public Builder streamIdPolicy(StreamIdPolicy policy) {
            this.streamIdPolicy = policy;
            return this;
        }

        public WriterOptions build() {
            return new WriterOptions(this);
        }

    }

}
