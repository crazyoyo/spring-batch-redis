package com.redis.spring.batch.writer;

import com.redis.spring.batch.common.PoolOptions;

public class WriteOperationOptions {

    private PoolOptions poolOptions = PoolOptions.builder().build();

    private ReplicaWaitOptions replicaWaitOptions = ReplicaWaitOptions.builder().build();

    private boolean multiExec;

    private WriteOperationOptions(Builder builder) {
        this.poolOptions = builder.poolOptions;
        this.replicaWaitOptions = builder.replicaWaitOptions;
        this.multiExec = builder.multiExec;
    }

    public PoolOptions getPoolOptions() {
        return poolOptions;
    }

    public void setPoolOptions(PoolOptions poolOptions) {
        this.poolOptions = poolOptions;
    }

    public ReplicaWaitOptions getReplicaWaitOptions() {
        return replicaWaitOptions;
    }

    public void setReplicaWaitOptions(ReplicaWaitOptions replicaWaitOptions) {
        this.replicaWaitOptions = replicaWaitOptions;
    }

    public boolean isMultiExec() {
        return multiExec;
    }

    public void setMultiExec(boolean multiExec) {
        this.multiExec = multiExec;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private PoolOptions poolOptions = PoolOptions.builder().build();

        private ReplicaWaitOptions replicaWaitOptions = ReplicaWaitOptions.builder().build();

        private boolean multiExec;

        private Builder() {
        }

        public Builder poolOptions(PoolOptions options) {
            this.poolOptions = options;
            return this;
        }

        public Builder replicaWaitOptions(ReplicaWaitOptions replicaWaitOptions) {
            this.replicaWaitOptions = replicaWaitOptions;
            return this;
        }

        public Builder multiExec(boolean multiExec) {
            this.multiExec = multiExec;
            return this;
        }

        public WriteOperationOptions build() {
            return new WriteOperationOptions(this);
        }

    }

}
