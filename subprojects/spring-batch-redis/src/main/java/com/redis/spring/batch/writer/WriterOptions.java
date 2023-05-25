package com.redis.spring.batch.writer;

import java.util.Optional;

import com.redis.spring.batch.common.PoolOptions;

public class WriterOptions {

	private PoolOptions poolOptions = PoolOptions.builder().build();
	private Optional<ReplicaOptions> replicaOptions = Optional.empty();
	private boolean multiExec;

	public WriterOptions() {

	}

	public WriterOptions(Builder builder) {
		this.poolOptions = builder.poolOptions;
		this.replicaOptions = builder.replicaOptions;
		this.multiExec = builder.multiExec;
	}

	public PoolOptions getPoolOptions() {
		return poolOptions;
	}

	public void setPoolOptions(PoolOptions poolOptions) {
		this.poolOptions = poolOptions;
	}

	public Optional<ReplicaOptions> getReplicaOptions() {
		return replicaOptions;
	}

	public void setReplicaOptions(Optional<ReplicaOptions> waitForReplication) {
		this.replicaOptions = waitForReplication;
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

	public static class Builder {

		private PoolOptions poolOptions = PoolOptions.builder().build();
		private Optional<ReplicaOptions> replicaOptions = Optional.empty();
		private boolean multiExec;

		public Builder poolOptions(PoolOptions options) {
			this.poolOptions = options;
			return this;
		}

		public Builder replicaOptions(ReplicaOptions options) {
			return replicaOptions(Optional.of(options));
		}

		public Builder replicaOptions(Optional<ReplicaOptions> waitForReplication) {
			this.replicaOptions = waitForReplication;
			return this;
		}

		public Builder multiExec(boolean multiExec) {
			this.multiExec = multiExec;
			return this;
		}

		public WriterOptions build() {
			return new WriterOptions(this);
		}
	}

}
