package com.redis.spring.batch.writer;

import java.util.Optional;

import com.redis.spring.batch.common.PoolOptions;

public class WriterOptions {

	private PoolOptions poolOptions = PoolOptions.builder().build();
	private Optional<WaitForReplication> waitForReplication = Optional.empty();
	private boolean multiExec;

	public WriterOptions(Builder builder) {
		this.poolOptions = builder.poolOptions;
		this.waitForReplication = builder.waitForReplication;
		this.multiExec = builder.multiExec;
	}

	public PoolOptions getPoolOptions() {
		return poolOptions;
	}

	public void setPoolOptions(PoolOptions poolOptions) {
		this.poolOptions = poolOptions;
	}

	public Optional<WaitForReplication> getWaitForReplication() {
		return waitForReplication;
	}

	public void setWaitForReplication(Optional<WaitForReplication> waitForReplication) {
		this.waitForReplication = waitForReplication;
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
		private Optional<WaitForReplication> waitForReplication = Optional.empty();
		private boolean multiExec;

		private Builder() {
		}

		public Builder poolOptions(PoolOptions options) {
			this.poolOptions = options;
			return this;
		}

		public Builder waitForReplication(WaitForReplication waitForReplication) {
			return waitForReplication(Optional.of(waitForReplication));
		}

		public Builder waitForReplication(Optional<WaitForReplication> waitForReplication) {
			this.waitForReplication = waitForReplication;
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
