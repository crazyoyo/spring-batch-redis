package com.redis.spring.batch.writer;

import java.util.Optional;

public class WriterOptions {

	private Optional<WaitForReplication> waitForReplication = Optional.empty();
	private boolean multiExec;

	private WriterOptions(Builder builder) {
		this.waitForReplication = builder.waitForReplication;
		this.multiExec = builder.multiExec;
	}

	public Optional<WaitForReplication> getWaitForReplication() {
		return waitForReplication;
	}

	public void setWaitForReplication(WaitForReplication waitForReplication) {
		setWaitForReplication(Optional.of(waitForReplication));
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

	public static final class Builder {

		private Optional<WaitForReplication> waitForReplication = Optional.empty();
		private boolean multiExec;

		private Builder() {
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
