package com.redis.spring.batch.reader;

import com.redis.spring.batch.common.PoolOptions;
import com.redis.spring.batch.common.StepOptions;

public class ReaderOptions {

	private PoolOptions poolOptions = PoolOptions.builder().build();
	private StepOptions stepOptions = StepOptions.builder().build();
	private QueueOptions queueOptions = QueueOptions.builder().build();

	private ReaderOptions(Builder builder) {
		this.poolOptions = builder.poolOptions;
		this.stepOptions = builder.stepOptions;
		this.queueOptions = builder.queueOptions;
	}

	public PoolOptions getPoolOptions() {
		return poolOptions;
	}

	public void setPoolOptions(PoolOptions poolOptions) {
		this.poolOptions = poolOptions;
	}

	public StepOptions getStepOptions() {
		return stepOptions;
	}

	public void setStepOptions(StepOptions stepOptions) {
		this.stepOptions = stepOptions;
	}

	public QueueOptions getQueueOptions() {
		return queueOptions;
	}

	public void setQueueOptions(QueueOptions queueOptions) {
		this.queueOptions = queueOptions;
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		private PoolOptions poolOptions = PoolOptions.builder().build();
		private StepOptions stepOptions = StepOptions.builder().build();
		private QueueOptions queueOptions = QueueOptions.builder().build();

		private Builder() {
		}

		public Builder poolOptions(PoolOptions poolOptions) {
			this.poolOptions = poolOptions;
			return this;
		}

		public Builder stepOptions(StepOptions stepOptions) {
			this.stepOptions = stepOptions;
			return this;
		}

		public Builder queueOptions(QueueOptions queueOptions) {
			this.queueOptions = queueOptions;
			return this;
		}

		public ReaderOptions build() {
			return new ReaderOptions(this);
		}
	}

}
