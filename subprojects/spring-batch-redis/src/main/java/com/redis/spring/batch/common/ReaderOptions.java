package com.redis.spring.batch.common;

import java.util.Optional;

import com.redis.spring.batch.reader.QueueOptions;

public class ReaderOptions {

	private Optional<JobRunner> jobRunner = Optional.empty();
	private PoolOptions poolOptions = PoolOptions.builder().build();
	private StepOptions stepOptions = StepOptions.builder().build();
	private QueueOptions queueOptions = QueueOptions.builder().build();

	private ReaderOptions(Builder builder) {
		this.poolOptions = builder.poolOptions;
		this.stepOptions = builder.stepOptions;
		this.queueOptions = builder.queueOptions;
		this.jobRunner = builder.jobRunner;
	}

	public Optional<JobRunner> getJobRunner() {
		return jobRunner;
	}

	public void setJobRunner(Optional<JobRunner> jobRunner) {
		this.jobRunner = jobRunner;
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

		private Optional<JobRunner> jobRunner = Optional.empty();
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

		public Builder jobRunner(JobRunner jobRunner) {
			this.jobRunner = Optional.of(jobRunner);
			return this;
		}

		public ReaderOptions build() {
			return new ReaderOptions(this);
		}
	}

}
