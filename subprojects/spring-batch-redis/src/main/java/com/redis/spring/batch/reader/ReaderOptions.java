package com.redis.spring.batch.reader;

import com.redis.spring.batch.common.StepOptions;

public class ReaderOptions {

	private StepOptions stepOptions = StepOptions.builder().build();
	private QueueOptions queueOptions = QueueOptions.builder().build();

	public ReaderOptions() {

	}

	private ReaderOptions(Builder builder) {
		this.stepOptions = builder.stepOptions;
		this.queueOptions = builder.queueOptions;
	}

	public void setQueueOptions(QueueOptions queueOptions) {
		this.queueOptions = queueOptions;
	}

	public QueueOptions getQueueOptions() {
		return queueOptions;
	}

	public StepOptions getStepOptions() {
		return stepOptions;
	}

	public void setStepOptions(StepOptions stepOptions) {
		this.stepOptions = stepOptions;
	}
	
	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {

		private StepOptions stepOptions = StepOptions.builder().build();
		private QueueOptions queueOptions = QueueOptions.builder().build();

		public Builder stepOptions(StepOptions options) {
			this.stepOptions = options;
			return this;
		}

		public Builder queueOptions(QueueOptions options) {
			this.queueOptions = options;
			return this;
		}

		public ReaderOptions build() {
			return new ReaderOptions(this);
		}

	}

}
