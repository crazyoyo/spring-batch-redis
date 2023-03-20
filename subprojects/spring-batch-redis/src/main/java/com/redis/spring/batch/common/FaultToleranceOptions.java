package com.redis.spring.batch.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.springframework.batch.core.step.skip.SkipPolicy;

public class FaultToleranceOptions {

	public static final int DEFAULT_SKIP_LIMIT = 0;

	private Optional<SkipPolicy> skipPolicy = Optional.empty();
	private int skipLimit = DEFAULT_SKIP_LIMIT;
	private List<Class<? extends Throwable>> skip = new ArrayList<>();
	private List<Class<? extends Throwable>> noSkip = new ArrayList<>();

	private FaultToleranceOptions(Builder builder) {
		this.skip = builder.skip;
		this.noSkip = builder.noSkip;
		this.skipLimit = builder.skipLimit;
		this.skipPolicy = builder.skipPolicy;
	}

	public List<Class<? extends Throwable>> getSkip() {
		return skip;
	}

	public void setSkip(List<Class<? extends Throwable>> skip) {
		this.skip = skip;
	}

	public List<Class<? extends Throwable>> getNoSkip() {
		return noSkip;
	}

	public void setNoSkip(List<Class<? extends Throwable>> noSkip) {
		this.noSkip = noSkip;
	}

	public int getSkipLimit() {
		return skipLimit;
	}

	public void setSkipLimit(int skipLimit) {
		this.skipLimit = skipLimit;
	}

	public Optional<SkipPolicy> getSkipPolicy() {
		return skipPolicy;
	}

	public void setSkipPolicy(Optional<SkipPolicy> skipPolicy) {
		this.skipPolicy = skipPolicy;
	}

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {

		private int skipLimit = DEFAULT_SKIP_LIMIT;
		private List<Class<? extends Throwable>> skip = new ArrayList<>();
		private List<Class<? extends Throwable>> noSkip = new ArrayList<>();
		private Optional<SkipPolicy> skipPolicy = Optional.empty();

		public FaultToleranceOptions build() {
			return new FaultToleranceOptions(this);
		}

		private Builder() {
		}

		@SuppressWarnings("unchecked")
		public Builder skip(Class<? extends Throwable>... types) {
			return skip(Arrays.asList(types));
		}

		public Builder skip(List<Class<? extends Throwable>> types) {
			this.skip.addAll(types);
			return this;
		}

		public Builder noSkip(List<Class<? extends Throwable>> types) {
			this.noSkip.addAll(types);
			return this;
		}

		@SuppressWarnings("unchecked")
		public Builder noSkip(Class<? extends Throwable>... types) {
			return noSkip(Arrays.asList(types));
		}

		public Builder skipLimit(int skipLimit) {
			this.skipLimit = skipLimit;
			return this;
		}

		public Builder skipPolicy(SkipPolicy skipPolicy) {
			return skipPolicy(Optional.of(skipPolicy));
		}

		public Builder skipPolicy(Optional<SkipPolicy> skipPolicy) {
			this.skipPolicy = skipPolicy;
			return this;
		}

	}

}