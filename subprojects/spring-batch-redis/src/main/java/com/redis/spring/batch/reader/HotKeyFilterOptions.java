package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.util.unit.DataSize;

import com.redis.spring.batch.common.DataStructure.Type;
import com.redis.spring.batch.common.StepOptions;

public class HotKeyFilterOptions {

	public static final double DEFAULT_MAX_THROUGHPUT = 10; // 10 updates per second
	public static final Duration DEFAULT_WINDOW = Duration.ofMillis(1000); // time window used to count updates

	private static final Type[] DEFAULT_BLOCKED_TYPES = { Type.LIST, Type.SET, Type.STREAM, Type.TIMESERIES,
			Type.ZSET };
	public static final DataSize DEFAULT_MAX_MEMORY_USAGE = DataSize.ofMegabytes(1);
	public static final Duration DEFAULT_PRUNE_INTERVAL = Duration.ofMinutes(1);
	public static final Duration DEFAULT_FLUSHING_INTERVAL = Duration.ofMillis(50);

	private QueueOptions candidateQueueOptions = QueueOptions.builder().build();
	private double updateRate = DEFAULT_MAX_THROUGHPUT; // Update rate threshold above which a key is considered hot
	private long window = DEFAULT_WINDOW.toMillis();
	private DataSize maxMemoryUsage = DEFAULT_MAX_MEMORY_USAGE;
	private Set<String> blockedTypes = defaultBlockedTypes();
	private StepOptions stepOptions = StepOptions.builder().flushingInterval(DEFAULT_FLUSHING_INTERVAL).build();
	private Duration pruneInterval = DEFAULT_PRUNE_INTERVAL;

	private HotKeyFilterOptions(Builder builder) {
		this.candidateQueueOptions = builder.candidateQueueOptions;
		this.updateRate = builder.maxThroughput;
		this.window = builder.window.toMillis();
		this.blockedTypes = builder.blockedTypes;
		this.stepOptions = builder.stepOptions;
		this.maxMemoryUsage = builder.maxMemoryUsage;
		this.pruneInterval = builder.pruneInterval;
	}

	public static Set<String> defaultBlockedTypes() {
		return Stream.of(DEFAULT_BLOCKED_TYPES).map(Type::getString).collect(Collectors.toSet());
	}

	public DataSize getMaxMemoryUsage() {
		return maxMemoryUsage;
	}

	public void setMaxMemoryUsage(DataSize maxMemoryUsage) {
		this.maxMemoryUsage = maxMemoryUsage;
	}

	public Duration getPruneInterval() {
		return pruneInterval;
	}

	public void setPruneInterval(Duration pruneInterval) {
		this.pruneInterval = pruneInterval;
	}

	public long getWindow() {
		return window;
	}

	public StepOptions getStepOptions() {
		return stepOptions;
	}

	public void setStepOptions(StepOptions stepOptions) {
		this.stepOptions = stepOptions;
	}

	public QueueOptions getCandidateQueueOptions() {
		return candidateQueueOptions;
	}

	public void setCandidateQueueOptions(QueueOptions candidateQueueOptions) {
		this.candidateQueueOptions = candidateQueueOptions;
	}

	public double getMaxThroughput() {
		return updateRate;
	}

	public void setMaxThroughput(double updateRate) {
		this.updateRate = updateRate;
	}

	public Set<String> getBlockedTypes() {
		return blockedTypes;
	}

	public void setBlockedTypes(Set<String> blockedTypes) {
		this.blockedTypes = blockedTypes;
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		private QueueOptions candidateQueueOptions = QueueOptions.builder().build();
		private double maxThroughput = DEFAULT_MAX_THROUGHPUT;
		private Duration window = DEFAULT_WINDOW;
		private DataSize maxMemoryUsage = DEFAULT_MAX_MEMORY_USAGE;
		private Set<String> blockedTypes = defaultBlockedTypes();
		private StepOptions stepOptions = StepOptions.builder().flushingInterval(DEFAULT_FLUSHING_INTERVAL).build();
		private Duration pruneInterval = DEFAULT_PRUNE_INTERVAL;

		public Builder candidateQueueOptions(QueueOptions queueOptions) {
			this.candidateQueueOptions = queueOptions;
			return this;
		}

		public Builder stepOptions(StepOptions stepOptions) {
			this.stepOptions = stepOptions;
			return this;
		}

		public Builder maxThroughput(double maxThroughput) {
			this.maxThroughput = maxThroughput;
			return this;
		}

		public Builder blockedTypes(String... blockedTypes) {
			this.blockedTypes = new HashSet<>(Arrays.asList(blockedTypes));
			return this;
		}

		public Builder maxMemoryUsage(DataSize maxMemoryUsage) {
			this.maxMemoryUsage = maxMemoryUsage;
			return this;
		}

		public Builder window(Duration window) {
			this.window = window;
			return this;
		}

		public Builder pruneInterval(Duration interval) {
			this.pruneInterval = interval;
			return this;
		}

		public HotKeyFilterOptions build() {
			return new HotKeyFilterOptions(this);
		}
	}

}
