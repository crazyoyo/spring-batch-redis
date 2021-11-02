package com.redis.spring.batch.support;

import java.time.Duration;
import java.util.ArrayList;

import org.springframework.batch.core.StepListener;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilderHelper;
import org.springframework.batch.core.step.item.ChunkProvider;
import org.springframework.batch.core.step.item.FaultTolerantChunkProvider;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.ItemReader;
import org.springframework.util.Assert;

import lombok.Setter;
import lombok.experimental.Accessors;

@Setter
@Accessors(fluent = true)
public class FlushingStepBuilder<I, O> extends FaultTolerantStepBuilder<I, O> {

	public static final Duration DEFAULT_FLUSHING_INTERVAL = Duration.ofMillis(50);

	private Duration flushingInterval = DEFAULT_FLUSHING_INTERVAL;
	private Duration idleTimeout; // no idle stream detection by default

	public FlushingStepBuilder(StepBuilderHelper<?> parent) {
		super(parent);
	}

	public FlushingStepBuilder(SimpleStepBuilder<I, O> parent) {
		super(parent);
	}

	public FlushingStepBuilder<I, O> flushingInterval(Duration flushingInterval) {
		Utils.assertPositive(flushingInterval, "Flushing interval");
		this.flushingInterval = flushingInterval;
		return this;
	}

	public FlushingStepBuilder<I, O> idleTimeout(Duration idleTimeout) {
		this.idleTimeout = idleTimeout;
		return this;
	}

	@Override
	protected ChunkProvider<I> createChunkProvider() {

		SkipPolicy readSkipPolicy = createSkipPolicy();
		readSkipPolicy = getFatalExceptionAwareProxy(readSkipPolicy);
		FlushingChunkProvider<I> chunkProvider = new FlushingChunkProvider<>(getReader(), createChunkOperations());
		chunkProvider.setMaxSkipsOnRead(Math.max(getChunkSize(), FaultTolerantChunkProvider.DEFAULT_MAX_SKIPS_ON_READ));
		chunkProvider.setSkipPolicy(readSkipPolicy);
		chunkProvider.setRollbackClassifier(getRollbackClassifier());
		chunkProvider.setFlushingInterval(flushingInterval);
		chunkProvider.setIdleTimeout(idleTimeout);
		ArrayList<StepListener> listeners = new ArrayList<>(getItemListeners());
		listeners.addAll(getSkipListeners());
		chunkProvider.setListeners(listeners);

		return chunkProvider;
	}

	@Override
	public FlushingStepBuilder<I, O> reader(ItemReader<? extends I> reader) {
		Assert.state(reader instanceof PollableItemReader, "Reader must be an instance of PollableItemReader");
		return (FlushingStepBuilder<I, O>) super.reader(reader);
	}

}
