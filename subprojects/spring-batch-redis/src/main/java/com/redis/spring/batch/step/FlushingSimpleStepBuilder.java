package com.redis.spring.batch.step;

import java.time.Duration;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.StepListener;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilderHelper;
import org.springframework.batch.core.step.item.ChunkProvider;
import org.springframework.batch.core.step.item.FaultTolerantChunkProvider;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.repeat.CompletionPolicy;
import org.springframework.util.Assert;

import com.redis.spring.batch.reader.PollableItemReader;

public class FlushingSimpleStepBuilder<I, O> extends FaultTolerantStepBuilder<I, O> {

	public static final long DEFAULT_FLUSHING_INTERVAL = 50;

	private final Log log = LogFactory.getLog(getClass());

	private long interval = FlushingSimpleStepBuilder.DEFAULT_FLUSHING_INTERVAL;
	private long idleTimeout = FlushingChunkProvider.NO_IDLE_TIMEOUT;

	public FlushingSimpleStepBuilder(StepBuilderHelper<?> parent) {
		super(parent);
	}

	public FlushingSimpleStepBuilder(SimpleStepBuilder<I, O> parent) {
		super(parent);
	}

	@Override
	public FlushingSimpleStepBuilder<I, O> chunk(int chunkSize) {
		return (FlushingSimpleStepBuilder<I, O>) super.chunk(chunkSize);
	}

	@Override
	public FlushingSimpleStepBuilder<I, O> chunk(CompletionPolicy completionPolicy) {
		return (FlushingSimpleStepBuilder<I, O>) super.chunk(completionPolicy);
	}

	public FlushingSimpleStepBuilder<I, O> interval(long millis) {
		this.interval = millis;
		return this;
	}

	public FlushingSimpleStepBuilder<I, O> idleTimeout(Duration timeout) {
		return idleTimeout(timeout.toMillis());
	}

	public FlushingSimpleStepBuilder<I, O> idleTimeout(long millis) {
		this.idleTimeout = millis;
		return this;
	}

	@Override
	public FlushingSimpleStepBuilder<I, O> faultTolerant() {
		return (FlushingSimpleStepBuilder<I, O>) super.faultTolerant();
	}

	@Override
	protected ChunkProvider<I> createChunkProvider() {
		SkipPolicy readSkipPolicy = createSkipPolicy();
		readSkipPolicy = getFatalExceptionAwareProxy(readSkipPolicy);
		int maxSkipsOnRead = maxSkipsOnRead();
		log.debug(String.format("Creating chunk provider: maxSkipsOnRead=%s skipPolicy=%s interval=%s idleTimeout=%s",
				maxSkipsOnRead, readSkipPolicy, interval, idleTimeout));
		FlushingChunkProvider<I> chunkProvider = new FlushingChunkProvider<>(getReader(), createChunkOperations());
		chunkProvider.setMaxSkipsOnRead(maxSkipsOnRead);
		chunkProvider.setSkipPolicy(readSkipPolicy);
		chunkProvider.setRollbackClassifier(getRollbackClassifier());
		chunkProvider.setInterval(interval);
		chunkProvider.setIdleTimeout(idleTimeout);
		ArrayList<StepListener> listeners = new ArrayList<>(getItemListeners());
		listeners.addAll(getSkipListeners());
		chunkProvider.setListeners(listeners);
		return chunkProvider;
	}

	private int maxSkipsOnRead() {
		return Math.max(getChunkSize(), FaultTolerantChunkProvider.DEFAULT_MAX_SKIPS_ON_READ);
	}

	@Override
	public FlushingSimpleStepBuilder<I, O> reader(ItemReader<? extends I> reader) {
		Assert.state(reader instanceof PollableItemReader, "Reader must be an instance of PollableItemReader");
		return (FlushingSimpleStepBuilder<I, O>) super.reader(reader);
	}

}
