package com.redis.spring.batch.step;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.observability.BatchMetrics;
import org.springframework.batch.core.step.item.SimpleChunkProvider;
import org.springframework.batch.core.step.item.SkipOverflowException;
import org.springframework.batch.core.step.skip.SkipListenerFailedException;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.repeat.RepeatOperations;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.util.Assert;

import com.redis.spring.batch.reader.PollableItemReader;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Timer.Sample;

/**
 * Fault-tolerant implementation of the ChunkProvider interface, that allows for
 * skipping or retry of items that cause exceptions, as well as incomplete
 * chunks when timeout is reached.
 */
public class FlushingChunkProvider<I> extends SimpleChunkProvider<I> {

	public static final Duration DEFAULT_FLUSH_INTERVAL = Duration.ofMillis(50);
	public static final Duration DEFAULT_IDLE_TIMEOUT = Duration.ofMillis(Long.MAX_VALUE);

	private final RepeatOperations repeatOperations;
	private Duration interval = DEFAULT_FLUSH_INTERVAL;
	// No idle timeout by default
	private long idleTimeout = DEFAULT_IDLE_TIMEOUT.toMillis();
	private long lastActivity = 0;

	public FlushingChunkProvider(ItemReader<? extends I> itemReader, RepeatOperations repeatOperations) {
		super(itemReader, repeatOperations);
		Assert.isTrue(itemReader instanceof PollableItemReader, "Reader must extend PollableItemReader");
		this.repeatOperations = repeatOperations;
	}

	public void setInterval(Duration interval) {
		this.interval = interval;
	}

	public void setIdleTimeout(Duration timeout) {
		Assert.notNull(timeout, "Idle timeout must not be null");
		this.idleTimeout = timeout.toMillis();
	}

	private void stopFlushTimer(Timer.Sample sample, StepExecution stepExecution, String status) {
		sample.stop(BatchMetrics.createTimer(Metrics.globalRegistry, "item.read", "Item reading duration",
				Tag.of("job.name", stepExecution.getJobExecution().getJobInstance().getJobName()),
				Tag.of("step.name", stepExecution.getStepName()), Tag.of("status", status)));
	}

	@Override
	public Chunk<I> provide(StepContribution contribution) {
		long start = System.currentTimeMillis();
		if (lastActivity == 0) {
			lastActivity = start;
		}
		final Chunk<I> inputs = new Chunk<>();
		repeatOperations.iterate(context -> {
			long pollingTimeout = interval.toMillis() - millisSince(start);
			if (pollingTimeout < 0) {
				return RepeatStatus.FINISHED;
			}
			Sample sample = Timer.start(Metrics.globalRegistry);
			I item;
			try {
				item = read(contribution, inputs, pollingTimeout);
			} catch (SkipOverflowException e) {
				// read() tells us about an excess of skips by throwing an exception
				stopFlushTimer(sample, contribution.getStepExecution(), BatchMetrics.STATUS_FAILURE);
				return RepeatStatus.FINISHED;
			}
			if (item == null) {
				if (millisSince(lastActivity) > idleTimeout) {
					inputs.setEnd();
				}
				return RepeatStatus.CONTINUABLE;
			}
			stopFlushTimer(sample, contribution.getStepExecution(), BatchMetrics.STATUS_SUCCESS);
			inputs.add(item);
			contribution.incrementReadCount();
			lastActivity = System.currentTimeMillis();
			return RepeatStatus.CONTINUABLE;
		});
		return inputs;
	}

	private long millisSince(long start) {
		return System.currentTimeMillis() - start;
	}

	protected I read(StepContribution contribution, Chunk<I> chunk, long timeout) throws InterruptedException {
		while (true) {
			try {
				return doRead(timeout);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw e;
			}
		}
	}

	@SuppressWarnings("unchecked")
	protected final I doRead(long timeout) throws InterruptedException {
		try {
			getListener().beforeRead();
			I item = ((PollableItemReader<I>) itemReader).poll(timeout, TimeUnit.MILLISECONDS);
			if (item != null) {
				getListener().afterRead(item);
			}
			return item;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw e;
		} catch (Exception e) {
			getListener().onReadError(e);
			throw e;
		}
	}

	@Override
	public void postProcess(StepContribution contribution, Chunk<I> chunk) {
		for (Exception e : chunk.getErrors()) {
			try {
				getListener().onSkipInRead(e);
			} catch (RuntimeException ex) {
				throw new SkipListenerFailedException("Fatal exception in SkipListener.", ex, e);
			}
		}
	}

}
