package com.redis.spring.batch.step;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.metrics.BatchMetrics;
import org.springframework.batch.core.step.item.Chunk;
import org.springframework.batch.core.step.item.FaultTolerantChunkProvider;
import org.springframework.batch.core.step.item.SkipOverflowException;
import org.springframework.batch.core.step.skip.LimitCheckingItemSkipPolicy;
import org.springframework.batch.core.step.skip.NonSkippableReadException;
import org.springframework.batch.core.step.skip.SkipException;
import org.springframework.batch.core.step.skip.SkipListenerFailedException;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.core.step.skip.SkipPolicyFailedException;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.repeat.RepeatOperations;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.classify.BinaryExceptionClassifier;
import org.springframework.classify.Classifier;
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
public class FlushingChunkProvider<I> extends FaultTolerantChunkProvider<I> {

	/**
	 * Hard limit for number of read skips in the same chunk. Should be sufficiently
	 * high that it is only encountered in a runaway step where all items are
	 * skipped before the chunk can complete (leading to a potential heap memory
	 * problem).
	 */
	public static final int DEFAULT_MAX_SKIPS_ON_READ = 100;
	public static final Duration DEFAULT_FLUSHING_INTERVAL = Duration.ofMillis(50);
	public static final long NO_IDLE_TIMEOUT = -1;

	private final RepeatOperations repeatOperations;

	private SkipPolicy skipPolicy = new LimitCheckingItemSkipPolicy();
	private Classifier<Throwable, Boolean> rollbackClassifier = new BinaryExceptionClassifier(true);
	private int maxSkipsOnRead = DEFAULT_MAX_SKIPS_ON_READ;

	private Duration interval = DEFAULT_FLUSHING_INTERVAL;
	private Optional<Duration> idleTimeout = Optional.empty();
	private long lastActivity = 0;

	public FlushingChunkProvider(ItemReader<? extends I> itemReader, RepeatOperations repeatOperations) {
		super(itemReader, repeatOperations);
		Assert.isTrue(itemReader instanceof PollableItemReader, "Reader must extend PollableItemReader");
		this.repeatOperations = repeatOperations;
	}

	public void setInterval(Duration interval) {
		this.interval = interval;
	}

	public void setIdleTimeout(Optional<Duration> timeout) {
		this.idleTimeout = timeout;
	}

	public void setIdleTimeout(Duration timeout) {
		setIdleTimeout(Optional.of(timeout));
	}

	/**
	 * @param maxSkipsOnRead the maximum number of skips on read
	 */
	@Override
	public void setMaxSkipsOnRead(int maxSkipsOnRead) {
		this.maxSkipsOnRead = maxSkipsOnRead;
	}

	/**
	 * The policy that determines whether exceptions can be skipped on read.
	 *
	 * @param skipPolicy instance of {@link SkipPolicy} to be used by
	 *                   FaultTolerantChunkProvider.
	 */
	@Override
	public void setSkipPolicy(SkipPolicy skipPolicy) {
		this.skipPolicy = skipPolicy;
	}

	/**
	 * Classifier to determine whether exceptions have been marked as no-rollback
	 * (as opposed to skippable). If encountered they are simply ignored, unless
	 * also skippable.
	 *
	 * @param rollbackClassifier the rollback classifier to set
	 */
	@Override
	public void setRollbackClassifier(Classifier<Throwable, Boolean> rollbackClassifier) {
		this.rollbackClassifier = rollbackClassifier;
	}

	private void stopTimer(Timer.Sample sample, StepExecution stepExecution, String status) {
		sample.stop(BatchMetrics.createTimer("item.read", "Item reading duration",
				Tag.of("job.name", stepExecution.getJobExecution().getJobInstance().getJobName()),
				Tag.of("step.name", stepExecution.getStepName()), Tag.of("status", status)));
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Chunk<I> provide(StepContribution contribution) {
		long start = System.currentTimeMillis();
		if (lastActivity == 0) {
			lastActivity = start;
		}
		final Chunk<I> inputs = new Chunk<>();
		repeatOperations.iterate(context -> {
			long pollingTimeout = interval.toMillis() - (System.currentTimeMillis() - start);
			if (pollingTimeout < 0) {
				return RepeatStatus.FINISHED;
			}
			Sample sample = Timer.start(Metrics.globalRegistry);
			I item;
			try {
				item = read(contribution, inputs, pollingTimeout);
			} catch (SkipOverflowException e) {
				// read() tells us about an excess of skips by throwing an exception
				stopTimer(sample, contribution.getStepExecution(), BatchMetrics.STATUS_FAILURE);
				return RepeatStatus.FINISHED;
			}
			if (item == null) {
				if (!((PollableItemReader) itemReader).isOpen() || isTimeout(lastActivity)) {
					inputs.setEnd();
				}
				return RepeatStatus.CONTINUABLE;
			}
			stopTimer(sample, contribution.getStepExecution(), BatchMetrics.STATUS_SUCCESS);
			inputs.add(item);
			contribution.incrementReadCount();
			lastActivity = System.currentTimeMillis();
			return RepeatStatus.CONTINUABLE;
		});
		return inputs;
	}

	private boolean isTimeout(long lastActivity) {
		return idleTimeout.isPresent() && System.currentTimeMillis() - lastActivity > idleTimeout.get().toMillis();
	}

	protected I read(StepContribution contribution, Chunk<I> chunk, long timeout) {
		while (true) {
			try {
				return doRead(timeout);
			} catch (Exception e) {

				if (shouldPolicySkip(skipPolicy, e, contribution.getStepSkipCount())) {

					// increment skip count and try again
					contribution.incrementReadSkipCount();
					chunk.skip(e);

					if (chunk.getErrors().size() >= maxSkipsOnRead) {
						throw new SkipOverflowException("Too many skips on read");
					}

					logger.debug("Skipping failed input", e);
				} else {
					if (Boolean.TRUE.equals(rollbackClassifier.classify(e))) {
						throw new NonSkippableReadException("Non-skippable exception during read", e);
					}
					logger.debug("No-rollback for non-skippable exception (ignored)", e);
				}

			}
		}
	}

	@SuppressWarnings("unchecked")
	protected final I doRead(long timeout) throws Exception {
		try {
			getListener().beforeRead();
			I item = ((PollableItemReader<I>) itemReader).poll(timeout, TimeUnit.MILLISECONDS);
			if (item != null) {
				getListener().afterRead(item);
			}
			return item;
		} catch (Exception e) {
			getListener().onReadError(e);
			throw e;
		}
	}

	/**
	 * Convenience method for calling process skip policy.
	 *
	 * @param policy    the skip policy
	 * @param e         the cause of the skip
	 * @param skipCount the current skip count
	 */
	private boolean shouldPolicySkip(SkipPolicy policy, Throwable e, int skipCount) {
		try {
			return policy.shouldSkip(e, skipCount);
		} catch (SkipException ex) {
			throw ex;
		} catch (RuntimeException ex) {
			throw new SkipPolicyFailedException("Fatal exception in SkipPolicy.", ex, e);
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
