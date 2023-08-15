package com.redis.spring.batch.step;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.metrics.BatchMetrics;
import org.springframework.batch.core.step.item.Chunk;
import org.springframework.batch.core.step.item.SimpleChunkProvider;
import org.springframework.batch.core.step.item.SkipOverflowException;
import org.springframework.batch.core.step.skip.SkipListenerFailedException;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.repeat.RepeatOperations;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.util.Assert;

import com.redis.spring.batch.reader.PollableItemReader;
import com.redis.spring.batch.reader.PollingException;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Timer.Sample;

/**
 * Fault-tolerant implementation of the ChunkProvider interface, that allows for skipping or retry of items that cause
 * exceptions, as well as incomplete chunks when timeout is reached.
 */
public class FlushingChunkProvider<I> extends SimpleChunkProvider<I> {

    public static final Duration DEFAULT_FLUSHING_INTERVAL = Duration.ofMillis(50);

    public static final Duration DEFAULT_IDLE_TIMEOUT = Duration.ZERO; // no idle stream detection by default

    private final RepeatOperations repeatOperations;

    private Duration interval = DEFAULT_FLUSHING_INTERVAL;

    private Duration idleTimeout = DEFAULT_IDLE_TIMEOUT;

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
        this.idleTimeout = timeout;
    }

    private void stopFlushingTimer(Timer.Sample sample, StepExecution stepExecution, String status) {
        sample.stop(BatchMetrics.createTimer("item.read", "Item reading duration",
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
                stopFlushingTimer(sample, contribution.getStepExecution(), BatchMetrics.STATUS_FAILURE);
                return RepeatStatus.FINISHED;
            }
            if (item == null) {
                if (isTimeout()) {
                    inputs.setEnd();
                }
                return RepeatStatus.CONTINUABLE;
            }
            stopFlushingTimer(sample, contribution.getStepExecution(), BatchMetrics.STATUS_SUCCESS);
            inputs.add(item);
            contribution.incrementReadCount();
            lastActivity = System.currentTimeMillis();
            return RepeatStatus.CONTINUABLE;
        });
        return inputs;
    }

    private boolean isTimeout() {
        if (idleTimeout.isZero() || idleTimeout.isNegative()) {
            return false;
        }
        return millisSinceLastActivity() > idleTimeout.toMillis();
    }

    private long millisSinceLastActivity() {
        return System.currentTimeMillis() - lastActivity;
    }

    protected I read(StepContribution contribution, Chunk<I> chunk, long timeout)
            throws InterruptedException, PollingException {
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
    protected final I doRead(long timeout) throws InterruptedException, PollingException {
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
        } catch (PollingException e) {
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
