package org.springframework.batch.item.redis.support;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepListener;
import org.springframework.batch.core.listener.MulticasterBatchListener;
import org.springframework.batch.core.metrics.BatchMetrics;
import org.springframework.batch.core.step.item.Chunk;
import org.springframework.batch.core.step.item.ChunkProvider;
import org.springframework.batch.core.step.item.SkipOverflowException;
import org.springframework.batch.repeat.RepeatOperations;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * ChunkProvider that allows for incomplete chunks when timeout is reached
 */
@Slf4j
public class FlushingChunkProvider<I> implements ChunkProvider<I> {

    private final PollableItemReader<? extends I> itemReader;
    private final MulticasterBatchListener<I, ?> listener = new MulticasterBatchListener<>();
    private final RepeatOperations repeatOperations;
    private final long timeout; // milliseconds

    public FlushingChunkProvider(PollableItemReader<? extends I> itemReader, RepeatOperations repeatOperations, Duration timeout) {
        Assert.notNull(itemReader, "Item reader is required.");
        Assert.notNull(repeatOperations, "Repeat operations are required.");
        Assert.notNull(timeout, "Timeout is required.");
        this.itemReader = itemReader;
        this.repeatOperations = repeatOperations;
        this.timeout = timeout.toMillis();
    }

    /**
     * Register some {@link StepListener}s with the handler. Each will get the
     * callbacks in the order specified at the correct stage.
     *
     * @param listeners list of {@link StepListener}s.
     */
    public void setListeners(List<? extends StepListener> listeners) {
        for (StepListener listener : listeners) {
            registerListener(listener);
        }
    }

    /**
     * Register a listener for callbacks at the appropriate stages in a process.
     *
     * @param listener a {@link StepListener}
     */
    public void registerListener(StepListener listener) {
        this.listener.register(listener);
    }

    @Override
    public Chunk<I> provide(final StepContribution contribution) {

        final long start = System.currentTimeMillis();
        final Chunk<I> inputs = new Chunk<>();
        repeatOperations.iterate(context -> {
            Timer.Sample sample = Timer.start(Metrics.globalRegistry);
            String status = BatchMetrics.STATUS_SUCCESS;
            I item = null;
            try {
                item = poll(timeout - (System.currentTimeMillis() - start));
                if (item == null) {
                    if (itemReader.isTerminated()) {
                        log.debug("End chunk");
                        inputs.setEnd();
                    }
                    return RepeatStatus.FINISHED;
                }
            } catch (SkipOverflowException e) {
                // read() tells us about an excess of skips by throwing an exception
                status = BatchMetrics.STATUS_FAILURE;
                return RepeatStatus.FINISHED;
            } finally {
                stopTimer(sample, contribution.getStepExecution(), status);
            }
            inputs.add(item);
            contribution.incrementReadCount();
            return RepeatStatus.CONTINUABLE;
        });
        return inputs;
    }

    private void stopTimer(Timer.Sample sample, StepExecution stepExecution, String status) {
        sample.stop(BatchMetrics.createTimer("item.read", "Item reading duration", Tag.of("job.name", stepExecution.getJobExecution().getJobInstance().getJobName()), Tag.of("step.name", stepExecution.getStepName()), Tag.of("status", status)));
    }

    protected I poll(long timeout) throws InterruptedException {
        listener.beforeRead();
        I item = itemReader.poll(timeout, TimeUnit.MILLISECONDS);
        if (item != null) {
            listener.afterRead(item);
        }
        return item;
    }

    @Override
    public void postProcess(StepContribution contribution, Chunk<I> chunk) {
        // do nothing
    }

}
