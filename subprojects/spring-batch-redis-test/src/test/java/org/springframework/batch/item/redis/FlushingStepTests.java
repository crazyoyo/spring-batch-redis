package org.springframework.batch.item.redis;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.FlushingStepBuilder;
import org.springframework.batch.item.redis.support.PollableItemReader;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.retry.policy.NeverRetryPolicy;
import org.springframework.util.ClassUtils;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FlushingStepTests extends TestBase {

    static class DelegatingPollableItemReader<T> extends AbstractItemCountingItemStreamItemReader<T> implements PollableItemReader<T> {

        private final ItemReader<T> delegate;
        private final Supplier<Exception> exceptionSupplier;
        private final long interval;

        public DelegatingPollableItemReader(ItemReader<T> delegate, Supplier<Exception> exceptionSupplier, long interval) {
            setName(ClassUtils.getShortName(DelegatingPollableItemReader.class));
            this.delegate = delegate;
            this.exceptionSupplier = exceptionSupplier;
            this.interval = interval;
        }

        @Override
        public T poll(long timeout, TimeUnit unit) throws Exception {
            return read();
        }

        @Override
        protected T doRead() throws Exception {
            T result = delegate.read();
            if (getCurrentItemCount() % interval == 0) {
                throw exceptionSupplier.get();
            }
            return result;
        }

        @Override
        protected void doOpen() {
        }

        @Override
        protected void doClose() {
        }

    }

    class ThrowingItemWriter<T> extends AbstractItemStreamItemWriter<T> {

        private final Supplier<Exception> exceptionSupplier;
        private final long interval;
        private final AtomicLong index = new AtomicLong();

        public ThrowingItemWriter(Supplier<Exception> exceptionSupplier, long interval) {
            this.exceptionSupplier = exceptionSupplier;
            this.interval = interval;
        }

        @Override
        public void write(List<? extends T> items) throws Exception {
            if (index.getAndIncrement() % interval == 0) {
                throw exceptionSupplier.get();
            }
        }

    }

    @Test
    public void testReaderSkipPolicy() throws Exception {
        List<Integer> items = IntStream.range(0, 100).boxed().collect(Collectors.toList());
        int interval = 2;
        DelegatingPollableItemReader<Integer> reader = new DelegatingPollableItemReader<>(new ListItemReader<>(items), TimeoutException::new, interval);
        ListItemWriter<Integer> writer = new ListItemWriter<>();
        FlushingStepBuilder<Integer, Integer> stepBuilder = new FlushingStepBuilder<>(steps.get("skip-policy-step").<Integer, Integer>chunk(1).reader(reader).writer(writer));
        stepBuilder.idleTimeout(Duration.ofMillis(100)).skip(TimeoutException.class).skipPolicy(new AlwaysSkipItemSkipPolicy());
        TaskletStep step = stepBuilder.build();
        JobExecution execution = asyncJobLauncher.run(job("skip-policy", step), new JobParameters());
        awaitRunning(execution);
        awaitJobTermination(execution);
        Assertions.assertEquals(items.size(), writer.getWrittenItems().size() * 2);
    }
}
