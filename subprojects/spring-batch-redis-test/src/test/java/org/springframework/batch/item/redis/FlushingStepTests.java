package org.springframework.batch.item.redis;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.redis.support.FlushingStepBuilder;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;

public class FlushingStepTests extends AbstractTestBase {

    @Test
    public void testReaderSkipPolicy() throws Throwable {
        List<Integer> items = IntStream.range(0, 100).boxed().collect(Collectors.toList());
        int interval = 2;
        DelegatingPollableItemReader<Integer> reader = new DelegatingPollableItemReader<>(new ListItemReader<>(items), TimeoutException::new, interval);
        ListItemWriter<Integer> writer = new ListItemWriter<>();
        FlushingStepBuilder<Integer, Integer> stepBuilder = new FlushingStepBuilder<>(jobFactory.step("skip-policy-step").<Integer, Integer>chunk(1).reader(reader).writer(writer));
        stepBuilder.idleTimeout(Duration.ofMillis(100)).skip(TimeoutException.class).skipPolicy(new AlwaysSkipItemSkipPolicy());
        TaskletStep step = stepBuilder.build();
        jobFactory.runAsync(jobFactory.job("skip-policy", step).build(), new JobParameters()).awaitRunning().awaitTermination();
        Assertions.assertEquals(items.size(), writer.getWrittenItems().size() * 2);
    }
}
