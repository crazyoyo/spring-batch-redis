package com.redis.spring.batch;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;

import com.redis.spring.batch.support.FlushingStepBuilder;

public class FlushingStepTests extends AbstractTestBase {

	@Test
	public void testReaderSkipPolicy() throws Exception {
		String name = "skip-policy";
		List<Integer> items = IntStream.range(0, 100).boxed().collect(Collectors.toList());
		int interval = 2;
		DelegatingPollableItemReader<Integer> reader = new DelegatingPollableItemReader<>(new ListItemReader<>(items),
				TimeoutException::new, interval);
		ListItemWriter<Integer> writer = new ListItemWriter<>();
		FlushingStepBuilder<Integer, Integer> stepBuilder = new FlushingStepBuilder<>(
				jobFactory.step(name).<Integer, Integer>chunk(1).reader(reader).writer(writer));
		stepBuilder.idleTimeout(Duration.ofMillis(100)).skip(TimeoutException.class)
				.skipPolicy(new AlwaysSkipItemSkipPolicy());
		TaskletStep step = stepBuilder.build();
		JobExecution execution = jobFactory.runAsync(jobFactory.job(name, step).build(), new JobParameters());
		jobFactory.awaitRunning(execution);
		jobFactory.awaitTermination(execution);
		Assertions.assertEquals(items.size(), writer.getWrittenItems().size() * 2);
	}
}
