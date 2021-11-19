package com.redis.spring.batch.test;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;

import com.redis.spring.batch.support.FlushingStepBuilder;
import com.redis.testcontainers.RedisServer;

class FlushingStepTests extends AbstractTestBase {

	@Override
	protected Collection<RedisServer> servers() {
		return Collections.emptyList();
	}

	@Test
	void testReaderSkipPolicy() throws Exception {
		String name = "skip-policy";
		List<Integer> items = IntStream.range(0, 100).boxed().collect(Collectors.toList());
		DelegatingPollableItemReader<Integer> reader = new DelegatingPollableItemReader<>(new ListItemReader<>(items));
		ListItemWriter<Integer> writer = new ListItemWriter<>();
		FlushingStepBuilder<Integer, Integer> stepBuilder = new FlushingStepBuilder<>(
				stepBuilderFactory.get(name).<Integer, Integer>chunk(1).reader(reader).writer(writer));
		stepBuilder.idleTimeout(Duration.ofMillis(100)).skip(TimeoutException.class)
				.skipPolicy(new AlwaysSkipItemSkipPolicy());
		launch(jobBuilderFactory.get(name).start(stepBuilder.build()).build());
		Assertions.assertEquals(items.size(), writer.getWrittenItems().size() * 2);
	}

}
