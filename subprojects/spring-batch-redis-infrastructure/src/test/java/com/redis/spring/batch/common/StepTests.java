package com.redis.spring.batch.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.boot.test.context.SpringBootTest;

import com.redis.spring.batch.JobFactory;
import com.redis.spring.batch.step.FlushingFaultTolerantStepBuilder;
import com.redis.spring.batch.step.FlushingStepBuilder;

import io.lettuce.core.RedisCommandTimeoutException;

@SpringBootTest(classes = BatchTestApplication.class)
@TestInstance(Lifecycle.PER_CLASS)
class StepTests {

	private JobFactory jobFactory;

	@BeforeAll
	void initialize() throws Exception {
		jobFactory = new JobFactory();
		jobFactory.setName("steptests");
		jobFactory.afterPropertiesSet();
	}

	@Test
	void flushingFaultTolerantStep() throws Exception {
		int count = 100;
		List<String> list = IntStream.range(0, 100).mapToObj(String::valueOf).collect(Collectors.toList());
		ErrorItemReader<String> reader = new ErrorItemReader<>(new ListItemReader<>(list));
		ListItemWriter<String> writer = new ListItemWriter<>();
		String name = "readKeyValueFaultTolerance";
		FlushingStepBuilder<String, String> step = new FlushingStepBuilder<>(jobFactory.step(name, 1));
		step.reader(reader);
		step.writer(writer);
		step.idleTimeout(Duration.ofMillis(300));
		FlushingFaultTolerantStepBuilder<String, String> ftStep = step.faultTolerant();
		ftStep.skipPolicy(new AlwaysSkipItemSkipPolicy());
		Job job = jobFactory.jobBuilder(name).start(ftStep.build()).build();
		jobFactory.run(job);
		assertEquals(count * ErrorItemReader.DEFAULT_ERROR_RATE, writer.getWrittenItems().size());
	}

	@Test
	void readerSkipPolicy() throws Exception {
		String name = "skip-policy";
		List<Integer> items = IntStream.range(0, 100).boxed().collect(Collectors.toList());
		ErrorItemReader<Integer> reader = new ErrorItemReader<>(new ListItemReader<>(items));
		ListItemWriter<Integer> writer = new ListItemWriter<>();
		SimpleStepBuilder<Integer, Integer> step = jobFactory.step(name, 1);
		step.reader(reader);
		step.writer(writer);
		FlushingFaultTolerantStepBuilder<Integer, Integer> ftStep = new FlushingFaultTolerantStepBuilder<>(step);
		ftStep.idleTimeout(Duration.ofMillis(300));
		ftStep.skip(RedisCommandTimeoutException.class);
		ftStep.skipPolicy(new AlwaysSkipItemSkipPolicy());
		Job job = jobFactory.jobBuilder(name).start(ftStep.build()).build();
		jobFactory.run(job);
		assertEquals(items.size(), writer.getWrittenItems().size() * 2);
	}

	@Test
	void flushingStep() throws Exception {
		String name = "flushingStep";
		int count = 100;
		BlockingQueue<String> queue = new LinkedBlockingDeque<>(count);
		QueueItemReader<String> reader = new QueueItemReader<>(queue);
		ListItemWriter<String> writer = new ListItemWriter<>();
		FlushingStepBuilder<String, String> step = new FlushingStepBuilder<>(jobFactory.step(name, 50));
		step.reader(reader);
		step.writer(writer);
		step.idleTimeout(Duration.ofMillis(500));
		step.listener(new ItemReadListener<String>() {

			private AtomicBoolean running = new AtomicBoolean();

			@Override
			public synchronized void beforeRead() {
				if (running.get()) {
					return;
				}
				running.set(true);
				Executors.newSingleThreadExecutor().execute(() -> {
					for (int index = 0; index < count; index++) {
						queue.offer("key" + index);
					}
				});
			}
		});
		Job job = jobFactory.jobBuilder(name).start(step.build()).build();
		jobFactory.run(job);
		writer.getWrittenItems().forEach(System.out::println);
		assertEquals(count, writer.getWrittenItems().size());
	}

}
