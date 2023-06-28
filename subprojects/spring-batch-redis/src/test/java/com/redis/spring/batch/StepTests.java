package com.redis.spring.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.PlatformTransactionManager;

import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.reader.GeneratorItemReader;
import com.redis.spring.batch.reader.GeneratorItemReader.Type;
import com.redis.spring.batch.reader.PollableItemReader;
import com.redis.spring.batch.step.FlushingChunkProvider;
import com.redis.spring.batch.step.FlushingFaultTolerantStepBuilder;
import com.redis.spring.batch.step.FlushingStepBuilder;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
@TestInstance(Lifecycle.PER_CLASS)
class StepTests {

	@Autowired
	protected JobRepository jobRepository;
	@Autowired
	protected PlatformTransactionManager transactionManager;
	@Autowired
	protected JobBuilderFactory jobBuilderFactory;
	@Autowired
	protected StepBuilderFactory stepBuilderFactory;
	@Autowired
	private JobLauncher jobLauncher;
	private SimpleJobLauncher asyncJobLauncher;

	@BeforeAll
	void initialize() {
		asyncJobLauncher = new SimpleJobLauncher();
		asyncJobLauncher.setJobRepository(jobRepository);
		asyncJobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
	}

	@Test
	void flushingFaultTolerantStep() throws Exception {
		int count = 100;
		GeneratorItemReader gen = new GeneratorItemReader();
		gen.setMaxItemCount(count);
		gen.setTypes(Arrays.asList(Type.STRING));
		ErrorItemReader<DataStructure<String>> reader = new ErrorItemReader<>(gen);
		SynchronizedListItemWriter<DataStructure<String>> writer = new SynchronizedListItemWriter<>();
		String name = "readKeyValueFaultTolerance";
		FlushingStepBuilder<DataStructure<String>, DataStructure<String>> step = new FlushingStepBuilder<>(
				stepBuilderFactory.get(name));
		step.chunk(1);
		step.reader(reader);
		step.writer(writer);
		step.idleTimeout(Duration.ofMillis(300));
		FlushingFaultTolerantStepBuilder<DataStructure<String>, DataStructure<String>> ftStep = step.faultTolerant();
		ftStep.skip(TimeoutException.class);
		ftStep.skipPolicy(new AlwaysSkipItemSkipPolicy());
		Job job = jobBuilderFactory.get(name).start(ftStep.build()).build();
		jobLauncher.run(job, new JobParameters());
		assertEquals(count * ErrorItemReader.DEFAULT_ERROR_RATE, writer.getItems().size());
	}

	@Test
	void readerSkipPolicy() throws Exception {
		String name = "skip-policy";
		List<Integer> items = IntStream.range(0, 100).boxed().collect(Collectors.toList());
		ErrorItemReader<Integer> reader = new ErrorItemReader<>(new ListItemReader<>(items));
		SynchronizedListItemWriter<Integer> writer = new SynchronizedListItemWriter<>();
		SimpleStepBuilder<Integer, Integer> step = stepBuilderFactory.get(name).chunk(1);
		step.reader(reader);
		step.writer(writer);
		FlushingFaultTolerantStepBuilder<Integer, Integer> ftStep = new FlushingFaultTolerantStepBuilder<>(step);
		ftStep.idleTimeout(Duration.ofMillis(300));
		ftStep.skip(TimeoutException.class);
		ftStep.skipPolicy(new AlwaysSkipItemSkipPolicy());
		Job job = jobBuilderFactory.get(name).start(ftStep.build()).build();
		jobLauncher.run(job, new JobParameters());
		assertEquals(items.size(), writer.getItems().size() * 2);
	}

	@Test
	void flushingStep() throws Exception {
		String name = "flushingStep";
		int count = 100;
		BlockingQueue<String> queue = new LinkedBlockingDeque<>(count);
		QueueItemReader<String> reader = new QueueItemReader<>(queue, Duration.ofMillis(10));
		SynchronizedListItemWriter<String> writer = new SynchronizedListItemWriter<>();
		SimpleStepBuilder<String, String> step = stepBuilderFactory.get(name).chunk(50);
		step.reader(reader);
		step.writer(writer);
		FlushingStepBuilder<String, String> flushingStep = new FlushingStepBuilder<>(step);
		flushingStep.flushingInterval(FlushingChunkProvider.DEFAULT_FLUSHING_INTERVAL);
		flushingStep.idleTimeout(Duration.ofMillis(500));
		Job job = jobBuilderFactory.get(name).start(flushingStep.build()).build();
		JobExecution execution = asyncJobLauncher.run(job, new JobParameters());
		for (int index = 1; index <= count; index++) {
			queue.offer("key" + index);
		}
		AbstractTestBase.awaitTermination(execution);
		assertEquals(count, writer.getItems().size());
	}

	private static class QueueItemReader<T> implements PollableItemReader<T> {

		private final BlockingQueue<T> queue;
		private final long timeout;

		public QueueItemReader(BlockingQueue<T> queue, Duration timeout) {
			this.queue = queue;
			this.timeout = timeout.toMillis();
		}

		@Override
		public T read() throws InterruptedException {
			return poll(timeout, TimeUnit.MILLISECONDS);
		}

		@Override
		public T poll(long timeout, TimeUnit unit) throws InterruptedException {
			return queue.poll(timeout, unit);
		}

	}

}
