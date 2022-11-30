package com.redis.spring.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.PlatformTransactionManager;

import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.DataStructure.Type;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.StepOptions;
import com.redis.spring.batch.reader.DataGeneratorItemReader;
import com.redis.spring.batch.reader.DataGeneratorOptions;
import com.redis.spring.batch.reader.QueueItemReader;
import com.redis.spring.batch.step.FlushingChunkProvider;
import com.redis.spring.batch.step.FlushingSimpleStepBuilder;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
class StepTests {

	@Autowired
	protected JobRepository jobRepository;
	@Autowired
	protected PlatformTransactionManager transactionManager;
	@Autowired
	protected JobBuilderFactory jobBuilderFactory;
	@Autowired
	protected StepBuilderFactory stepBuilderFactory;
	@SuppressWarnings("unused")
	@Autowired
	private JobLauncher jobLauncher;
	private JobRunner jobRunner;

	@BeforeEach
	private void createJobRunner() {
		this.jobRunner = new JobRunner(jobRepository, transactionManager);
	}

	@SuppressWarnings("unchecked")
	@Test
	void readKeyValueFaultTolerance() throws Exception {
		int count = 100;
		DataGeneratorItemReader generator = new DataGeneratorItemReader(
				DataGeneratorOptions.builder().types(Type.STRING).build());
		generator.setMaxItemCount(count);
		ErrorItemReader<DataStructure<String>> reader = new ErrorItemReader<>(generator);
		ListItemWriter<DataStructure<String>> writer = new ListItemWriter<>();
		jobRunner.run("readKeyValueFaultTolerance", reader, null, writer, StepOptions.builder().faultTolerant(true)
				.skip(TimeoutException.class).skipPolicy(new AlwaysSkipItemSkipPolicy()).chunkSize(1).build());
		assertEquals(count * ErrorItemReader.DEFAULT_ERROR_RATE, writer.getWrittenItems().size());
	}

	@Test
	void readerSkipPolicy() throws Exception {
		String name = "skip-policy";
		List<Integer> items = IntStream.range(0, 100).boxed().collect(Collectors.toList());
		ErrorItemReader<Integer> reader = new ErrorItemReader<>(new ListItemReader<>(items));
		ListItemWriter<Integer> writer = new ListItemWriter<>();
		FlushingSimpleStepBuilder<Integer, Integer> stepBuilder = new FlushingSimpleStepBuilder<>(
				stepBuilderFactory.get(name).<Integer, Integer>chunk(1).reader(reader).writer(writer));
		stepBuilder.idleTimeout(100).skip(TimeoutException.class).skipPolicy(new AlwaysSkipItemSkipPolicy());
		jobRunner.run(jobBuilderFactory.get(name).start(stepBuilder.build()).build());
		assertEquals(items.size(), writer.getWrittenItems().size() * 2);
	}

	@Test
	void flushingStep() throws Exception {
		String name = "flushingStep";
		int count = 100;
		BlockingQueue<String> queue = new LinkedBlockingDeque<>(count);
		QueueItemReader<String> reader = new QueueItemReader<>(queue, Duration.ofMillis(10));
		ListItemWriter<String> writer = new ListItemWriter<>();
		JobExecution execution = jobRunner.runAsync(name, reader, null, writer,
				StepOptions.builder().flushingInterval(FlushingChunkProvider.DEFAULT_FLUSHING_INTERVAL)
						.idleTimeout(Duration.ofMillis(500)).build());
		Awaitility.await().until(() -> reader.isOpen());
		for (int index = 1; index <= count; index++) {
			queue.offer("key" + index);
		}
		jobRunner.awaitTermination(execution);
		Awaitility.await().until(() -> !reader.isOpen());
		assertEquals(count, writer.getWrittenItems().size());
	}
}
