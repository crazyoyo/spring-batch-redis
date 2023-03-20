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
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.PlatformTransactionManager;

import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.DataStructure.Type;
import com.redis.spring.batch.common.FaultToleranceOptions;
import com.redis.spring.batch.common.FlushingOptions;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.reader.GeneratorItemReader;
import com.redis.spring.batch.reader.GeneratorReaderOptions;
import com.redis.spring.batch.reader.QueueItemReader;
import com.redis.spring.batch.reader.ReaderOptions;
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
		GeneratorItemReader generator = new GeneratorItemReader(
				GeneratorReaderOptions.builder().types(Type.STRING).build());
		generator.setMaxItemCount(count);
		ErrorItemReader<DataStructure<String>> reader = new ErrorItemReader<>(generator);

		ListItemWriter<DataStructure<String>> writer = new ListItemWriter<>();
		String name = "readKeyValueFaultTolerance";
		SimpleStepBuilder<DataStructure<String>, DataStructure<String>> step = jobRunner.step(name).chunk(1);
		step.reader(reader).writer(writer);
		FaultTolerantStepBuilder<DataStructure<String>, DataStructure<String>> ftStep = JobRunner.faultTolerant(step,
				FaultToleranceOptions.builder().skip(TimeoutException.class).skipPolicy(new AlwaysSkipItemSkipPolicy())
						.build());
		Job job = jobRunner.job(name).start(ftStep.build()).build();
		jobRunner.getJobLauncher().run(job, new JobParameters());
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
		stepBuilder.idleTimeout(Duration.ofMillis(100)).skip(TimeoutException.class)
				.skipPolicy(new AlwaysSkipItemSkipPolicy());
		Job job = jobBuilderFactory.get(name).start(stepBuilder.build()).build();
		jobRunner.getJobLauncher().run(job, new JobParameters());
		assertEquals(items.size(), writer.getWrittenItems().size() * 2);
	}

	@Test
	void flushingStep() throws Exception {
		String name = "flushingStep";
		int count = 100;
		BlockingQueue<String> queue = new LinkedBlockingDeque<>(count);
		QueueItemReader<String> reader = new QueueItemReader<>(queue, Duration.ofMillis(10));
		ListItemWriter<String> writer = new ListItemWriter<>();
		SimpleStepBuilder<String, String> step = jobRunner.step(name).chunk(ReaderOptions.DEFAULT_CHUNK_SIZE);
		step.reader(reader).writer(writer);
		FlushingSimpleStepBuilder<String, String> flushingStep = JobRunner.flushing(step,
				FlushingOptions.builder().flushingInterval(FlushingChunkProvider.DEFAULT_FLUSHING_INTERVAL)
						.idleTimeout(Duration.ofMillis(500)).build());
		Job job = jobRunner.job(name).start(flushingStep.build()).build();
		JobExecution execution = jobRunner.getAsyncJobLauncher().run(job, new JobParameters());
		Awaitility.await().until(() -> reader.isOpen());
		for (int index = 1; index <= count; index++) {
			queue.offer("key" + index);
		}
		Awaitility.await().until(() -> JobRunner.isTerminated(execution));
		assertEquals(count, writer.getWrittenItems().size());
	}
}
