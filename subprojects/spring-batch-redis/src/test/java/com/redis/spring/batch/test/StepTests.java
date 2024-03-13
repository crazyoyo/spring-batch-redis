package com.redis.spring.batch.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.awaitility.Awaitility;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.boot.autoconfigure.batch.BatchDataSourceScriptDatabaseInitializer;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.boot.sql.init.DatabaseInitializationMode;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.PlatformTransactionManager;

import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.reader.AbstractPollableItemReader;
import com.redis.spring.batch.step.FlushingFaultTolerantStepBuilder;
import com.redis.spring.batch.step.FlushingStepBuilder;

import io.lettuce.core.RedisCommandTimeoutException;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
@TestInstance(Lifecycle.PER_CLASS)
class StepTests {

	protected JobRepository jobRepository;
	protected PlatformTransactionManager transactionManager;

	private TaskExecutorJobLauncher jobLauncher;
	private TaskExecutorJobLauncher asyncJobLauncher;

	private DataSource dataSource() throws Exception {
		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setURL("jdbc:hsqldb:mem:steptests");
		BatchProperties.Jdbc jdbc = new BatchProperties.Jdbc();
		jdbc.setInitializeSchema(DatabaseInitializationMode.ALWAYS);
		BatchDataSourceScriptDatabaseInitializer initializer = new BatchDataSourceScriptDatabaseInitializer(dataSource,
				jdbc);
		initializer.afterPropertiesSet();
		initializer.initializeDatabase();
		return dataSource;
	}

	@BeforeAll
	void initialize() throws Exception {
		transactionManager = new ResourcelessTransactionManager();
		JobRepositoryFactoryBean bean = new JobRepositoryFactoryBean();
		bean.setDataSource(dataSource());
		bean.setTransactionManager(transactionManager);
		bean.afterPropertiesSet();
		if (jobRepository == null) {
			try {
				jobRepository = bean.getObject();
			} catch (Exception e) {
				throw new ItemStreamException("Could not initialize job repository");
			}
			if (jobRepository == null) {
				throw new ItemStreamException("Job repository is null");
			}
		}
		jobLauncher = new TaskExecutorJobLauncher();
		jobLauncher.setJobRepository(jobRepository);
		jobLauncher.setTaskExecutor(new SyncTaskExecutor());
		asyncJobLauncher = new TaskExecutorJobLauncher();
		asyncJobLauncher.setJobRepository(jobRepository);
		asyncJobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
	}

	@Test
	void flushingFaultTolerantStep() throws Exception {
		int count = 100;
		GeneratorItemReader gen = new GeneratorItemReader();
		gen.setMaxItemCount(count);
		gen.setTypes(DataType.STRING);
		ErrorItemReader<KeyValue<String>> reader = new ErrorItemReader<>(gen);
		ListItemWriter<KeyValue<String>> writer = new ListItemWriter<>();
		String name = "readKeyValueFaultTolerance";
		FlushingStepBuilder<KeyValue<String>, KeyValue<String>> step = new FlushingStepBuilder<>(
				new StepBuilder(name, jobRepository));
		step.transactionManager(transactionManager);
		step.chunk(1);
		step.reader(reader);
		step.writer(writer);
		step.idleTimeout(Duration.ofMillis(300));
		FlushingFaultTolerantStepBuilder<KeyValue<String>, KeyValue<String>> ftStep = step.faultTolerant();
		ftStep.skip(RedisCommandTimeoutException.class);
		ftStep.skipPolicy(new AlwaysSkipItemSkipPolicy());
		Job job = new JobBuilder(name, jobRepository).start(ftStep.build()).build();
		jobLauncher.run(job, new JobParameters());
		assertEquals(count * ErrorItemReader.DEFAULT_ERROR_RATE, writer.getWrittenItems().size());
	}

	@Test
	void readerSkipPolicy() throws Exception {
		String name = "skip-policy";
		List<Integer> items = IntStream.range(0, 100).boxed().collect(Collectors.toList());
		ErrorItemReader<Integer> reader = new ErrorItemReader<>(new ListItemReader<>(items));
		ListItemWriter<Integer> writer = new ListItemWriter<>();
		SimpleStepBuilder<Integer, Integer> step = new StepBuilder(name, jobRepository).chunk(1, transactionManager);
		step.transactionManager(transactionManager);
		step.reader(reader);
		step.writer(writer);
		FlushingFaultTolerantStepBuilder<Integer, Integer> ftStep = new FlushingFaultTolerantStepBuilder<>(step);
		ftStep.idleTimeout(Duration.ofMillis(300));
		ftStep.skip(RedisCommandTimeoutException.class);
		ftStep.skipPolicy(new AlwaysSkipItemSkipPolicy());
		Job job = new JobBuilder(name, jobRepository).start(ftStep.build()).build();
		jobLauncher.run(job, new JobParameters());
		assertEquals(items.size(), writer.getWrittenItems().size() * 2);
	}

	@Test
	void flushingStep() throws Exception {
		String name = "flushingStep";
		int count = 100;
		BlockingQueue<String> queue = new LinkedBlockingDeque<>(count);
		QueueItemReader<String> reader = new QueueItemReader<>(queue);
		ListItemWriter<String> writer = new ListItemWriter<>();
		FlushingStepBuilder<String, String> step = new FlushingStepBuilder<>(
				new StepBuilder(name, jobRepository).chunk(50, transactionManager));
		step.transactionManager(transactionManager);
		step.reader(reader);
		step.writer(writer);
		step.idleTimeout(Duration.ofMillis(500));
		Job job = new JobBuilder(name, jobRepository).start(step.build()).build();
		JobExecution execution = asyncJobLauncher.run(job, new JobParameters());
		for (int index = 1; index <= count; index++) {
			queue.offer("key" + index);
		}
		Awaitility.await().until(() -> !execution.isRunning());
		assertEquals(count, writer.getWrittenItems().size());
	}

	private static class QueueItemReader<T> extends AbstractPollableItemReader<T> {

		private final BlockingQueue<T> queue;

		public QueueItemReader(BlockingQueue<T> queue) {
			this.queue = queue;
		}

		@Override
		protected T doPoll(long timeout, TimeUnit unit) throws InterruptedException {
			return queue.poll(timeout, unit);
		}

	}

}
