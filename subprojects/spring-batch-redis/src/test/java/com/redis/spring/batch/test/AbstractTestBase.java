package com.redis.spring.batch.test;

import java.time.Duration;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.PlatformTransactionManager;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.ItemReaderBuilder;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.RedisItemWriter.OperationItemWriterBuilder;
import com.redis.spring.batch.RedisItemWriter.RedisItemWriterBuilder;
import com.redis.spring.batch.builder.JobRepositoryBuilder;
import com.redis.spring.batch.builder.LiveRedisItemReaderBuilder;
import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.DataStructureValueReader;
import com.redis.spring.batch.support.FlushingStepBuilder;
import com.redis.spring.batch.support.KeyValue;
import com.redis.spring.batch.support.LiveRedisItemReader;
import com.redis.spring.batch.support.PollableItemReader;
import com.redis.spring.batch.support.RedisOperation;
import com.redis.spring.batch.support.generator.Generator;
import com.redis.spring.batch.support.generator.Generator.ClientGeneratorBuilder;
import com.redis.spring.batch.support.generator.Generator.GeneratorBuilder;
import com.redis.testcontainers.junit.jupiter.AbstractTestcontainersRedisTestBase;
import com.redis.testcontainers.junit.jupiter.RedisTestContext;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
public abstract class AbstractTestBase extends AbstractTestcontainersRedisTestBase {

	private static final int DEFAULT_CHUNK_SIZE = 50;
	protected static final Duration IDLE_TIMEOUT = Duration.ofSeconds(1);

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
	private JobLauncher asyncJobLauncher;

	@BeforeEach
	private void createAsyncJobLauncher() {
		SimpleJobLauncher launcher = new SimpleJobLauncher();
		launcher.setJobRepository(jobRepository);
		launcher.setTaskExecutor(new SimpleAsyncTaskExecutor());
		this.asyncJobLauncher = launcher;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected <B extends JobRepositoryBuilder> B configureJobRepository(B runner) {
		return (B) runner.jobRepository(jobRepository).transactionManager(transactionManager);
	}

	protected static String name(RedisTestContext context, String name) {
		return context.getRedisURI() + "-" + name;
	}

	protected <I, O> JobExecution launch(Job job) throws JobExecutionException {
		return awaitTermination(jobLauncher.run(job, new JobParameters()));
	}

	protected <I, O> JobExecution launchAsync(Job job) throws JobExecutionException {
		JobExecution execution = asyncJobLauncher.run(job, new JobParameters());
		awaitRunning(execution);
		return execution;
	}

	protected static JobExecution awaitRunning(JobExecution execution) {
		Awaitility.await().until(execution::isRunning);
		return execution;
	}

	protected static JobExecution awaitTermination(JobExecution execution) {
		Awaitility.await().timeout(Duration.ofMinutes(1)).until(() -> !execution.isRunning());
		return execution;
	}

	protected static void awaitTermination(JobExecution execution, PollableItemReader<?> reader) {
		awaitTermination(execution);
		Awaitility.await().until(() -> !reader.isOpen());
	}

	protected static FlowBuilder<SimpleFlow> flow(RedisTestContext context, String name) {
		return new FlowBuilder<SimpleFlow>(name(context, name));
	}

	protected <I, O> SimpleStepBuilder<I, O> step(RedisTestContext redis, String name, ItemReader<? extends I> reader,
			ItemProcessor<I, O> processor, ItemWriter<O> writer) throws JobExecutionException {
		return stepBuilderFactory.get(name(redis, name)).<I, O>chunk(DEFAULT_CHUNK_SIZE).reader(reader)
				.processor(processor).writer(writer);
	}

	protected SimpleJobBuilder job(RedisTestContext redis, String name, Step step) {
		return job(redis, name).start(step);
	}

	protected JobBuilder job(RedisTestContext redis, String name) {
		return jobBuilderFactory.get(name(redis, name));
	}

	protected <I, O> JobExecution run(RedisTestContext redis, String name, ItemReader<? extends I> reader,
			ItemProcessor<I, O> processor, ItemWriter<O> writer) throws JobExecutionException {
		return launch(job(redis, name, step(redis, name, reader, processor, writer).build()).build());
	}

	protected <T> JobExecution run(RedisTestContext redis, String name, ItemReader<? extends T> reader,
			ItemWriter<T> writer) throws JobExecutionException {
		return run(redis, name, reader, null, writer);
	}

	protected <I, O> JobExecution runFlushing(RedisTestContext redis, String name,
			PollableItemReader<? extends I> reader, ItemWriter<O> writer) throws JobExecutionException {
		return runFlushing(redis, name, reader, null, writer);
	}

	protected <I, O> JobExecution runFlushing(RedisTestContext redis, String name,
			PollableItemReader<? extends I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer)
			throws JobExecutionException {
		JobExecution execution = launchAsync(
				job(redis, name, flushingStep(redis, name, reader, processor, writer).build()).build());
		awaitOpen(reader);
		return execution;
	}

	protected static void awaitOpen(PollableItemReader<?> reader) {
		Awaitility.await().until(reader::isOpen);
	}

	protected <I, O> FlushingStepBuilder<I, O> flushingStep(RedisTestContext redis, String name,
			PollableItemReader<? extends I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer)
			throws JobExecutionException {
		return new FlushingStepBuilder<>(step(redis, name, reader, processor, writer)).idleTimeout(IDLE_TIMEOUT);
	}

	protected static void execute(GeneratorBuilder generator) throws Exception {
		awaitTermination(generator.build().call());
	}

	protected static OperationItemWriterBuilder<String, String> writer(RedisTestContext context) {
		if (context.isCluster()) {
			return RedisItemWriter.client(context.getRedisClusterClient());
		}
		return RedisItemWriter.client(context.getRedisClient());
	}

	private static <T extends AbstractItemStreamItemReader<?>> T setName(T reader, RedisTestContext redis,
			String name) {
		reader.setName(name(redis, name + "-reader"));
		return reader;
	}

	protected RedisItemReader<String, DataStructure<String>> dataStructureReader(RedisTestContext redis, String name)
			throws Exception {
		return setName(configureJobRepository(reader(redis).dataStructure()).build(), redis, name + "-data-structure");
	}

	protected RedisItemReader<String, KeyValue<String, byte[]>> keyDumpReader(RedisTestContext redis, String name)
			throws Exception {
		return setName(configureJobRepository(reader(redis).keyDump()).build(), redis, name + "-key-dump");
	}

	protected static RedisItemWriter<String, String, KeyValue<String, byte[]>> keyDumpWriter(RedisTestContext context) {
		return writer(context).keyDump().build();
	}

	protected static RedisItemWriter<String, String, DataStructure<String>> dataStructureWriter(
			RedisTestContext context) {
		return writer(context).dataStructure().build();
	}

	protected static DataStructureValueReader<String, String> dataStructureValueReader(RedisTestContext context) {
		if (context.isCluster()) {
			return DataStructureValueReader.client(context.getRedisClusterClient()).build();
		}
		return DataStructureValueReader.client(context.getRedisClient()).build();
	}

	protected static DataStructureValueReader<String, String> dataStructureValueReader(AbstractRedisClient client) {
		if (client instanceof RedisClusterClient) {
			return DataStructureValueReader.client((RedisClusterClient) client).build();
		}
		return DataStructureValueReader.client((RedisClient) client).build();
	}

	protected static <T> RedisItemWriterBuilder<String, String, T> operationWriter(RedisTestContext context,
			RedisOperation<String, String, T> operation) {
		return writer(context).operation(operation);
	}

	protected static ItemReaderBuilder reader(RedisTestContext context) {
		if (context.isCluster()) {
			return RedisItemReader.client(context.getRedisClusterClient());
		}
		return RedisItemReader.client(context.getRedisClient());
	}

	protected GeneratorBuilder dataGenerator(RedisTestContext context, String name) {
		return configureJobRepository(dataGenerator(context.getClient()).id(name(context, name + "-generator")));
	}

	protected ClientGeneratorBuilder dataGenerator(AbstractRedisClient client) {
		if (client instanceof RedisClusterClient) {
			return Generator.client((RedisClusterClient) client);
		}
		return Generator.client((RedisClient) client);
	}

	protected LiveRedisItemReader<String, KeyValue<String, byte[]>> liveKeyDumpReader(RedisTestContext redis,
			String name, int notificationQueueCapacity) throws Exception {
		return setName(configureLiveReader(reader(redis).keyDump().live(), notificationQueueCapacity).build(), redis,
				name + "-live-key-dump");
	}

	@SuppressWarnings("unchecked")
	private <B extends LiveRedisItemReaderBuilder<?, ?>> B configureLiveReader(B builder,
			int notificationQueueCapacity) {
		return (B) configureJobRepository(builder).idleTimeout(IDLE_TIMEOUT)
				.notificationQueueCapacity(notificationQueueCapacity);
	}

	protected LiveRedisItemReader<String, DataStructure<String>> liveDataStructureReader(RedisTestContext context,
			String name, int notificationQueueCapacity) throws Exception {
		return setName(configureLiveReader(reader(context).dataStructure().live(), notificationQueueCapacity).build(),
				context, name + "-live-data-structure");
	}

	protected void flushAll(RedisTestContext context) {
		context.sync().flushall();
		Awaitility.await().until(() -> context.sync().dbsize() == 0);
	}
}
