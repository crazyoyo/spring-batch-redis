package com.redis.spring.batch;

import java.time.Duration;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

import com.redis.spring.batch.RedisItemReader.Builder;
import com.redis.spring.batch.RedisItemWriter.DataStructureWriterBuilder;
import com.redis.spring.batch.compare.KeyComparator;
import com.redis.spring.batch.compare.KeyComparator.KeyComparatorBuilder;
import com.redis.spring.batch.compare.KeyComparator.RightComparatorBuilder;
import com.redis.spring.batch.compare.KeyComparisonLogger;
import com.redis.spring.batch.compare.KeyComparisonResults;
import com.redis.spring.batch.reader.DataStructureValueReader;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.LiveRedisItemReaderBuilder;
import com.redis.spring.batch.reader.PollableItemReader;
import com.redis.spring.batch.reader.ScanRedisItemReaderBuilder;
import com.redis.spring.batch.step.FlushingSimpleStepBuilder;
import com.redis.spring.batch.support.ConnectionPoolItemStream;
import com.redis.spring.batch.support.JobRunner;
import com.redis.spring.batch.support.RandomDataStructureItemReader;
import com.redis.spring.batch.support.RedisConnectionBuilder;
import com.redis.spring.batch.writer.RedisOperation;
import com.redis.testcontainers.junit.AbstractTestcontainersRedisTestBase;
import com.redis.testcontainers.junit.RedisTestContext;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.StringCodec;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
public abstract class AbstractTestBase extends AbstractTestcontainersRedisTestBase {

	private static final Logger log = LoggerFactory.getLogger(AbstractTestBase.class);

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
	protected JobRunner jobRunner;

	@BeforeEach
	private void createAsyncJobLauncher() {
		SimpleJobLauncher launcher = new SimpleJobLauncher();
		launcher.setJobRepository(jobRepository);
		launcher.setTaskExecutor(new SimpleAsyncTaskExecutor());
		this.asyncJobLauncher = launcher;
		this.jobRunner = new JobRunner(jobRepository, transactionManager);
	}

	private RightComparatorBuilder comparator(RedisTestContext context) {
		return KeyComparator.left(context.getClient());
	}

	protected void compare(String name, RedisTestContext server, RedisTestContext target) throws Exception {
		Assertions.assertEquals(server.sync().dbsize(), target.sync().dbsize());
		KeyComparator comparator = comparator(name, server, target).build();
		comparator.addListener(new KeyComparisonLogger(log));
		KeyComparisonResults results = comparator.call();
		Assertions.assertTrue(results.isOK());
	}

	protected KeyComparatorBuilder comparator(String name, RedisTestContext left, RedisTestContext right) {
		return comparator(left).right(right.getClient()).jobRunner(jobRunner).id(name(left, name));
	}

	protected static String name(RedisTestContext context, String name) {
		return context.getRedisURI() + "-" + name;
	}

	protected <I, O> JobExecution launch(Job job) throws JobExecutionException {
		return JobRunner.awaitTermination(jobLauncher.run(job, new JobParameters()));
	}

	protected <I, O> JobExecution launchAsync(Job job) throws JobExecutionException {
		return JobRunner.awaitRunning(asyncJobLauncher.run(job, new JobParameters()));
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
		return launchAsync(job(redis, name, flushingStep(redis, name, reader, processor, writer).build()).build());
	}

	protected static void awaitClosed(ConnectionPoolItemStream<?, ?> itemStream) {
		Awaitility.await().until(() -> !itemStream.isOpen());
	}

	protected <I, O> FlushingSimpleStepBuilder<I, O> flushingStep(RedisTestContext redis, String name,
			PollableItemReader<? extends I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer)
			throws JobExecutionException {
		return new FlushingSimpleStepBuilder<>(step(redis, name, reader, processor, writer)).idleTimeout(IDLE_TIMEOUT);
	}

	protected JobExecution generate(String id, RedisTestContext redis) throws JobExecutionException {
		return generate(id, RandomDataStructureItemReader.builder().build(), redis);
	}

	protected JobExecution generate(String id, RandomDataStructureItemReader reader, RedisTestContext redis)
			throws JobExecutionException {
		return generate(id, reader, redis, DEFAULT_CHUNK_SIZE);
	}

	protected JobExecution generate(String id, RandomDataStructureItemReader reader, RedisTestContext redis,
			int chunkSize) throws JobExecutionException {
		String name = id + "-generator";
		reader.setName(name + "-reader");
		JobRunner jobRunner = new JobRunner(jobRepository, transactionManager);
		return jobRunner.run(name, chunkSize, reader, dataStructureWriter(redis).xaddArgs(m -> null).build());
	}

	protected static <T extends AbstractItemStreamItemReader<?>> T setName(T reader, RedisTestContext redis,
			String name) {
		reader.setName(name(redis, name + "-reader"));
		return reader;
	}

	protected RedisItemReader<String, DataStructure<String>> dataStructureReader(RedisTestContext redis, String name)
			throws Exception {
		ScanRedisItemReaderBuilder<String, String, DataStructure<String>> builder = new ScanRedisItemReaderBuilder<String, String, DataStructure<String>>(
				redis.getClient(), StringCodec.UTF8, DataStructureValueReader::new);
		builder.jobRunner(jobRunner);
		return setName(builder.build(), redis, name + "-data-structure");
	}

	protected RedisItemReader<String, KeyValue<String, byte[]>> keyDumpReader(RedisTestContext redis, String name)
			throws Exception {
		ScanRedisItemReaderBuilder<String, String, KeyValue<String, byte[]>> builder = reader(redis).keyDump();
		builder.jobRunner(jobRunner);
		return setName(builder.build(), redis, name + "-key-dump");
	}

	protected static RedisItemWriter<String, String, KeyValue<String, byte[]>> keyDumpWriter(RedisTestContext context) {
		return RedisItemWriter.client(context.getClient()).keyDump().build();
	}

	protected static DataStructureWriterBuilder<String, String> dataStructureWriter(RedisTestContext context) {
		return RedisItemWriter.client(context.getClient()).dataStructure();
	}

	protected static DataStructureValueReader<String, String> dataStructureValueReader(RedisTestContext context) {
		return dataStructureValueReader(context.getClient());
	}

	protected static DataStructureValueReader<String, String> dataStructureValueReader(AbstractRedisClient client) {
		RedisConnectionBuilder<String, String, ?> builder = new RedisConnectionBuilder<>(client, StringCodec.UTF8);
		return new DataStructureValueReader<>(builder.connectionSupplier(), builder.getPoolConfig(), builder.async());
	}

	protected static <T> RedisItemWriter.Builder<String, String, T> operationWriter(RedisTestContext context,
			RedisOperation<String, String, T> operation) {
		return RedisItemWriter.client(context.getClient()).operation(operation);
	}

	protected static Builder<String, String> reader(RedisTestContext context) {
		return RedisItemReader.client(context.getClient());
	}

	protected LiveRedisItemReader<String, KeyValue<String, byte[]>> liveKeyDumpReader(RedisTestContext redis,
			String name, int notificationQueueCapacity) throws Exception {
		return setName(configureLiveReader(reader(redis).keyDump().live(), notificationQueueCapacity).build(), redis,
				name + "-live-key-dump");
	}

	@SuppressWarnings("unchecked")
	private <B extends LiveRedisItemReaderBuilder<String, String, ?>> B configureLiveReader(B builder,
			int notificationQueueCapacity) {
		return (B) builder.jobRunner(jobRunner).idleTimeout(IDLE_TIMEOUT)
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
