package com.redis.spring.batch;

import java.time.Duration;
import java.util.logging.Logger;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.compare.KeyComparator;
import com.redis.spring.batch.compare.KeyComparator.RightComparatorBuilder;
import com.redis.spring.batch.compare.KeyComparisonLogger;
import com.redis.spring.batch.compare.KeyComparisonResults;
import com.redis.spring.batch.reader.DataStructureGeneratorItemReader;
import com.redis.spring.batch.reader.DataStructureValueReader;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.PollableItemReader;
import com.redis.spring.batch.step.FlushingSimpleStepBuilder;
import com.redis.spring.batch.support.ConnectionPoolItemStream;
import com.redis.spring.batch.support.JobRunner;
import com.redis.spring.batch.support.RedisConnectionBuilder;
import com.redis.testcontainers.junit.AbstractTestcontainersRedisTestBase;
import com.redis.testcontainers.junit.RedisTestContext;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.StringCodec;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
public abstract class AbstractTestBase extends AbstractTestcontainersRedisTestBase {

	private static final Logger log = Logger.getLogger(AbstractTestBase.class.getName());

	public static final Duration DEFAULT_IDLE_TIMEOUT = Duration.ofMillis(500);

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
	protected JobRunner jobRunner;

	private TestInfo testInfo;

	@BeforeEach
	void init(TestInfo testInfo) {
		this.testInfo = testInfo;
	}

	@BeforeEach
	private void createJobRunner() {
		this.jobRunner = new JobRunner(jobRepository, transactionManager);
	}

	private RightComparatorBuilder comparator(RedisTestContext context) {
		return KeyComparator.left(context.getClient());
	}

	protected KeyComparisonResults compare(RedisTestContext left, RedisTestContext right) throws Exception {
		Assertions.assertEquals(left.sync().dbsize(), right.sync().dbsize());
		KeyComparator comparator = comparator(left, right).build();
		comparator.addListener(new KeyComparisonLogger(log));
		return comparator.call();
	}

	protected KeyComparator.Builder comparator(RedisTestContext left, RedisTestContext right) {
		return comparator(left).right(right.getClient()).jobRunner(jobRunner);
	}

	protected void awaitClosed(ConnectionPoolItemStream<?, ?> itemStream) {
		Awaitility.await().until(() -> !itemStream.isOpen());
	}

	protected <T> JobExecution run(RedisTestContext redis, ItemReader<T> reader, ItemWriter<T> writer)
			throws JobExecutionException {
		return run(name(redis), reader, writer);
	}

	protected <T> JobExecution run(String id, ItemReader<T> reader, ItemWriter<T> writer) throws JobExecutionException {
		return jobRunner.run(id, RedisItemReader.DEFAULT_CHUNK_SIZE, reader, writer);
	}

	protected <T> JobExecution runFlushing(RedisTestContext redis, PollableItemReader<T> reader, ItemWriter<T> writer)
			throws JobExecutionException {
		return runFlushing(name(redis), reader, writer);
	}

	protected <T> JobExecution runFlushing(String name, PollableItemReader<T> reader, ItemWriter<T> writer)
			throws JobExecutionException {
		return jobRunner.runAsync(jobRunner.job(name,
				new FlushingSimpleStepBuilder<>(
						jobRunner.step(name, RedisItemReader.DEFAULT_CHUNK_SIZE, reader, writer))
						.idleTimeout(DEFAULT_IDLE_TIMEOUT)));
	}

	protected <T> JobExecution runAsync(RedisTestContext redis, ItemReader<T> reader, ItemWriter<T> writer)
			throws JobExecutionException {
		return runAsync(name(redis), redis, reader, writer);
	}

	private <T> JobExecution runAsync(String id, RedisTestContext redis, ItemReader<T> reader, ItemWriter<T> writer)
			throws JobExecutionException {
		return jobRunner.runAsync(id, RedisItemReader.DEFAULT_CHUNK_SIZE, reader, writer);
	}

	protected String name(RedisTestContext redis) {
		return testInfo.getTestMethod().get().getName() + "-" + ClassUtils.getShortName(redis.getServer().getClass());
	}

	protected void generate(RedisTestContext redis) throws JobExecutionException {
		run(name(redis) + "-gen", DataStructureGeneratorItemReader.builder().build(),
				RedisItemWriter.dataStructure(redis.getClient()).build());
	}

	protected RedisItemReader<String, DataStructure<String>> dataStructureReader(RedisTestContext redis)
			throws Exception {
		return RedisItemReader.dataStructure(redis.getClient()).jobRunner(jobRunner).build();
	}

	protected RedisItemReader<String, KeyValue<String, byte[]>> keyDumpReader(RedisTestContext redis) throws Exception {
		return RedisItemReader.keyDump(redis.getClient()).jobRunner(jobRunner).build();
	}

	protected RedisItemWriter<String, String, KeyValue<String, byte[]>> keyDumpWriter(RedisTestContext context) {
		return RedisItemWriter.keyDump(context.getClient()).build();
	}

	protected DataStructureValueReader<String, String> dataStructureValueReader(RedisTestContext context) {
		return dataStructureValueReader(context.getClient());
	}

	protected DataStructureValueReader<String, String> dataStructureValueReader(AbstractRedisClient client) {
		RedisConnectionBuilder<String, String, ?> builder = new RedisConnectionBuilder<>(client, StringCodec.UTF8);
		return new DataStructureValueReader<>(builder.connectionSupplier(), builder.getPoolConfig(), builder.async());
	}

	protected LiveRedisItemReader<String, KeyValue<String, byte[]>> liveKeyDumpReader(RedisTestContext redis,
			int notificationQueueCapacity) throws Exception {
		return configureLiveReader(RedisItemReader.keyDump(redis.getClient()).live(), notificationQueueCapacity)
				.build();
	}

	@SuppressWarnings("unchecked")
	private <K, V, T extends KeyValue<K, ?>, B extends LiveRedisItemReader.Builder<K, V, T>> B configureLiveReader(
			B builder, int notificationQueueCapacity) {
		return (B) builder.jobRunner(jobRunner).notificationQueueCapacity(notificationQueueCapacity)
				.idleTimeout(DEFAULT_IDLE_TIMEOUT);
	}

	protected LiveRedisItemReader<String, DataStructure<String>> liveDataStructureReader(RedisTestContext context,
			int notificationQueueCapacity) throws Exception {
		return configureLiveReader(RedisItemReader.dataStructure(context.getClient()).live(), notificationQueueCapacity)
				.build();
	}

	protected void flushAll(RedisTestContext context) {
		context.sync().flushall();
		Awaitility.await().until(() -> context.sync().dbsize() == 0);
	}
}
