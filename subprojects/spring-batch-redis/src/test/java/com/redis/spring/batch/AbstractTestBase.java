package com.redis.spring.batch;

import java.time.Duration;
import java.util.UUID;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

import com.redis.spring.batch.RedisItemReader.TypeBuilder;
import com.redis.spring.batch.RedisItemWriter.CodecBuilder;
import com.redis.spring.batch.compare.KeyComparator;
import com.redis.spring.batch.compare.KeyComparator.RightComparatorBuilder;
import com.redis.spring.batch.compare.KeyComparisonLogger;
import com.redis.spring.batch.compare.KeyComparisonResults;
import com.redis.spring.batch.reader.DataStructureValueReader;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.PollableItemReader;
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

	@BeforeEach
	private void createJobRunner() {
		this.jobRunner = new JobRunner(jobRepository, transactionManager);
	}

	private RightComparatorBuilder comparator(RedisTestContext context) {
		return KeyComparator.left(context.getClient());
	}

	protected void compare(RedisTestContext server, RedisTestContext target) throws Exception {
		Assertions.assertEquals(server.sync().dbsize(), target.sync().dbsize());
		KeyComparator comparator = comparator(server, target).build();
		comparator.addListener(new KeyComparisonLogger(log));
		KeyComparisonResults results = comparator.call();
		Assertions.assertTrue(results.isOK());
	}

	protected KeyComparator.Builder comparator(RedisTestContext left, RedisTestContext right) {
		return comparator(left).right(right.getClient()).jobRunner(jobRunner);
	}

	protected String name(RedisTestContext context, String name) {
		return context.getRedisURI() + "-" + name;
	}

	protected void awaitClosed(ConnectionPoolItemStream<?, ?> itemStream) {
		Awaitility.await().until(() -> !itemStream.isOpen());
	}

	protected <I, O> JobExecution run(ItemReader<I> reader, ItemWriter<O> writer) throws JobExecutionException {
		return jobRunner.run(UUID.randomUUID().toString(), RedisItemReader.DEFAULT_CHUNK_SIZE, reader, writer);
	}

	protected <I, O> JobExecution runFlushing(PollableItemReader<I> reader, ItemWriter<O> writer)
			throws JobExecutionException {
		String name = UUID.randomUUID().toString();
		return jobRunner.runAsync(jobRunner.job(name,
				new FlushingSimpleStepBuilder<>(
						jobRunner.step(name, RedisItemReader.DEFAULT_CHUNK_SIZE, reader, writer))
						.idleTimeout(DEFAULT_IDLE_TIMEOUT)));
	}

	protected <I, O> JobExecution runAsync(ItemReader<I> reader, ItemWriter<O> writer) throws JobExecutionException {
		return runAsync(UUID.randomUUID().toString(), reader, writer);
	}

	protected <I, O> JobExecution runAsync(String name, ItemReader<I> reader, ItemWriter<O> writer)
			throws JobExecutionException {
		return jobRunner.runAsync(name, RedisItemReader.DEFAULT_CHUNK_SIZE, reader, writer);
	}

	protected void generate(RedisTestContext server) throws JobExecutionException {
		run(RandomDataStructureItemReader.builder().build(), dataStructureWriter(server));
	}

	protected RedisItemReader<String, DataStructure<String>> dataStructureReader(RedisTestContext redis)
			throws Exception {
		return reader(redis).dataStructure().jobRunner(jobRunner).build();
	}

	protected RedisItemReader<String, KeyValue<String, byte[]>> keyDumpReader(RedisTestContext redis) throws Exception {
		return reader(redis).keyDump().jobRunner(jobRunner).build();
	}

	protected RedisItemWriter<String, String, KeyValue<String, byte[]>> keyDumpWriter(RedisTestContext context) {
		return RedisItemWriter.client(context.getClient()).string().keyDump().build();
	}

	protected RedisItemWriter<String, String, DataStructure<String>> dataStructureWriter(RedisTestContext context) {
		return RedisItemWriter.client(context.getClient()).string().dataStructure().build();
	}

	protected DataStructureValueReader<String, String> dataStructureValueReader(RedisTestContext context) {
		return dataStructureValueReader(context.getClient());
	}

	protected CodecBuilder writer(RedisTestContext redis) {
		return RedisItemWriter.client(redis.getClient());
	}

	protected DataStructureValueReader<String, String> dataStructureValueReader(AbstractRedisClient client) {
		RedisConnectionBuilder<String, String, ?> builder = new RedisConnectionBuilder<>(client, StringCodec.UTF8);
		return new DataStructureValueReader<>(builder.connectionSupplier(), builder.getPoolConfig(), builder.async());
	}

	protected <T> RedisItemWriter.AbstractBuilder<String, String, T, ?> operationWriter(RedisTestContext context,
			RedisOperation<String, String, T> operation) {
		return writer(context).string().operation(operation);
	}

	protected TypeBuilder<String, String> reader(RedisTestContext context) {
		return RedisItemReader.client(context.getClient()).string();
	}

	protected LiveRedisItemReader<String, KeyValue<String, byte[]>> liveKeyDumpReader(RedisTestContext redis,
			int notificationQueueCapacity) throws Exception {
		return configureLiveReader(reader(redis).keyDump().live(), notificationQueueCapacity).build();
	}

	@SuppressWarnings("unchecked")
	private <K, V, T extends KeyValue<K, ?>, B extends LiveRedisItemReader.Builder<K, V, T>> B configureLiveReader(
			B builder, int notificationQueueCapacity) {
		return (B) builder.jobRunner(jobRunner).notificationQueueCapacity(notificationQueueCapacity)
				.idleTimeout(DEFAULT_IDLE_TIMEOUT);
	}

	protected LiveRedisItemReader<String, DataStructure<String>> liveDataStructureReader(RedisTestContext context,
			int notificationQueueCapacity) throws Exception {
		return configureLiveReader(reader(context).dataStructure().live(), notificationQueueCapacity).build();
	}

	protected void flushAll(RedisTestContext context) {
		context.sync().flushall();
		Awaitility.await().until(() -> context.sync().dbsize() == 0);
	}
}
