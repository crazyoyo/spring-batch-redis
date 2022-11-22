package com.redis.spring.batch;

import java.time.Duration;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.PlatformTransactionManager;

import com.redis.spring.batch.common.ConnectionPoolBuilder;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.common.StepOptions;
import com.redis.spring.batch.reader.DataGeneratorItemReader;
import com.redis.spring.batch.reader.DataGeneratorOptions;
import com.redis.spring.batch.reader.DataStructureValueReader;
import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.spring.batch.reader.LiveReaderBuilder;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.PollableItemReader;
import com.redis.spring.batch.reader.QueueOptions;
import com.redis.spring.batch.step.FlushingSimpleStepBuilder;
import com.redis.testcontainers.junit.AbstractTestcontainersRedisTestBase;
import com.redis.testcontainers.junit.RedisTestContext;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
public abstract class AbstractTestBase extends AbstractTestcontainersRedisTestBase {

	public static final long DEFAULT_IDLE_TIMEOUT = 500;

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

	/**
	 * 
	 * @param left
	 * @param right
	 * @return true if left and right have same keys
	 * @throws Exception
	 */
	protected boolean compare(String name, RedisTestContext left, RedisTestContext right) throws Exception {
		Long leftSize = left.sync().dbsize();
		Long rightSize = right.sync().dbsize();
		if (!leftSize.equals(rightSize)) {
			return false;
		}
		RedisItemReader<String, KeyComparison<String>> reader = comparisonReader(left, right);
		reader.open(new ExecutionContext());
		KeyComparison<String> comparison;
		while ((comparison = reader.read()) != null) {
			if (comparison.getStatus() != Status.OK) {
				return false;
			}
		}
		reader.close();
		return true;
	}

	protected RedisItemReader<String, KeyComparison<String>> comparisonReader(RedisTestContext left,
			RedisTestContext right) {
		RedisItemReader<String, KeyComparison<String>> reader = RedisItemReader
				.comparator(jobRunner, pool(left), pool(right), Duration.ofMillis(100)).build();
		reader.setName(name(left) + "-compare");
		return reader;
	}

	protected void awaitClosed(RedisItemReader<?, ?> reader) {
		Awaitility.await().until(() -> !reader.isOpen());
	}

	protected <T> JobExecution run(boolean async, String name, ItemReader<T> reader, ItemWriter<T> writer)
			throws JobExecutionException {
		Job job = jobRunner.job(name).start(step(name, reader, writer).build()).build();
		if (async) {
			return jobRunner.runAsync(job);
		}
		return jobRunner.run(job);
	}

	protected <T> SimpleStepBuilder<T, T> step(String name, ItemReader<T> reader, ItemWriter<T> writer) {
		if (reader instanceof ItemStreamSupport) {
			((ItemStreamSupport) reader).setName(name + "-reader");
		}
		return jobRunner.step(name).<T, T>chunk(StepOptions.DEFAULT_CHUNK_SIZE).reader(reader).writer(writer);
	}

	protected FlowBuilder<SimpleFlow> flow(String name) {
		return new FlowBuilder<>(name);
	}

	protected <T> JobExecution runFlushing(RedisTestContext redis, PollableItemReader<T> reader, ItemWriter<T> writer)
			throws JobExecutionException {
		return runFlushing(name(redis), reader, writer);
	}

	protected <T> JobExecution runFlushing(String name, PollableItemReader<T> reader, ItemWriter<T> writer)
			throws JobExecutionException {
		SimpleStepBuilder<T, T> stepBuilder = step(name, reader, writer);
		TaskletStep step = new FlushingSimpleStepBuilder<>(stepBuilder).idleTimeout(DEFAULT_IDLE_TIMEOUT).build();
		return jobRunner.runAsync(jobRunner.job(name).start(step).build());
	}

	protected String name(RedisTestContext redis) {
		return testInfo.getTestMethod().get().getName() + "-" + redis.hashCode();
	}

	protected void generate(RedisTestContext redis) throws JobExecutionException {
		DataGeneratorOptions options = DataGeneratorOptions.builder().build();
		DataGeneratorItemReader reader = new DataGeneratorItemReader(options);
		run(false, name(redis) + "-gen", reader, RedisItemWriter.dataStructure(pool(redis)).build());
	}

	protected static GenericObjectPool<StatefulConnection<String, String>> pool(RedisTestContext redis) {
		return connectionPool(redis, StringCodec.UTF8);
	}

	protected static GenericObjectPool<StatefulConnection<byte[], byte[]>> bytesPool(RedisTestContext redis) {
		return connectionPool(redis, ByteArrayCodec.INSTANCE);
	}

	private static <K, V> GenericObjectPool<StatefulConnection<K, V>> connectionPool(RedisTestContext redis,
			RedisCodec<K, V> codec) {
		return ConnectionPoolBuilder.create(redis.getClient()).build(codec);
	}

	protected RedisItemReader<String, DataStructure<String>> dataStructureReader(RedisTestContext redis)
			throws Exception {
		return RedisItemReader.dataStructure(pool(redis), jobRunner).build();
	}

	protected RedisItemReader<String, KeyDump<String>> keyDumpReader(RedisTestContext redis) throws Exception {
		return RedisItemReader.keyDump(pool(redis), jobRunner).build();
	}

	protected RedisItemWriter<String, String, KeyDump<String>> keyDumpWriter(RedisTestContext context) {
		return RedisItemWriter.keyDump(pool(context)).build();
	}

	protected DataStructureValueReader<String, String> dataStructureValueReader(RedisTestContext context) {
		return new DataStructureValueReader<>(pool(context));
	}

	protected LiveRedisItemReader<String, KeyDump<String>> liveKeyDumpReader(RedisTestContext redis,
			int notificationQueueCapacity) throws Exception {
		return configureLiveReader(RedisItemReader.liveKeyDump(pool(redis), jobRunner, redis.getPubSubConnection()),
				notificationQueueCapacity).build();
	}

	@SuppressWarnings("unchecked")
	private <B extends LiveReaderBuilder<?, ?, ?>> B configureLiveReader(B builder, int notificationQueueCapacity) {
		return (B) builder.notificationQueueOptions(QueueOptions.builder().capacity(notificationQueueCapacity).build())
				.stepOptions(
						StepOptions.builder().flushing().idleTimeout(Duration.ofMillis(DEFAULT_IDLE_TIMEOUT)).build());
	}

	protected LiveReaderBuilder<String, String, DataStructure<String>> liveDataStructureReader(RedisTestContext context,
			int notificationQueueCapacity) throws Exception {
		return configureLiveReader(
				RedisItemReader.liveDataStructure(pool(context), jobRunner, context.getPubSubConnection()),
				notificationQueueCapacity);
	}

	protected void flushAll(RedisTestContext context) {
		context.sync().flushall();
		Awaitility.await().until(() -> context.sync().dbsize() == 0);
	}
}
