package com.redis.spring.batch;

import java.time.Duration;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
import com.redis.spring.batch.reader.QueueOptions;
import com.redis.spring.batch.reader.ScanReaderBuilder;
import com.redis.spring.batch.writer.WriterBuilder;
import com.redis.testcontainers.junit.AbstractTestcontainersRedisTestBase;
import com.redis.testcontainers.junit.RedisTestContext;

import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
public abstract class AbstractTestBase extends AbstractTestcontainersRedisTestBase {

	public static final Duration DEFAULT_IDLE_TIMEOUT = Duration.ofMillis(500);

	protected static final StepOptions DEFAULT_FLUSHING_STEP_OPTIONS = StepOptions.builder()
			.flushingInterval(LiveReaderBuilder.DEFAULT_FLUSHING_INTERVAL).idleTimeout(DEFAULT_IDLE_TIMEOUT).build();

	protected static final StepOptions DEFAULT_STEP_OPTIONS = StepOptions.builder().build();

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

	@Value("${running-timeout:PT5S}")
	private Duration runningTimeout;
	@Value("${termination-timeout:PT5S}")
	private Duration terminationTimeout;

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

	protected void awaitOpen(RedisItemReader<?, ?> reader) {
		Awaitility.await().until(reader::isOpen);
	}

	protected void awaitClosed(RedisItemReader<?, ?> reader) {
		Awaitility.await().until(() -> !reader.isOpen());
	}

	protected void awaitClosed(RedisItemWriter<?, ?, ?> writer) {
		Awaitility.await().until(() -> !writer.isOpen());
	}

	protected void awaitTermination(JobExecution jobExecution) {
		Awaitility.await().timeout(terminationTimeout).until(() -> JobRunner.isTerminated(jobExecution));
	}

	protected <I, O> JobExecution runAsync(RedisTestContext redis, ItemReader<I> reader, ItemProcessor<I, O> processor,
			ItemWriter<O> writer) throws JobExecutionException {
		return runAsync(name(redis), reader, processor, writer);
	}

	protected <I, O> JobExecution runAsync(String name, ItemReader<I> reader, ItemProcessor<I, O> processor,
			ItemWriter<O> writer) throws JobExecutionException {
		return runAsync(name, reader, processor, writer, DEFAULT_FLUSHING_STEP_OPTIONS);
	}

	protected <I, O> JobExecution runAsync(String name, ItemReader<I> reader, ItemWriter<O> writer, StepOptions options)
			throws JobExecutionException {
		return runAsync(name, reader, null, writer, options);
	}

	protected <I, O> JobExecution runAsync(String name, ItemReader<I> reader, ItemProcessor<I, O> processor,
			ItemWriter<O> writer, StepOptions stepOptions) throws JobExecutionException {
		setName(name, reader, writer);
		SimpleStepBuilder<I, O> step = jobRunner.step(name, reader, processor, writer, stepOptions);
		Job job = jobRunner.job(name).start(step.build()).build();
		return jobRunner.getAsyncJobLauncher().run(job, new JobParameters());
	}

	protected <I, O> JobExecution runAsync(RedisTestContext redis, ItemReader<I> reader, ItemProcessor<I, O> processor,
			ItemWriter<O> writer, StepOptions stepOptions) throws JobExecutionException {
		return runAsync(name(redis), reader, processor, writer, stepOptions);
	}

	protected <T> JobExecution runAsync(RedisTestContext redis, ItemReader<T> reader, ItemWriter<T> writer)
			throws JobExecutionException {
		return runAsync(redis, reader, null, writer);
	}

	protected <T> JobExecution runAsync(String name, ItemReader<T> reader, ItemWriter<T> writer)
			throws JobExecutionException {
		return runAsync(name, reader, null, writer);
	}

	protected <I, O> void run(String name, ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer,
			StepOptions stepOptions) throws JobExecutionException {
		setName(name, reader, writer);
		SimpleStepBuilder<I, O> step = jobRunner.step(name, reader, processor, writer, stepOptions);
		Job job = jobRunner.job(name).start(step.build()).build();
		jobRunner.getJobLauncher().run(job, new JobParameters());
	}

	protected <T> void run(String name, ItemReader<T> reader, ItemWriter<T> writer) throws JobExecutionException {
		run(name, reader, null, writer);
	}

	protected <I, O> void run(String name, ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer)
			throws JobExecutionException {
		run(name, reader, processor, writer, DEFAULT_STEP_OPTIONS);
	}

	protected <I, O> void run(RedisTestContext redis, ItemReader<I> reader, ItemProcessor<I, O> processor,
			ItemWriter<O> writer) throws JobExecutionException {
		run(name(redis), reader, processor, writer);
	}

	protected <I, O> void run(RedisTestContext redis, ItemReader<I> reader, ItemProcessor<I, O> processor,
			ItemWriter<O> writer, StepOptions stepOptions) throws JobExecutionException {
		run(name(redis), reader, processor, writer, stepOptions);
	}

	protected <T> void run(RedisTestContext redis, ItemReader<T> reader, ItemWriter<T> writer)
			throws JobExecutionException {
		run(redis, reader, null, writer);
	}

	protected void setName(String name, ItemReader<?> reader, ItemWriter<?> writer) {
		if (reader instanceof ItemStreamSupport) {
			((ItemStreamSupport) reader).setName(name + "-reader");
		}
		if (writer instanceof ItemStreamSupport) {
			((ItemStreamSupport) writer).setName(name + "-writer");
		}
	}

	/**
	 * 
	 * @param left
	 * @param right
	 * @return true if left and right have same keys
	 * @throws Exception
	 */
	protected void compare(RedisTestContext left, RedisTestContext right) throws Exception {
		Assertions.assertEquals(left.sync().dbsize(), right.sync().dbsize());
		RedisItemReader<String, KeyComparison<String>> reader = comparisonReader(left, right);
		ListItemWriter<KeyComparison<String>> writer = new ListItemWriter<>();
		run(name(left) + "-compare", reader, writer);
		awaitClosed(reader);
		Assertions.assertFalse(writer.getWrittenItems().isEmpty());
		for (KeyComparison<String> comparison : writer.getWrittenItems()) {
			Assertions.assertEquals(Status.OK, comparison.getStatus());
		}
	}

	protected RedisItemReader<String, KeyComparison<String>> comparisonReader(RedisTestContext left,
			RedisTestContext right) {
		return RedisItemReader.comparator(jobRunner, pool(left), pool(right), Duration.ofMillis(100)).build();
	}

	protected String name(RedisTestContext redis) {
		return testInfo.getTestMethod().get().getName() + "-" + redis.hashCode();
	}

	protected void generate(RedisTestContext redis) throws JobExecutionException {
		DataGeneratorOptions options = DataGeneratorOptions.builder().build();
		DataGeneratorItemReader reader = new DataGeneratorItemReader(options);
		reader.setMaxItemCount(options.getKeyRange().getMax() - options.getKeyRange().getMin() + 1);
		String name = name(redis) + "-gen";
		SimpleStepBuilder<DataStructure<String>, DataStructure<String>> step = jobRunner.step(name, reader, null,
				RedisItemWriter.dataStructure(pool(redis)).build(), StepOptions.builder().build());
		Job job = jobRunner.job(name).start(step.build()).build();
		jobRunner.getJobLauncher().run(job, new JobParameters());
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

	protected ScanReaderBuilder<String, String, DataStructure<String>> dataStructureReader(RedisTestContext redis)
			throws Exception {
		return RedisItemReader.dataStructure(pool(redis), jobRunner);
	}

	protected ScanReaderBuilder<String, String, KeyDump<String>> keyDumpReader(RedisTestContext redis)
			throws Exception {
		return RedisItemReader.keyDump(pool(redis), jobRunner);
	}

	protected WriterBuilder<String, String, KeyDump<String>> keyDumpWriter(RedisTestContext context) {
		return RedisItemWriter.keyDump(pool(context));
	}

	protected WriterBuilder<String, String, DataStructure<String>> dataStructureWriter(RedisTestContext context) {
		return RedisItemWriter.dataStructure(pool(context), m -> new XAddArgs().id(m.getId()));
	}

	protected DataStructureValueReader<String, String> dataStructureValueReader(RedisTestContext context) {
		return new DataStructureValueReader<>(pool(context));
	}

	protected LiveReaderBuilder<String, String, KeyDump<String>> liveKeyDumpReader(RedisTestContext redis)
			throws Exception {
		return RedisItemReader.liveKeyDump(pool(redis), jobRunner, redis.getPubSubConnection(), StringCodec.UTF8);
	}

	@SuppressWarnings("unchecked")
	protected <B extends LiveReaderBuilder<?, ?, ?>> B configureLiveReader(B builder, int notificationQueueCapacity) {
		return (B) builder.notificationQueueOptions(QueueOptions.builder().capacity(notificationQueueCapacity).build())
				.stepOptions(DEFAULT_FLUSHING_STEP_OPTIONS);
	}

	protected LiveReaderBuilder<String, String, DataStructure<String>> liveDataStructureReader(RedisTestContext context)
			throws Exception {
		return RedisItemReader.liveDataStructure(pool(context), jobRunner, context.getPubSubConnection(),
				StringCodec.UTF8);
	}

	protected void flushAll(RedisTestContext context) {
		context.sync().flushall();
		Awaitility.await().until(() -> context.sync().dbsize() == 0);
	}
}
