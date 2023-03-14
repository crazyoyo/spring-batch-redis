package com.redis.spring.batch;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.UUID;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
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

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.ClientBuilder;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.common.ConnectionPoolBuilder;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.StepOptions;
import com.redis.spring.batch.reader.GeneratorItemReader;
import com.redis.spring.batch.reader.GeneratorReaderOptions;
import com.redis.spring.batch.reader.DataStructureValueReader;
import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.spring.batch.reader.KeyspaceNotificationReaderOptions;
import com.redis.spring.batch.reader.LiveReaderBuilder;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.QueueOptions;
import com.redis.spring.batch.reader.ScanReaderBuilder;
import com.redis.spring.batch.writer.WriterBuilder;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
@TestInstance(Lifecycle.PER_CLASS)
abstract class AbstractTestBase {

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

	protected static JobRunner jobRunner;

	protected AbstractRedisClient sourceClient;
	protected StatefulRedisModulesConnection<String, String> sourceConnection;
	protected StatefulRedisPubSubConnection<String, String> sourcePubSubConnection;
	protected GenericObjectPool<StatefulConnection<String, String>> sourcePool;
	protected GenericObjectPool<StatefulConnection<byte[], byte[]>> sourceByteArrayPool;
	protected AbstractRedisClient targetClient;
	protected StatefulRedisModulesConnection<String, String> targetConnection;
	protected StatefulRedisPubSubConnection<String, String> targetPubSubConnection;
	protected GenericObjectPool<StatefulConnection<String, String>> targetPool;
	protected GenericObjectPool<StatefulConnection<byte[], byte[]>> targetByteArrayPool;

	protected abstract RedisServer getSourceServer();

	protected abstract RedisServer getTargetServer();

	@BeforeAll
	void setup() {
		RedisServer source = getSourceServer();
		source.start();
		RedisServer target = getTargetServer();
		target.start();
		sourceClient = ClientBuilder.create(RedisURI.create(source.getRedisURI())).cluster(source.isCluster()).build();
		sourceConnection = RedisModulesUtils.connection(sourceClient);
		sourcePubSubConnection = RedisModulesUtils.pubSubConnection(sourceClient);
		sourcePool = pool(sourceClient);
		sourceByteArrayPool = pool(sourceClient, ByteArrayCodec.INSTANCE);
		targetClient = ClientBuilder.create(RedisURI.create(target.getRedisURI())).cluster(target.isCluster()).build();
		targetConnection = RedisModulesUtils.connection(targetClient);
		targetPubSubConnection = RedisModulesUtils.pubSubConnection(targetClient);
		targetPool = pool(targetClient);
		targetByteArrayPool = pool(targetClient, ByteArrayCodec.INSTANCE);
		jobRunner = new JobRunner(jobRepository, transactionManager);

	}

	private GenericObjectPool<StatefulConnection<String, String>> pool(AbstractRedisClient redis) {
		return pool(redis, StringCodec.UTF8);
	}

	private <K, V> GenericObjectPool<StatefulConnection<K, V>> pool(AbstractRedisClient redis, RedisCodec<K, V> codec) {
		return ConnectionPoolBuilder.create(redis).build(codec);
	}

	@AfterAll
	void teardown() {
		sourcePool.close();
		sourceConnection.close();
		sourcePubSubConnection.close();
		sourceClient.shutdown();
		sourceClient.getResources().shutdown();
		targetConnection.close();
		targetPubSubConnection.close();
		targetPool.close();
		targetClient.shutdown();
		targetClient.getResources().shutdown();
		getTargetServer().close();
		getSourceServer().close();
	}

	@BeforeEach
	void flushAll() {
		sourceConnection.sync().flushall();
		targetConnection.sync().flushall();
	}

	protected AbstractRedisClient client(RedisServer server) {
		RedisURI uri = RedisURI.create(server.getRedisURI());
		return ClientBuilder.create(uri).cluster(server.isCluster()).build();
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

	protected <I, O> JobExecution runAsync(ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer,
			StepOptions stepOptions) throws JobExecutionException {
		String name = name();
		setName(name, reader, writer);
		SimpleStepBuilder<I, O> step = jobRunner.step(name, reader, processor, writer, stepOptions);
		Job job = jobRunner.job(name).start(step.build()).build();
		return jobRunner.getAsyncJobLauncher().run(job, new JobParameters());
	}

	protected <I, O> JobExecution runAsync(ItemReader<I> reader, ItemWriter<O> writer) throws JobExecutionException {
		return runAsync(reader, null, writer, DEFAULT_FLUSHING_STEP_OPTIONS);
	}

	protected <I, O> void run(ItemReader<I> reader, ItemWriter<O> writer) throws JobExecutionException {
		run(reader, null, writer, DEFAULT_STEP_OPTIONS);
	}

	protected <I, O> void run(ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer,
			StepOptions stepOptions) throws JobExecutionException {
		String name = name();
		setName(name, reader, writer);
		SimpleStepBuilder<I, O> step = jobRunner.step(name, reader, processor, writer, stepOptions);
		Job job = jobRunner.job(name).start(step.build()).build();
		jobRunner.getJobLauncher().run(job, new JobParameters());
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
	 * @throws JobExecutionException
	 * @throws Exception
	 */
	protected boolean compare() throws JobExecutionException {
		Assertions.assertEquals(sourceConnection.sync().dbsize(), targetConnection.sync().dbsize(),
				"Source and target have different db sizes");
		RedisItemReader<String, KeyComparison<String>> reader = comparisonReader();
		ListItemWriter<KeyComparison<String>> writer = new ListItemWriter<>();
		run(reader, null, writer, DEFAULT_STEP_OPTIONS);
		awaitClosed(reader);
		Assertions.assertFalse(writer.getWrittenItems().isEmpty());
		for (KeyComparison<String> comparison : writer.getWrittenItems()) {
			Assertions.assertEquals(Status.OK, comparison.getStatus(),
					MessageFormat.format("Key={0}, Type={1}", comparison.getKey(), comparison.getSource().getType()));
		}
		return true;
	}

	protected RedisItemReader<String, KeyComparison<String>> comparisonReader() {
		return RedisItemReader.comparator(jobRunner, sourcePool, targetPool, Duration.ofMillis(100)).build();
	}

	protected String name() {
		return UUID.randomUUID().toString();
	}

	protected void generate() throws JobExecutionException {
		String name = name();
		GeneratorReaderOptions options = GeneratorReaderOptions.builder().build();
		GeneratorItemReader reader = new GeneratorItemReader(options);
		reader.setMaxItemCount(options.getKeyRange().getMax() - options.getKeyRange().getMin() + 1);
		SimpleStepBuilder<DataStructure<String>, DataStructure<String>> step = jobRunner.step(name, reader, null,
				RedisItemWriter.dataStructure(sourcePool).build(), StepOptions.builder().build());
		Job job = jobRunner.job(name).start(step.build()).build();
		jobRunner.getJobLauncher().run(job, new JobParameters());
	}

	protected ScanReaderBuilder<String, String, DataStructure<String>> dataStructureReader() throws Exception {
		return RedisItemReader.dataStructure(sourcePool, jobRunner);
	}

	protected ScanReaderBuilder<String, String, KeyDump<String>> keyDumpReader() throws Exception {
		return RedisItemReader.keyDump(sourcePool, jobRunner);
	}

	protected WriterBuilder<String, String, KeyDump<String>> keyDumpWriter() {
		return RedisItemWriter.keyDump(targetPool);
	}

	protected WriterBuilder<String, String, DataStructure<String>> dataStructureWriter() {
		return RedisItemWriter.dataStructure(targetPool, m -> new XAddArgs().id(m.getId()));
	}

	protected DataStructureValueReader<String, String> dataStructureValueReader() {
		return new DataStructureValueReader<>(sourcePool);
	}

	protected LiveReaderBuilder<String, String, KeyDump<String>> liveKeyDumpReader() {
		return RedisItemReader.liveKeyDump(sourcePool, jobRunner, sourceClient, StringCodec.UTF8);
	}

	protected <K, V, T extends KeyValue<K>, B extends LiveReaderBuilder<K, V, T>> LiveRedisItemReader<K, T> liveReader(
			B builder, int notificationQueueCapacity) {
		return builder
				.notificationReaderOptions(KeyspaceNotificationReaderOptions.builder()
						.queueOptions(QueueOptions.builder().capacity(notificationQueueCapacity).build()).build())
				.stepOptions(DEFAULT_FLUSHING_STEP_OPTIONS).build();
	}

	protected LiveReaderBuilder<String, String, DataStructure<String>> liveDataStructureReader() throws Exception {
		return RedisItemReader.liveDataStructure(sourcePool, jobRunner, sourceClient, StringCodec.UTF8);
	}

	protected void flushAll(AbstractRedisClient client) {
		try (StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils.connection(client)) {
			connection.sync().flushall();
			Awaitility.await().until(() -> connection.sync().dbsize() == 0);
		}
	}
}
