package com.redis.spring.batch.test;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.awaitility.Awaitility;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.batch.BatchDataSourceScriptDatabaseInitializer;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.boot.sql.init.DatabaseInitializationMode;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.retry.policy.MaxAttemptsRetryPolicy;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.testcontainers.lifecycle.Startable;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.Mode;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.common.Range;
import com.redis.spring.batch.common.ValueReader;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.gen.StreamOptions;
import com.redis.spring.batch.reader.DumpItemReader;
import com.redis.spring.batch.reader.PollableItemReader;
import com.redis.spring.batch.reader.StreamItemReader;
import com.redis.spring.batch.reader.StructItemReader;
import com.redis.spring.batch.step.FlushingStepBuilder;
import com.redis.spring.batch.util.CodecUtils;
import com.redis.spring.batch.util.ConnectionUtils;
import com.redis.spring.batch.writer.OperationItemWriter;
import com.redis.spring.batch.writer.StructItemWriter;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.support.ConnectionPoolSupport;

@TestInstance(Lifecycle.PER_CLASS)
public abstract class AbstractTestBase {

	protected static final int chunkSize = 50;

	protected static final Duration idleTimeout = Duration.ofMillis(300);

	private static final Duration pollDelay = Duration.ZERO;

	protected static final int defaultGeneratorCount = 73;

	private static final Duration awaitPollInterval = Duration.ofMillis(1);

	private static final Duration awaitTimeout = Duration.ofSeconds(3);

	@Value("${running-timeout:PT5S}")
	private Duration runningTimeout;

	@Value("${termination-timeout:PT5S}")
	private Duration terminationTimeout;

	protected JobRepository jobRepository;

	protected PlatformTransactionManager transactionManager;

	protected AbstractRedisClient client;

	protected GenericObjectPool<StatefulConnection<String, String>> pool;

	protected StatefulRedisModulesConnection<String, String> connection;

	protected RedisModulesCommands<String, String> commands;

	private TaskExecutorJobLauncher jobLauncher;

	protected abstract RedisServer getRedisServer();

	public static final DataType[] REDIS_GENERATOR_TYPES = { DataType.HASH, DataType.LIST, DataType.SET,
			DataType.STREAM, DataType.STRING, DataType.ZSET };

	public static final DataType[] REDIS_MODULES_GENERATOR_TYPES = { DataType.HASH, DataType.LIST, DataType.SET,
			DataType.STREAM, DataType.STRING, DataType.ZSET, DataType.JSON };

	public static TestInfo testInfo(TestInfo info, String... suffixes) {
		return new SimpleTestInfo(info, suffixes);
	}

	public static <T> List<T> readAll(ItemReader<T> reader) throws Exception {
		List<T> list = new ArrayList<>();
		T element;
		while ((element = reader.read()) != null) {
			list.add(element);
		}
		return list;
	}

	public static void assertDbNotEmpty(RedisModulesCommands<String, String> commands) {
		Assertions.assertTrue(commands.dbsize() > 0, "Redis database is empty");
	}

	@BeforeAll
	void setup() throws Exception {
		// Source Redis setup
		RedisServer redis = getRedisServer();
		if (redis instanceof Startable) {
			((Startable) redis).start();
		}
		client = client(getRedisServer());
		pool = ConnectionPoolSupport.createGenericObjectPool(ConnectionUtils.supplier(client),
				new GenericObjectPoolConfig<>());
		connection = RedisModulesUtils.connection(client);
		commands = connection.sync();

		transactionManager = new ResourcelessTransactionManager();
		// Job infra setup
		JobRepositoryFactoryBean bean = new JobRepositoryFactoryBean();
		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setURL("jdbc:hsqldb:mem:" + UUID.randomUUID());
		bean.setDataSource(dataSource);
		BatchProperties.Jdbc jdbc = new BatchProperties.Jdbc();
		jdbc.setInitializeSchema(DatabaseInitializationMode.ALWAYS);
		BatchDataSourceScriptDatabaseInitializer initializer = new BatchDataSourceScriptDatabaseInitializer(dataSource,
				jdbc);
		initializer.afterPropertiesSet();
		initializer.initializeDatabase();
		bean.setTransactionManager(transactionManager);
		bean.afterPropertiesSet();
		jobRepository = bean.getObject();
		jobLauncher = new TaskExecutorJobLauncher();
		jobLauncher.setJobRepository(jobRepository);
		jobLauncher.setTaskExecutor(new SyncTaskExecutor());
		jobLauncher.afterPropertiesSet();
	}

	@AfterAll
	void teardown() {
		if (connection != null) {
			connection.close();
		}
		if (pool != null) {
			pool.close();
		}
		if (client != null) {
			client.shutdown();
			client.getResources().shutdown();
		}
		RedisServer redis = getRedisServer();
		if (redis instanceof Startable) {
			((Startable) redis).stop();
		}
	}

	@BeforeEach
	void flushAll() {
		commands.flushall();
	}

	protected GeneratorItemReader generator(int count) {
		return generator(count, generatorDataTypes());
	}

	protected abstract DataType[] generatorDataTypes();

	protected GeneratorItemReader generator(DataType... types) {
		return generator(defaultGeneratorCount, types);
	}

	protected GeneratorItemReader generator(int count, DataType... types) {
		GeneratorItemReader gen = new GeneratorItemReader();
		gen.setMaxItemCount(count);
		gen.setTypes(types);
		return gen;
	}

	protected <I, O> SimpleStepBuilder<I, O> step(TestInfo info, ItemReader<I> reader, ItemWriter<O> writer) {
		return step(info, reader, null, writer);
	}

	protected <I, O> SimpleStepBuilder<I, O> step(TestInfo info, ItemReader<I> reader, ItemProcessor<I, O> processor,
			ItemWriter<O> writer) {
		return step(info, chunkSize, reader, processor, writer);
	}

	@SuppressWarnings("rawtypes")
	protected <I, O> SimpleStepBuilder<I, O> step(TestInfo info, int chunkSize, ItemReader<I> reader,
			ItemProcessor<I, O> processor, ItemWriter<O> writer) {
		String name = name(info);
		SimpleStepBuilder<I, O> step = new StepBuilder(name, jobRepository).chunk(chunkSize, transactionManager);
		if (reader instanceof RedisItemReader) {
			((RedisItemReader) reader).setName(name + "-reader");
		}
		step.reader(reader);
		step.processor(processor);
		step.writer(writer);
		return step;
	}

	public String name(TestInfo info) {
		String displayName = info.getDisplayName().replace("(TestInfo)", "");
		if (info.getTestClass().isPresent()) {
			displayName += ClassUtils.getShortName(info.getTestClass().get());
		}
		return displayName;
	}

	protected AbstractRedisClient client(RedisServer server) {
		if (server.isRedisCluster()) {
			return RedisModulesClusterClient.create(server.getRedisURI());
		}
		return RedisModulesClient.create(server.getRedisURI());
	}

	public void awaitRunning(JobExecution jobExecution) {
		awaitUntil(jobExecution::isRunning);
	}

	public void awaitTermination(JobExecution jobExecution) {
		awaitUntilFalse(jobExecution::isRunning);
	}

	protected void awaitUntilFalse(Callable<Boolean> evaluator) {
		awaitUntil(() -> !evaluator.call());
	}

	protected void awaitUntil(Callable<Boolean> evaluator) {
		Awaitility.await().pollDelay(pollDelay).pollInterval(awaitPollInterval).timeout(awaitTimeout).until(evaluator);
	}

	protected JobBuilder job(TestInfo info) {
		return new JobBuilder(name(info), jobRepository);
	}

	protected GeneratorItemReader generator() {
		return generator(defaultGeneratorCount);
	}

	protected <I, O> void generateAsync(TestInfo info, FlushingStepBuilder<I, O> step, GeneratorItemReader reader) {
		AtomicBoolean flag = new AtomicBoolean();
		step.listener(new ItemReadListener<>() {

			@Override
			public void beforeRead() {
				if (flag.get()) {
					return;
				}
				Executors.newSingleThreadExecutor().execute(() -> {
					try {
						generate(info, reader);
					} catch (Exception e) {
						throw new RuntimeException("Could not run data gen", e);
					}
				});
				flag.set(true);
			}
		});
	}

	protected void generate(TestInfo info) throws Exception {
		generate(info, generator(defaultGeneratorCount));
	}

	protected void generate(TestInfo info, GeneratorItemReader reader) throws Exception {
		generate(info, client, reader);
	}

	protected void generate(TestInfo info, AbstractRedisClient client, GeneratorItemReader reader) throws Exception {
		TestInfo testInfo = testInfo(info, "generate", String.valueOf(client.hashCode()));
		StructItemWriter<String, String> writer = RedisItemWriter.struct(client, CodecUtils.STRING_CODEC);
		run(testInfo, reader, writer);
	}

	protected void flushAll(AbstractRedisClient client) {
		try (StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils.connection(client)) {
			commands.flushall();
			awaitUntil(() -> commands.dbsize() == 0);
		}
	}

	protected <R extends RedisItemReader<?, ?, ?>> R live(R reader) {
		reader.setMode(Mode.LIVE);
		reader.setIdleTimeout(idleTimeout);
		return reader;
	}

	protected <I, O> JobExecution run(TestInfo info, ItemReader<I> reader, ItemWriter<O> writer)
			throws JobExecutionException {
		return run(info, reader, null, writer);
	}

	protected <I, O> JobExecution run(TestInfo info, ItemReader<I> reader, ItemProcessor<I, O> processor,
			ItemWriter<O> writer) throws JobExecutionException {
		return run(info, step(info, reader, processor, writer));
	}

	protected <I, O> JobExecution run(TestInfo info, SimpleStepBuilder<I, O> step) throws JobExecutionException {
		return run(job(info).start(faultTolerant(step).build()).build());
	}

	protected <I, O> FaultTolerantStepBuilder<I, O> faultTolerant(SimpleStepBuilder<I, O> step) {
		return step.faultTolerant().retryPolicy(new MaxAttemptsRetryPolicy()).retry(RedisCommandTimeoutException.class);
	}

	protected JobExecution run(Job job) throws JobExecutionException {
		return jobLauncher.run(job, new JobParameters());
	}

	protected void enableKeyspaceNotifications(AbstractRedisClient client) {
		commands.configSet("notify-keyspace-events", "AK");
	}

	protected <I, O> FlushingStepBuilder<I, O> flushingStep(TestInfo info, PollableItemReader<I> reader,
			ItemWriter<O> writer) {
		return new FlushingStepBuilder<>(step(info, reader, writer)).idleTimeout(idleTimeout);
	}

	protected void generateStreams(TestInfo info, int messageCount) throws Exception {
		GeneratorItemReader gen = generator(3, DataType.STREAM);
		StreamOptions streamOptions = new StreamOptions();
		streamOptions.setMessageCount(Range.of(messageCount));
		gen.setStreamOptions(streamOptions);
		generate(info, gen);
	}

	protected StreamItemReader<String, String> streamReader(String stream, Consumer<String> consumer) {
		return new StreamItemReader<>(client, CodecUtils.STRING_CODEC, stream, consumer);
	}

	protected void assertMessageBody(List<? extends StreamMessage<String, String>> items) {
		for (StreamMessage<String, String> message : items) {
			assertTrue(message.getBody().containsKey("field1"));
			assertTrue(message.getBody().containsKey("field2"));
		}
	}

	protected void assertStreamEquals(String expectedId, Map<String, String> expectedBody, String expectedStream,
			StreamMessage<String, String> message) {
		Assertions.assertEquals(expectedId, message.getId());
		Assertions.assertEquals(expectedBody, message.getBody());
		Assertions.assertEquals(expectedStream, message.getStream());
	}

	protected Map<String, String> map(String... args) {
		Assert.notNull(args, "Args cannot be null");
		Assert.isTrue(args.length % 2 == 0, "Args length is not a multiple of 2");
		Map<String, String> body = new LinkedHashMap<>();
		for (int index = 0; index < args.length / 2; index++) {
			body.put(args[index * 2], args[index * 2 + 1]);
		}
		return body;
	}

	protected byte[] toByteArray(String key) {
		return CodecUtils.toByteArrayKeyFunction(CodecUtils.STRING_CODEC).apply(key);
	}

	protected String toString(byte[] key) {
		return CodecUtils.toStringKeyFunction(ByteArrayCodec.INSTANCE).apply(key);
	}

	protected ValueReader<byte[], byte[], byte[], KeyValue<byte[]>> dumpOperationExecutor() {
		DumpItemReader reader = RedisItemReader.dump(client);
		ValueReader<byte[], byte[], byte[], KeyValue<byte[]>> executor = reader.operationValueReader();
		executor.open();
		return executor;
	}

	protected ValueReader<String, String, String, KeyValue<String>> structOperationExecutor() {
		return structOperationExecutor(CodecUtils.STRING_CODEC);
	}

	protected <K, V> ValueReader<K, V, K, KeyValue<K>> structOperationExecutor(RedisCodec<K, V> codec) {
		StructItemReader<K, V> reader = RedisItemReader.struct(client, codec);
		ValueReader<K, V, K, KeyValue<K>> executor = reader.operationValueReader();
		executor.open();
		return executor;
	}

	protected <T> OperationItemWriter<String, String, T> writer(Operation<String, String, T, Object> operation) {
		return RedisItemWriter.operation(client, CodecUtils.STRING_CODEC, operation);
	}

}
