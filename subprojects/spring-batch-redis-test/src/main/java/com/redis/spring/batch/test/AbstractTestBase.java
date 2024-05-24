package com.redis.spring.batch.test;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.retry.policy.MaxAttemptsRetryPolicy;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;
import org.testcontainers.lifecycle.Startable;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.Await;
import com.redis.spring.batch.JobUtils;
import com.redis.spring.batch.Range;
import com.redis.spring.batch.item.PollableItemReader;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemReader.ReaderMode;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.DataType;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.common.Operation;
import com.redis.spring.batch.item.redis.gen.GeneratorItemReader;
import com.redis.spring.batch.item.redis.gen.StreamOptions;
import com.redis.spring.batch.item.redis.reader.MemKeyValue;
import com.redis.spring.batch.item.redis.reader.StreamItemReader;
import com.redis.spring.batch.step.FlushingStepBuilder;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

@TestInstance(Lifecycle.PER_CLASS)
public abstract class AbstractTestBase {

	public static final int DEFAULT_CHUNK_SIZE = 50;
	public static final Duration DEFAULT_IDLE_TIMEOUT = Duration.ofMillis(500);
	public static final Duration DEFAULT_POLL_DELAY = Duration.ZERO;
	public static final Duration DEFAULT_AWAIT_POLL_INTERVAL = Duration.ofMillis(1);
	public static final Duration DEFAULT_AWAIT_TIMEOUT = Duration.ofSeconds(3);

	private int chunkSize = DEFAULT_CHUNK_SIZE;
	private Duration idleTimeout = DEFAULT_IDLE_TIMEOUT;
	private Duration pollDelay = DEFAULT_POLL_DELAY;
	private Duration awaitPollInterval = DEFAULT_AWAIT_POLL_INTERVAL;
	private Duration awaitTimeout = DEFAULT_AWAIT_TIMEOUT;

	protected RedisURI redisURI;
	protected AbstractRedisClient redisClient;
	protected StatefulRedisModulesConnection<String, String> redisConnection;
	protected RedisModulesCommands<String, String> redisCommands;
	protected RedisModulesAsyncCommands<String, String> redisAsyncCommands;
	protected JobRepository jobRepository;
	private PlatformTransactionManager transactionManager;
	private TaskExecutorJobLauncher jobLauncher;

	public static RedisURI redisURI(RedisServer server) {
		return RedisURI.create(server.getRedisURI());
	}

	public void setIdleTimeout(Duration idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

	@BeforeAll
	void setup() throws Exception {
		// Source Redis setup
		RedisServer redis = getRedisServer();
		if (redis instanceof Startable) {
			((Startable) redis).start();
		}
		redisURI = redisURI(redis);
		redisClient = client(redis);
		redisConnection = RedisModulesUtils.connection(redisClient);
		redisCommands = redisConnection.sync();
		redisAsyncCommands = redisConnection.async();
		jobRepository = JobUtils.jobRepositoryFactoryBean().getObject();
		Assert.notNull(jobRepository, "Job repository is null");
		transactionManager = JobUtils.resourcelessTransactionManager();
		jobLauncher = new TaskExecutorJobLauncher();
		jobLauncher.setTaskExecutor(new SyncTaskExecutor());
		jobLauncher.setJobRepository(jobRepository);
		jobLauncher.afterPropertiesSet();
	}

	@AfterAll
	void teardown() {
		if (redisConnection != null) {
			redisConnection.close();
		}
		if (redisClient != null) {
			redisClient.shutdown();
			redisClient.getResources().shutdown();
		}
		RedisServer redis = getRedisServer();
		if (redis instanceof Startable) {
			((Startable) redis).stop();
		}
	}

	@BeforeEach
	void flushAll() throws TimeoutException, InterruptedException {
		redisCommands.flushall();
		awaitUntilNoSubscribers();
	}

	public static TestInfo testInfo(TestInfo info, String... suffixes) {
		return new SimpleTestInfo(info, suffixes);
	}

	public static <T> List<T> readAllAndClose(ItemStreamReader<T> reader) throws Exception {
		try {
			reader.open(new ExecutionContext());
			return readAll(reader);
		} finally {
			reader.close();
		}
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

	protected GeneratorItemReader generator(int count, DataType... types) {
		GeneratorItemReader gen = new GeneratorItemReader();
		gen.setMaxItemCount(count);
		if (!ObjectUtils.isEmpty(types)) {
			gen.getOptions().setTypes(types);
		}
		return gen;
	}

	protected void live(RedisItemReader<?, ?, ?> reader) {
		reader.setMode(ReaderMode.LIVEONLY);
		reader.setIdleTimeout(idleTimeout);
	}

	protected void configure(TestInfo info, RedisItemReader<?, ?, ?> reader, String... suffixes) {
		setName(info, reader, suffixes);
		reader.setJobRepository(jobRepository);
		reader.setClient(redisClient);
	}

	protected void setName(TestInfo info, ItemStreamSupport streamSupport, String... suffixes) {
		List<String> allSuffixes = new ArrayList<>(Arrays.asList(suffixes));
		allSuffixes.add("reader");
		streamSupport.setName(name(testInfo(info, allSuffixes.toArray(new String[0]))));
	}

	protected RedisItemReader<byte[], byte[], MemKeyValue<byte[], byte[]>> dumpReader(TestInfo info,
			String... suffixes) {
		RedisItemReader<byte[], byte[], MemKeyValue<byte[], byte[]>> reader = RedisItemReader.dump();
		configure(info, reader, suffixes);
		return reader;
	}

	protected RedisItemReader<String, String, MemKeyValue<String, Object>> structReader(TestInfo info,
			String... suffixes) {
		return structReader(info, StringCodec.UTF8, suffixes);
	}

	protected <K, V> RedisItemReader<K, V, MemKeyValue<K, Object>> structReader(TestInfo info, RedisCodec<K, V> codec,
			String... suffixes) {
		RedisItemReader<K, V, MemKeyValue<K, Object>> reader = RedisItemReader.struct(codec);
		configure(info, reader, suffixes);
		return reader;
	}

	protected int keyCount(String pattern) {
		return redisCommands.keys(pattern).size();
	}

	protected void awaitUntilSubscribers() {
		try {
			awaitUntil(() -> redisCommands.pubsubNumpat() > 0);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ItemStreamException("Interrupted", e);
		} catch (TimeoutException e) {
			throw new ItemStreamException("Timeout while waiting for subscribers", e);
		}
	}

	protected void awaitUntilNoSubscribers() throws TimeoutException, InterruptedException {
		awaitUntil(() -> redisCommands.pubsubNumpat() == 0);
	}

	public int getChunkSize() {
		return chunkSize;
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	public Duration getPollDelay() {
		return pollDelay;
	}

	public Duration getAwaitPollInterval() {
		return awaitPollInterval;
	}

	public Duration getAwaitTimeout() {
		return awaitTimeout;
	}

	public Duration getIdleTimeout() {
		return idleTimeout;
	}

	public void setPollDelay(Duration pollDelay) {
		this.pollDelay = pollDelay;
	}

	public void setAwaitPollInterval(Duration awaitPollInterval) {
		this.awaitPollInterval = awaitPollInterval;
	}

	public void setAwaitTimeout(Duration awaitTimeout) {
		this.awaitTimeout = awaitTimeout;
	}

	protected abstract RedisServer getRedisServer();

	protected <I, O> SimpleStepBuilder<I, O> step(TestInfo info, ItemReader<? extends I> reader, ItemWriter<O> writer) {
		return step(info, reader, null, writer);
	}

	protected <I, O> SimpleStepBuilder<I, O> step(TestInfo info, ItemReader<? extends I> reader,
			ItemProcessor<I, O> processor, ItemWriter<O> writer) {
		return step(info, chunkSize, reader, processor, writer);
	}

	protected <I, O> SimpleStepBuilder<I, O> step(TestInfo info, int chunkSize, ItemReader<? extends I> reader,
			ItemProcessor<I, O> processor, ItemWriter<O> writer) {
		String name = name(info);
		SimpleStepBuilder<I, O> step = step(name, chunkSize);
		step.reader(reader);
		step.processor(processor);
		step.writer(writer);
		return step;
	}

	protected <I, O> SimpleStepBuilder<I, O> step(String name, int chunkSize) {
		return new StepBuilder(name, jobRepository).chunk(chunkSize, transactionManager);
	}

	public static String name(TestInfo info) {
		StringBuilder displayName = new StringBuilder(info.getDisplayName().replace("(TestInfo)", ""));
		info.getTestClass().ifPresent(c -> displayName.append("-").append(ClassUtils.getShortName(c)));
		return displayName.toString();
	}

	public static AbstractRedisClient client(RedisServer server) {
		if (server.isRedisCluster()) {
			return RedisModulesClusterClient.create(server.getRedisURI());
		}
		return RedisModulesClient.create(server.getRedisURI());
	}

	public void awaitRunning(JobExecution jobExecution) throws TimeoutException, InterruptedException {
		awaitUntil(jobExecution::isRunning);
	}

	public void awaitTermination(JobExecution jobExecution) throws TimeoutException, InterruptedException {
		awaitUntilFalse(jobExecution::isRunning);
	}

	protected void awaitUntilFalse(BooleanSupplier evaluator) throws TimeoutException, InterruptedException {
		awaitUntil(() -> !evaluator.getAsBoolean());
	}

	protected void awaitUntil(BooleanSupplier evaluator) throws TimeoutException, InterruptedException {
		Await.await().initialDelay(pollDelay).delay(awaitPollInterval).timeout(awaitTimeout).until(evaluator);
	}

	protected JobBuilder job(TestInfo info) {
		return job(name(info));
	}

	protected JobBuilder job(String name) {
		return new JobBuilder(name, jobRepository);
	}

	protected void generateAsync(TestInfo info, GeneratorItemReader reader) {
		Executors.newSingleThreadExecutor().execute(() -> {
			try {
				awaitUntilSubscribers();
				generate(info, reader);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new ItemStreamException("Data gen interrupted", e);
			} catch (Exception e) {
				throw new ItemStreamException("Could not run data gen", e);
			}
		});
	}

	protected void generate(TestInfo info, GeneratorItemReader reader)
			throws JobExecutionException, TimeoutException, InterruptedException {
		generate(info, redisClient, reader);
	}

	protected void generate(TestInfo info, AbstractRedisClient client, GeneratorItemReader reader)
			throws JobExecutionException, TimeoutException, InterruptedException {
		TestInfo testInfo = testInfo(info, "generate");
		RedisItemWriter<String, String, KeyValue<String, Object>> writer = RedisItemWriter.struct(StringCodec.UTF8);
		writer.setClient(client);
		run(testInfo, reader, writer);
	}

	protected <T> JobExecution run(TestInfo info, ItemReader<? extends T> reader, ItemWriter<T> writer)
			throws JobExecutionException, TimeoutException, InterruptedException {
		return run(info, reader, null, writer);
	}

	protected <I, O> JobExecution run(TestInfo info, ItemReader<I> reader, ItemProcessor<I, O> processor,
			ItemWriter<O> writer) throws JobExecutionException, TimeoutException, InterruptedException {
		return run(info, step(info, reader, processor, writer));
	}

	protected <I, O> JobExecution run(TestInfo info, SimpleStepBuilder<I, O> step)
			throws JobExecutionException, TimeoutException, InterruptedException {
		return run(job(info).start(faultTolerant(step).build()).build());
	}

	protected <I, O> FaultTolerantStepBuilder<I, O> faultTolerant(SimpleStepBuilder<I, O> step) {
		return step.faultTolerant().retryPolicy(new MaxAttemptsRetryPolicy()).retry(RedisCommandTimeoutException.class);
	}

	protected JobExecution run(Job job) throws JobExecutionException, TimeoutException, InterruptedException {
		JobExecution execution = jobLauncher.run(job, new JobParameters());
		awaitUntilFalse(execution::isRunning);
		return execution;
	}

	protected void enableKeyspaceNotifications() {
		redisCommands.configSet("notify-keyspace-events", "AKE");
	}

	protected <I, O> FlushingStepBuilder<I, O> flushingStep(TestInfo info, PollableItemReader<I> reader,
			ItemWriter<O> writer) {
		return new FlushingStepBuilder<>(step(info, reader, writer)).idleTimeout(idleTimeout);
	}

	protected void generateStreams(TestInfo info, int messageCount)
			throws JobExecutionException, TimeoutException, InterruptedException {
		GeneratorItemReader gen = generator(3, DataType.STREAM);
		StreamOptions streamOptions = new StreamOptions();
		streamOptions.setMessageCount(Range.of(messageCount));
		gen.getOptions().setStreamOptions(streamOptions);
		generate(info, gen);
	}

	protected StreamItemReader<String, String> streamReader(TestInfo info, String stream, Consumer<String> consumer) {
		StreamItemReader<String, String> reader = new StreamItemReader<>(redisClient, StringCodec.UTF8, stream,
				consumer);
		reader.setName(name(testInfo(info, "stream-reader")));
		return reader;
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
		return BatchUtils.toByteArrayKeyFunction(StringCodec.UTF8).apply(key);
	}

	protected String toString(byte[] key) {
		return BatchUtils.toStringKeyFunction(ByteArrayCodec.INSTANCE).apply(key);
	}

	protected <T> RedisItemWriter<String, String, T> writer(Operation<String, String, T, Object> operation) {
		RedisItemWriter<String, String, T> writer = new RedisItemWriter<>(StringCodec.UTF8, operation);
		writer.setClient(redisClient);
		return writer;
	}

}
