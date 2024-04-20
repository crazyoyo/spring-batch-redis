package com.redis.spring.batch.test;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
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
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.retry.policy.MaxAttemptsRetryPolicy;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;
import org.testcontainers.lifecycle.Startable;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.ReaderMode;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.FlushingStepBuilder;
import com.redis.spring.batch.common.JobFactory;
import com.redis.spring.batch.common.PollableItemReader;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.gen.Item;
import com.redis.spring.batch.gen.ItemToKeyValueFunction;
import com.redis.spring.batch.gen.Range;
import com.redis.spring.batch.gen.StreamOptions;
import com.redis.spring.batch.operation.Operation;
import com.redis.spring.batch.reader.StreamItemReader;
import com.redis.spring.batch.util.Await;
import com.redis.spring.batch.util.BatchUtils;
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

	protected static final ItemProcessor<Item, KeyValue<String, Object>> genItemProcessor = new FunctionItemProcessor<>(
			new ItemToKeyValueFunction());

	private int chunkSize = DEFAULT_CHUNK_SIZE;
	private Duration idleTimeout = DEFAULT_IDLE_TIMEOUT;
	private Duration pollDelay = DEFAULT_POLL_DELAY;
	private Duration awaitPollInterval = DEFAULT_AWAIT_POLL_INTERVAL;
	private Duration awaitTimeout = DEFAULT_AWAIT_TIMEOUT;

	protected RedisURI redisURI;
	protected AbstractRedisClient redisClient;
	protected StatefulRedisModulesConnection<String, String> redisConnection;
	protected RedisModulesCommands<String, String> redisCommands;
	protected JobFactory jobFactory;

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
		jobFactory = new JobFactory();
		jobFactory.setName(UUID.randomUUID().toString());
		jobFactory.afterPropertiesSet();
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
	void flushAll() throws InterruptedException {
		redisCommands.flushall();
		awaitUntil(() -> redisCommands.pubsubNumpat() == 0);
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

	protected GeneratorItemReader generator(int count, Item.Type... types) {
		GeneratorItemReader gen = new GeneratorItemReader();
		gen.setMaxItemCount(count);
		if (!ObjectUtils.isEmpty(types)) {
			gen.setTypes(types);
		}
		return gen;
	}

	protected void live(RedisItemReader<?, ?, ?> reader) {
		reader.setMode(ReaderMode.LIVE);
		reader.setIdleTimeout(idleTimeout);
	}

	protected void configure(TestInfo info, RedisItemReader<?, ?, ?> reader, String... suffixes) {
		List<String> allSuffixes = new ArrayList<>(Arrays.asList(suffixes));
		allSuffixes.add("reader");
		reader.setName(name(testInfo(info, allSuffixes.toArray(new String[0]))));
		reader.setJobFactory(jobFactory);
		reader.setClient(redisClient);
	}

	protected RedisItemReader<byte[], byte[], KeyValue<byte[], byte[]>> dumpReader(TestInfo info, String... suffixes) {
		RedisItemReader<byte[], byte[], KeyValue<byte[], byte[]>> reader = RedisItemReader.dump();
		configure(info, reader, suffixes);
		return reader;
	}

	protected RedisItemReader<String, String, KeyValue<String, Object>> structReader(TestInfo info,
			String... suffixes) {
		RedisItemReader<String, String, KeyValue<String, Object>> reader = RedisItemReader.struct();
		configure(info, reader, suffixes);
		return reader;
	}

	protected int keyCount(String pattern) {
		return redisCommands.keys(pattern).size();
	}

	protected void awaitPubSub() {
		try {
			Await.await().until(() -> redisCommands.pubsubNumpat() > 0);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException("Interrupted", e);
		}
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

	protected <I, O> SimpleStepBuilder<I, O> step(TestInfo info, ItemReader<I> reader, ItemWriter<O> writer) {
		return step(info, reader, null, writer);
	}

	protected <I, O> SimpleStepBuilder<I, O> step(TestInfo info, ItemReader<I> reader, ItemProcessor<I, O> processor,
			ItemWriter<O> writer) {
		return step(info, chunkSize, reader, processor, writer);
	}

	protected <I, O> SimpleStepBuilder<I, O> step(TestInfo info, int chunkSize, ItemReader<I> reader,
			ItemProcessor<I, O> processor, ItemWriter<O> writer) {
		String name = name(info);
		SimpleStepBuilder<I, O> step = jobFactory.step(name, chunkSize);
		step.reader(reader);
		step.processor(processor);
		step.writer(writer);
		return step;
	}

	public static String name(TestInfo info) {
		String displayName = info.getDisplayName().replace("(TestInfo)", "");
		if (info.getTestClass().isPresent()) {
			displayName += "-" + ClassUtils.getShortName(info.getTestClass().get());
		}
		return displayName;
	}

	public static AbstractRedisClient client(RedisServer server) {
		if (server.isRedisCluster()) {
			return RedisModulesClusterClient.create(server.getRedisURI());
		}
		return RedisModulesClient.create(server.getRedisURI());
	}

	public void awaitRunning(JobExecution jobExecution) throws InterruptedException {
		awaitUntil(jobExecution::isRunning);
	}

	public void awaitTermination(JobExecution jobExecution) throws InterruptedException {
		awaitUntilFalse(jobExecution::isRunning);
	}

	protected void awaitUntilFalse(BooleanSupplier evaluator) throws InterruptedException {
		awaitUntil(() -> !evaluator.getAsBoolean());
	}

	protected void awaitUntil(BooleanSupplier evaluator) throws InterruptedException {
		Await.await().initialDelay(pollDelay).delay(awaitPollInterval).timeout(awaitTimeout).until(evaluator);
	}

	protected JobBuilder job(TestInfo info) {
		return jobFactory.jobBuilder(name(info));
	}

	protected <I, O> void generateAsync(TestInfo info, GeneratorItemReader reader) {
		Executors.newSingleThreadExecutor().execute(() -> {
			try {
				awaitPubSub();
				generate(info, reader);
			} catch (Exception e) {
				throw new RuntimeException("Could not run data gen", e);
			}
		});
	}

	protected void generate(TestInfo info, GeneratorItemReader reader)
			throws JobExecutionException, InterruptedException {
		generate(info, redisClient, reader);
	}

	protected void generate(TestInfo info, AbstractRedisClient client, GeneratorItemReader reader)
			throws JobExecutionException, InterruptedException {
		TestInfo testInfo = testInfo(info, "generate");
		RedisItemWriter<String, String, KeyValue<String, Object>> writer = RedisItemWriter.struct(StringCodec.UTF8);
		writer.setClient(client);
		run(testInfo, reader, writer);
	}

	protected void run(TestInfo info, GeneratorItemReader reader, ItemWriter<KeyValue<String, Object>> writer)
			throws JobExecutionException, InterruptedException {
		run(info, reader, genItemProcessor, writer);
	}

	protected <T> JobExecution run(TestInfo info, ItemReader<T> reader, ItemWriter<T> writer)
			throws JobExecutionException, InterruptedException {
		return run(info, reader, null, writer);
	}

	protected <I, O> JobExecution run(TestInfo info, ItemReader<I> reader, ItemProcessor<I, O> processor,
			ItemWriter<O> writer) throws JobExecutionException, InterruptedException {
		return run(info, step(info, reader, processor, writer));
	}

	protected <I, O> JobExecution run(TestInfo info, SimpleStepBuilder<I, O> step)
			throws JobExecutionException, InterruptedException {
		return run(job(info).start(faultTolerant(step).build()).build());
	}

	protected <I, O> FaultTolerantStepBuilder<I, O> faultTolerant(SimpleStepBuilder<I, O> step) {
		return step.faultTolerant().retryPolicy(new MaxAttemptsRetryPolicy()).retry(RedisCommandTimeoutException.class);
	}

	protected JobExecution run(Job job) throws JobExecutionException, InterruptedException {
		JobExecution execution = jobFactory.run(job);
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

	protected void generateStreams(TestInfo info, int messageCount) throws Exception {
		GeneratorItemReader gen = generator(3, Item.Type.STREAM);
		StreamOptions streamOptions = new StreamOptions();
		streamOptions.setMessageCount(Range.of(messageCount));
		gen.setStreamOptions(streamOptions);
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

	protected RedisItemReader<byte[], byte[], KeyValue<byte[], byte[]>> dumpReader(TestInfo info) {
		RedisItemReader<byte[], byte[], KeyValue<byte[], byte[]>> reader = RedisItemReader.dump();
		configure(info, reader);
		return reader;
	}

	protected <K, V> RedisItemReader<K, V, KeyValue<K, Object>> structReader(TestInfo info, RedisCodec<K, V> codec) {
		RedisItemReader<K, V, KeyValue<K, Object>> reader = RedisItemReader.struct(codec);
		configure(info, reader);
		return reader;
	}

	protected <T> RedisItemWriter<String, String, T> writer(Operation<String, String, T, Object> operation) {
		RedisItemWriter<String, String, T> writer = RedisItemWriter.operation(operation);
		writer.setClient(redisClient);
		return writer;
	}

}
