package com.redis.spring.batch;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.util.ClientBuilder;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.RedisItemReader.ScanBuilder;
import com.redis.spring.batch.RedisItemWriter.DataStructureBuilder;
import com.redis.spring.batch.RedisItemWriter.KeyDumpBuilder;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.common.ReaderOptions;
import com.redis.spring.batch.common.StepOptions;
import com.redis.spring.batch.reader.GeneratorItemReader;
import com.redis.spring.batch.reader.GeneratorReaderOptions;
import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.spring.batch.writer.DataStructureWriteOptions;
import com.redis.spring.batch.writer.DataStructureWriteOptions.StreamIdPolicy;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.RedisCodec;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
@TestInstance(Lifecycle.PER_CLASS)
abstract class AbstractTestBase {

	private static final Duration DEFAULT_AWAIT_TIMEOUT = Duration.ofMillis(1000);
	protected static final StepOptions DEFAULT_STEP_OPTIONS = StepOptions.builder().build();
	protected static final StepOptions DEFAULT_FLUSHING_STEP_OPTIONS = StepOptions.builder()
			.flushingInterval(Duration.ofMillis(50)).idleTimeout(Duration.ofMillis(300)).build();
	private static final Duration DEFAULT_POLL_INTERVAL = Duration.ofMillis(30);

	@Value("${running-timeout:PT5S}")
	private Duration runningTimeout;
	@Value("${termination-timeout:PT5S}")
	private Duration terminationTimeout;

	protected JobRunner jobRunner;

	protected AbstractRedisClient sourceClient;
	protected StatefulRedisModulesConnection<String, String> sourceConnection;
	protected AbstractRedisClient targetClient;
	protected StatefulRedisModulesConnection<String, String> targetConnection;
	private ReaderOptions readerOptions;

	protected abstract RedisServer getSourceServer();

	protected abstract RedisServer getTargetServer();

	@BeforeAll
	void setup() throws Exception {
		getSourceServer().start();
		getTargetServer().start();
		sourceClient = client(getSourceServer());
		sourceConnection = RedisModulesUtils.connection(sourceClient);
		targetClient = client(getTargetServer());
		targetConnection = RedisModulesUtils.connection(targetClient);
		jobRunner = JobRunner.inMemory().runningTimeout(runningTimeout).terminationTimeout(terminationTimeout);
		readerOptions = ReaderOptions.builder().jobRunner(jobRunner).build();
	}

	@AfterAll
	void teardown() {
		sourceConnection.close();
		sourceClient.shutdown();
		sourceClient.getResources().shutdown();
		targetConnection.close();
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

	private static class SimpleTestInfo implements TestInfo {

		private final TestInfo delegate;
		private final String[] suffixes;

		public SimpleTestInfo(TestInfo delegate, String... suffixes) {
			this.delegate = delegate;
			this.suffixes = suffixes;
		}

		@Override
		public String getDisplayName() {
			List<String> elements = new ArrayList<>();
			elements.add(delegate.getDisplayName());
			elements.addAll(Arrays.asList(suffixes));
			return String.join("-", elements);
		}

		@Override
		public Set<String> getTags() {
			return delegate.getTags();
		}

		@Override
		public Optional<Class<?>> getTestClass() {
			return delegate.getTestClass();
		}

		@Override
		public Optional<Method> getTestMethod() {
			return delegate.getTestMethod();
		}

	}

	public static String name(TestInfo testInfo) {
		return testInfo.getDisplayName().replace("(TestInfo)", "");
	}

	public static TestInfo testInfo(TestInfo testInfo, String... suffixes) {
		return new SimpleTestInfo(testInfo, suffixes);
	}

	protected AbstractRedisClient client(RedisServer server) {
		RedisURI uri = RedisURI.create(server.getRedisURI());
		return ClientBuilder.create(uri).cluster(server.isCluster()).build();
	}

	protected JobExecution awaitRunning(JobExecution jobExecution) {
		return jobRunner.awaitRunning(jobExecution);
	}

	protected JobExecution awaitTermination(JobExecution jobExecution) {
		return jobRunner.awaitTermination(jobExecution);
	}

	protected void awaitUntilFalse(Callable<Boolean> conditionEvaluator) {
		awaitUntil(() -> !conditionEvaluator.call());
	}

	protected void awaitUntil(Callable<Boolean> conditionEvaluator) {
		Awaitility.await().pollInterval(DEFAULT_POLL_INTERVAL).timeout(DEFAULT_AWAIT_TIMEOUT).until(conditionEvaluator);
	}

	protected Job job(TestInfo testInfo, SimpleStepBuilder<?, ?> step) {
		return job(testInfo).start(step.build()).build();
	}

	protected JobBuilder job(TestInfo testInfo) {
		return jobRunner.job(name(testInfo));
	}

	protected <I, O> SimpleStepBuilder<I, O> step(TestInfo testInfo, ItemReader<I> reader, ItemWriter<O> writer) {
		return step(testInfo, reader, null, writer, DEFAULT_STEP_OPTIONS);
	}

	protected <I, O> SimpleStepBuilder<I, O> step(TestInfo testInfo, ItemReader<I> reader,
			ItemProcessor<I, O> processor, ItemWriter<O> writer, StepOptions options) {
		setName(reader, testInfo);
		setName(writer, testInfo);
		return jobRunner.step(name(testInfo), reader, processor, writer, options);
	}

	protected <I, O> JobExecution run(TestInfo info, ItemReader<I> reader, ItemWriter<O> writer) throws Exception {
		return run(info, reader, null, writer);
	}

	protected <I, O> JobExecution run(TestInfo info, ItemReader<I> reader, ItemProcessor<I, O> processor,
			ItemWriter<O> writer) throws Exception {
		JobExecution execution = jobRunner.run(job(info, step(info, reader, processor, writer, DEFAULT_STEP_OPTIONS)));
		return execution;
	}

	protected <I, O> JobExecution runAsync(TestInfo info, ItemReader<I> reader, ItemProcessor<I, O> processor,
			ItemWriter<O> writer, StepOptions stepOptions) throws Exception {
		JobExecution execution = jobRunner.runAsync(job(info, step(info, reader, processor, writer, stepOptions)));
		return execution;
	}

	protected static void setName(ItemWriter<?> writer, TestInfo testInfo) {
		setName(writer, testInfo, "writer");
	}

	protected static void setName(ItemReader<?> reader, TestInfo testInfo) {
		setName(reader, testInfo, "reader");
	}

	private static void setName(Object object, TestInfo testInfo, String suffix) {
		if (object instanceof ItemStreamSupport) {
			((ItemStreamSupport) object).setName(name(testInfo(testInfo, suffix)));
		}
	}

	/**
	 * 
	 * @param left
	 * @param right
	 * @return list of differences
	 * @throws Exception
	 */
	protected List<? extends KeyComparison> compare(TestInfo testInfo) throws Exception {
		Assertions.assertEquals(sourceConnection.sync().dbsize(), targetConnection.sync().dbsize(),
				"Source and target have different db sizes");
		RedisItemReader<String, String, KeyComparison> reader = comparisonReader();
		SynchronizedListItemWriter<KeyComparison> writer = new SynchronizedListItemWriter<>();
		run(testInfo(testInfo, "compare"), reader, null, writer);
		Assertions.assertFalse(writer.getItems().isEmpty());
		return writer.getItems().stream().filter(c -> c.getStatus() != Status.OK).collect(Collectors.toList());
	}

	protected RedisItemReader<String, String, KeyComparison> comparisonReader() throws Exception {
		return RedisItemReader.compare(sourceClient, targetClient).options(readerOptions)
				.ttlTolerance(Duration.ofMillis(100)).build();
	}

	protected void generate(TestInfo testInfo) throws JobExecutionException {
		generate(testInfo, GeneratorReaderOptions.builder().build());
	}

	protected void generate(TestInfo testInfo, GeneratorReaderOptions options) throws JobExecutionException {
		generate(testInfo, sourceClient, options);
	}

	protected void generate(TestInfo testInfo, AbstractRedisClient client, GeneratorReaderOptions options)
			throws JobExecutionException {
		TestInfo finalTestInfo = testInfo(testInfo, "generate", String.valueOf(client.hashCode()));
		GeneratorItemReader reader = new GeneratorItemReader(options);
		RedisItemWriter<String, String, DataStructure<String>> writer = dataStructureWriter(client)
				.dataStructureOptions(DataStructureWriteOptions.builder().streamIdPolicy(StreamIdPolicy.DROP).build())
				.build();
		SimpleStepBuilder<DataStructure<String>, DataStructure<String>> step = step(finalTestInfo, reader, writer);
		jobRunner.run(job(finalTestInfo, step));
	}

	protected ScanBuilder<String, String, DataStructure<String>> dataStructureSourceReader() {
		return dataStructureReader(sourceClient);
	}

	protected ScanBuilder<String, String, DataStructure<String>> dataStructureReader(AbstractRedisClient client) {
		if (client instanceof RedisModulesClusterClient) {
			return RedisItemReader.dataStructure((RedisModulesClusterClient) client).options(readerOptions);
		}
		return RedisItemReader.dataStructure((RedisModulesClient) client).options(readerOptions);
	}

	protected <K, V> ScanBuilder<K, V, DataStructure<K>> dataStructureReader(AbstractRedisClient client,
			RedisCodec<K, V> codec) {
		if (client instanceof RedisModulesClusterClient) {
			return RedisItemReader.dataStructure((RedisModulesClusterClient) client, codec).options(readerOptions);
		}
		return RedisItemReader.dataStructure((RedisModulesClient) client, codec).options(readerOptions);
	}

	protected ScanBuilder<byte[], byte[], KeyDump<byte[]>> keyDumpSourceReader() {
		if (sourceClient instanceof RedisModulesClusterClient) {
			return RedisItemReader.keyDump((RedisModulesClusterClient) sourceClient).options(readerOptions);
		}
		return RedisItemReader.keyDump((RedisModulesClient) sourceClient).options(readerOptions);
	}

	protected KeyDumpBuilder<byte[], byte[]> keyDumpWriter(AbstractRedisClient client) {
		if (client instanceof RedisModulesClusterClient) {
			return RedisItemWriter.keyDump((RedisModulesClusterClient) client);
		}
		return RedisItemWriter.keyDump((RedisModulesClient) client);
	}

	protected DataStructureBuilder<String, String> dataStructureTargetWriter() {
		return dataStructureWriter(targetClient);
	}

	protected DataStructureBuilder<String, String> dataStructureWriter(AbstractRedisClient client) {
		if (client instanceof RedisModulesClusterClient) {
			return RedisItemWriter.dataStructure((RedisModulesClusterClient) client);
		}
		return RedisItemWriter.dataStructure((RedisModulesClient) client);
	}

	protected <K, V> DataStructureBuilder<K, V> dataStructureWriter(AbstractRedisClient client,
			RedisCodec<K, V> codec) {
		if (client instanceof RedisModulesClusterClient) {
			return RedisItemWriter.dataStructure((RedisModulesClusterClient) client, codec);
		}
		return RedisItemWriter.dataStructure((RedisModulesClient) client, codec);
	}

	protected void flushAll(AbstractRedisClient client) {
		try (StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils.connection(client)) {
			connection.sync().flushall();
			awaitUntil(() -> connection.sync().dbsize() == 0);
		}
	}
}
