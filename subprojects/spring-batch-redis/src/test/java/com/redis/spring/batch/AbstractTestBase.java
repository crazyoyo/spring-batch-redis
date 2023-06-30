package com.redis.spring.batch;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.test.context.junit4.SpringRunner;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.ClientBuilder;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.RedisItemReader.ScanBuilder;
import com.redis.spring.batch.RedisItemWriter.WriterBuilder;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.common.Openable;
import com.redis.spring.batch.common.Utils;
import com.redis.spring.batch.reader.GeneratorItemReader;
import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.spring.batch.reader.KeyComparisonItemReader;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.LiveRedisItemReader.Builder;
import com.redis.spring.batch.reader.PollableItemReader;
import com.redis.spring.batch.reader.ReaderOptions;
import com.redis.spring.batch.step.FlushingStepBuilder;
import com.redis.spring.batch.step.FlushingStepOptions;
import com.redis.spring.batch.writer.StreamIdPolicy;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
@TestInstance(Lifecycle.PER_CLASS)
abstract class AbstractTestBase {

	private static final Duration DEFAULT_AWAIT_TIMEOUT = Duration.ofMillis(1000);
	protected static final Duration DEFAULT_IDLE_TIMEOUT = Duration.ofMillis(300);
	private static final Duration DEFAULT_POLL_INTERVAL = Duration.ofMillis(30);

	@Value("${running-timeout:PT5S}")
	private Duration runningTimeout;
	@Value("${termination-timeout:PT5S}")
	private Duration terminationTimeout;

	protected JobRepository jobRepository;

	protected AbstractRedisClient sourceClient;
	protected StatefulRedisModulesConnection<String, String> sourceConnection;
	protected AbstractRedisClient targetClient;
	protected StatefulRedisModulesConnection<String, String> targetConnection;
	private SimpleJobLauncher jobLauncher;
	private SimpleJobLauncher asyncJobLauncher;
	private JobBuilderFactory jobBuilderFactory;
	private StepBuilderFactory stepBuilderFactory;

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
		jobRepository = Utils.inMemoryJobRepository();
		jobLauncher = new SimpleJobLauncher();
		jobLauncher.setJobRepository(jobRepository);
		jobLauncher.afterPropertiesSet();
		asyncJobLauncher = new SimpleJobLauncher();
		asyncJobLauncher.setJobRepository(jobRepository);
		asyncJobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
		asyncJobLauncher.afterPropertiesSet();
		jobBuilderFactory = new JobBuilderFactory(jobRepository);
		stepBuilderFactory = new StepBuilderFactory(jobRepository, new ResourcelessTransactionManager());
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

	protected void awaitClosed(Object object) {
		if (object instanceof Openable) {
			awaitUntilFalse(((Openable) object)::isOpen);
		}
	}

	protected <I, O> SimpleStepBuilder<I, O> step(TestInfo testInfo, ItemReader<I> reader, ItemWriter<O> writer) {
		String name = name(testInfo);
		if (reader instanceof ItemStreamSupport) {
			((ItemStreamSupport) reader).setName(name + "-reader");
		}
		SimpleStepBuilder<I, O> step = stepBuilderFactory.get(name).chunk(ReaderOptions.DEFAULT_CHUNK_SIZE);
		step.reader(reader);
		step.writer(writer);
		if (reader instanceof PollableItemReader) {
			return new FlushingStepBuilder<>(step)
					.options(FlushingStepOptions.builder().idleTimeout(DEFAULT_IDLE_TIMEOUT).build());
		}
		return step;
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

	public static JobExecution awaitRunning(JobExecution jobExecution) {
		Awaitility.await().until(() -> isRunning(jobExecution));
		return jobExecution;
	}

	public static JobExecution awaitTermination(JobExecution jobExecution) {
		Awaitility.await().until(() -> isTerminated(jobExecution));
		return jobExecution;
	}

	public static boolean isRunning(JobExecution jobExecution) {
		return jobExecution.isRunning() || jobExecution.getStatus().isUnsuccessful()
				|| jobExecution.getStatus() != BatchStatus.STARTING;
	}

	public static boolean isTerminated(JobExecution jobExecution) {
		return !jobExecution.isRunning() || jobExecution.getStatus().isUnsuccessful()
				|| jobExecution.getStatus() == BatchStatus.COMPLETED || jobExecution.getStatus() == BatchStatus.STOPPED
				|| jobExecution.getStatus().isGreaterThan(BatchStatus.STOPPED);
	}

	protected void awaitUntilFalse(Callable<Boolean> conditionEvaluator) {
		awaitUntil(() -> !conditionEvaluator.call());
	}

	protected void awaitUntil(Callable<Boolean> conditionEvaluator) {
		Awaitility.await().pollInterval(DEFAULT_POLL_INTERVAL).timeout(DEFAULT_AWAIT_TIMEOUT).until(conditionEvaluator);
	}

	protected JobBuilder job(TestInfo testInfo) {
		return jobBuilderFactory.get(name(testInfo));
	}

	/**
	 * 
	 * @param left
	 * @param right
	 * @return
	 * @return list of differences
	 * @throws Exception
	 */
	protected boolean compare(TestInfo testInfo) throws Exception {
		TestInfo finalTestInfo = testInfo(testInfo, "compare");
		KeyComparisonItemReader reader = comparisonReader();
		SynchronizedListItemWriter<KeyComparison> writer = new SynchronizedListItemWriter<>();
		run(job(finalTestInfo).start(step(finalTestInfo, reader, writer).build()).build());
		awaitClosed(reader);
		awaitClosed(writer);
		Assertions.assertFalse(writer.getItems().isEmpty());
		return writer.getItems().stream().allMatch(c -> c.getStatus() == Status.OK);
	}

	protected KeyComparisonItemReader comparisonReader() throws Exception {
		return new KeyComparisonItemReader.Builder(sourceClient, targetClient).jobRepository(jobRepository)
				.ttlTolerance(Duration.ofMillis(100)).build();
	}

	protected void generate(TestInfo testInfo) throws JobExecutionException {
		GeneratorItemReader gen = new GeneratorItemReader();
		gen.setMaxItemCount(100);
		generate(testInfo, gen);
	}

	protected void generate(TestInfo testInfo, GeneratorItemReader reader) throws JobExecutionException {
		generate(testInfo, sourceClient, reader);
	}

	protected void generate(TestInfo testInfo, AbstractRedisClient client, GeneratorItemReader reader)
			throws JobExecutionException {
		TestInfo finalTestInfo = testInfo(testInfo, "generate", String.valueOf(client.hashCode()));
		RedisItemWriter<String, String, DataStructure<String>> writer = new WriterBuilder(client)
				.streamIdPolicy(StreamIdPolicy.DROP).dataStructure();
		run(finalTestInfo, reader, writer);
	}

	protected RedisItemReader<String, String, DataStructure<String>> dataStructureSourceReader() {
		return dataStructureSourceReader(StringCodec.UTF8);
	}

	protected <K, V> RedisItemReader<K, V, DataStructure<K>> dataStructureSourceReader(RedisCodec<K, V> codec) {
		return dataStructureReader(sourceClient, codec);
	}

	protected <K, V> RedisItemReader<K, V, DataStructure<K>> dataStructureReader(AbstractRedisClient client,
			RedisCodec<K, V> codec) {
		return reader(client).dataStructure(codec);
	}

	protected ScanBuilder reader(AbstractRedisClient client) {
		return new ScanBuilder(client).jobRepository(jobRepository);
	}

	protected LiveRedisItemReader.Builder liveReader(AbstractRedisClient client) {
		Builder builder = reader(client).live();
		builder.flushingOptions(FlushingStepOptions.builder().idleTimeout(DEFAULT_IDLE_TIMEOUT).build());
		return builder;
	}

	protected RedisItemReader<byte[], byte[], KeyDump<byte[]>> keyDumpSourceReader() {
		return new ScanBuilder(sourceClient).jobRepository(jobRepository).keyDump();
	}

	protected RedisItemWriter<byte[], byte[], KeyDump<byte[]>> keyDumpWriter(AbstractRedisClient client) {
		return new WriterBuilder(client).keyDump();
	}

	protected void flushAll(AbstractRedisClient client) {
		try (StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils.connection(client)) {
			connection.sync().flushall();
			awaitUntil(() -> connection.sync().dbsize() == 0);
		}
	}

	protected <I, O> JobExecution run(TestInfo testInfo, ItemReader<I> reader, ItemWriter<O> writer)
			throws JobExecutionException {
		SimpleStepBuilder<I, O> step = step(testInfo, reader, writer);
		SimpleJobBuilder job = job(testInfo).start(step.build());
		JobExecution execution = run(job.build());
		awaitClosed(reader);
		awaitClosed(writer);
		return execution;
	}

	protected JobExecution run(Job job) throws JobExecutionException {
		JobExecution execution = jobLauncher.run(job, new JobParameters());
		awaitTermination(execution);
		return execution;
	}

	protected JobExecution runAsync(Job job) throws JobExecutionException {
		JobExecution execution = asyncJobLauncher.run(job, new JobParameters());
		awaitRunning(execution);
		return execution;
	}
}
