package com.redis.spring.batch;

import java.lang.reflect.Method;
import java.text.MessageFormat;
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
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
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
import com.redis.spring.batch.RedisItemReader.Builder;
import com.redis.spring.batch.RedisItemWriter.DataStructureBuilder.StreamIdPolicy;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.common.Openable;
import com.redis.spring.batch.common.PoolOptions;
import com.redis.spring.batch.common.StepOptions;
import com.redis.spring.batch.reader.DataStructureReadOperation;
import com.redis.spring.batch.reader.GeneratorItemReader;
import com.redis.spring.batch.reader.GeneratorReaderOptions;
import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.QueueOptions;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
@TestInstance(Lifecycle.PER_CLASS)
abstract class AbstractTestBase {

	private static final Duration DEFAULT_POLL_INTERVAL = Duration.ofMillis(100);
	private static final Duration DEFAULT_AWAIT_TIMEOUT = Duration.ofSeconds(1);
	protected static final StepOptions DEFAULT_STEP_OPTIONS = StepOptions.builder().build();
	protected static final StepOptions DEFAULT_FLUSHING_STEP_OPTIONS = StepOptions.builder()
			.flushingInterval(Duration.ofMillis(50)).idleTimeout(Duration.ofMillis(500)).build();

	@Value("${running-timeout:PT5S}")
	private Duration runningTimeout;
	@Value("${termination-timeout:PT5S}")
	private Duration terminationTimeout;

	private JobRunner jobRunner;

	protected AbstractRedisClient sourceClient;
	protected StatefulRedisModulesConnection<String, String> sourceConnection;
	private AbstractRedisClient targetClient;
	protected StatefulRedisModulesConnection<String, String> targetConnection;

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

	protected static String name(TestInfo testInfo) {
		return testInfo.getDisplayName().replace("(TestInfo)", "");
	}

	protected static TestInfo testInfo(TestInfo testInfo, String... suffixes) {
		return new SimpleTestInfo(testInfo, suffixes);
	}

	protected RedisItemReader.Builder<String, String> sourceReader() {
		return reader(sourceClient, StringCodec.UTF8);
	}

	protected <K, V> Builder<K, V> sourceReader(RedisCodec<K, V> codec) {
		return reader(sourceClient, codec);
	}

	protected <K, V> Builder<K, V> reader(AbstractRedisClient client, RedisCodec<K, V> codec) {
		if (client instanceof RedisModulesClusterClient) {
			return RedisItemReader.client((RedisModulesClusterClient) client, codec).jobRunner(jobRunner);
		}
		return RedisItemReader.client((RedisModulesClient) client, codec).jobRunner(jobRunner);
	}

	protected RedisItemWriter.WriterBuilder<String, String> targetWriter() {
		return writer(targetClient, StringCodec.UTF8);
	}

	protected <K, V> RedisItemWriter.WriterBuilder<K, V> targetWriter(RedisCodec<K, V> codec) {
		return writer(targetClient, codec);
	}

	protected static <K, V> RedisItemWriter.WriterBuilder<K, V> writer(AbstractRedisClient client,
			RedisCodec<K, V> codec) {
		if (client instanceof RedisModulesClusterClient) {
			return RedisItemWriter.client((RedisModulesClusterClient) client, codec);
		}
		return RedisItemWriter.client((RedisModulesClient) client, codec);
	}

	protected AbstractRedisClient client(RedisServer server) {
		RedisURI uri = RedisURI.create(server.getRedisURI());
		return ClientBuilder.create(uri).cluster(server.isCluster()).build();
	}

	protected void awaitOpen(Object object) {
		if (object instanceof Openable) {
			awaitUntil(((Openable) object)::isOpen);
		}
	}

	protected void awaitClosed(Object object) {
		if (object instanceof Openable) {
			awaitUntilFalse(((Openable) object)::isOpen);
		}
	}

	protected void awaitRunning(JobExecution jobExecution) {
		jobRunner.awaitRunning(jobExecution);
	}

	protected void awaitTermination(JobExecution jobExecution) {
		jobRunner.awaitTermination(jobExecution);
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

	protected JobExecution run(Job job) throws JobExecutionAlreadyRunningException, JobRestartException,
			JobInstanceAlreadyCompleteException, JobParametersInvalidException {
		return jobRunner.getJobLauncher().run(job, new JobParameters());
	}

	protected JobExecution runAsync(Job job) throws JobExecutionAlreadyRunningException, JobRestartException,
			JobInstanceAlreadyCompleteException, JobParametersInvalidException {
		return jobRunner.getAsyncJobLauncher().run(job, new JobParameters());
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
		JobExecution execution = run(job(info, step(info, reader, processor, writer, DEFAULT_STEP_OPTIONS)));
		awaitTermination(execution);
		awaitClosed(reader);
		awaitClosed(writer);
		return execution;
	}

	protected <I, O> JobExecution runAsync(TestInfo info, ItemReader<I> reader, ItemProcessor<I, O> processor,
			ItemWriter<O> writer, StepOptions stepOptions) throws Exception {
		JobExecution execution = runAsync(job(info, step(info, reader, processor, writer, stepOptions)));
		awaitRunning(execution);
		awaitOpen(reader);
		awaitOpen(writer);
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
	 * @return true if left and right have same keys
	 * @throws Exception
	 */
	protected boolean compare(TestInfo testInfo) throws Exception {
		Assertions.assertEquals(sourceConnection.sync().dbsize(), targetConnection.sync().dbsize(),
				"Source and target have different db sizes");
		RedisItemReader<String, KeyComparison> reader = comparisonReader();
		SynchronizedListItemWriter<KeyComparison> writer = new SynchronizedListItemWriter<>();
		run(testInfo(testInfo, "compare"), reader, null, writer);
		Assertions.assertFalse(writer.getItems().isEmpty());
		for (KeyComparison comparison : writer.getItems()) {
			Assertions.assertEquals(Status.OK, comparison.getStatus(),
					MessageFormat.format("Key={0}, Type={1}", comparison.getKey(), comparison.getSource().getType()));
		}
		return true;
	}

	protected RedisItemReader<String, KeyComparison> comparisonReader() throws Exception {
		return sourceReader().comparator(targetClient).ttlTolerance(Duration.ofMillis(100)).build();
	}

	protected void generate(TestInfo testInfo) throws JobExecutionException {
		generate(testInfo, GeneratorReaderOptions.builder().build());
	}

	protected void generate(TestInfo testInfo, GeneratorReaderOptions options) throws JobExecutionException {
		TestInfo finalTestInfo = testInfo(testInfo, "generate");
		GeneratorItemReader reader = new GeneratorItemReader(options);
		SimpleStepBuilder<DataStructure<String>, DataStructure<String>> step = step(finalTestInfo, reader,
				sourceWriter().dataStructure().build());
		JobExecution execution = run(job(finalTestInfo, step));
		awaitTermination(execution);
	}

	protected RedisItemWriter.WriterBuilder<String, String> sourceWriter() {
		return writer(sourceClient, StringCodec.UTF8);
	}

	protected RedisItemWriter<String, String, KeyDump<String>> targetKeyDumpWriter() {
		return targetWriter().keyDump().build();
	}

	protected RedisItemWriter<String, String, DataStructure<String>> targetDataStructureWriter() {
		return targetWriter().dataStructure().streamIdPolicy(StreamIdPolicy.PROPAGATE).build();
	}

	protected DataStructureReadOperation<String, String> sourceDataStructureValueReader() {
		return new DataStructureReadOperation<>(sourceClient, StringCodec.UTF8, PoolOptions.builder().build());
	}

	protected LiveRedisItemReader.Builder<String, String> sourceLiveReader() {
		return sourceReader().live().stepOptions(DEFAULT_FLUSHING_STEP_OPTIONS);
	}

	protected LiveRedisItemReader.Builder<String, String> sourceLiveReader(int notificationQueueCapacity) {
		return sourceLiveReader().queueOptions(QueueOptions.builder().capacity(notificationQueueCapacity).build());
	}

	protected void flushAll(AbstractRedisClient client) {
		try (StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils.connection(client)) {
			connection.sync().flushall();
			awaitUntil(() -> connection.sync().dbsize() == 0);
		}
	}
}
