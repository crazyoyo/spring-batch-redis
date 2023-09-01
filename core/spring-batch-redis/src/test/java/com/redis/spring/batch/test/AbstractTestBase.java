package com.redis.spring.batch.test;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.Assert;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.util.ClientBuilder;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.Mode;
import com.redis.spring.batch.ValueType;
import com.redis.spring.batch.RedisItemWriter.StreamIdPolicy;
import com.redis.spring.batch.gen.DataType;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.gen.StreamOptions;
import com.redis.spring.batch.reader.KeyValueItemProcessor;
import com.redis.spring.batch.reader.PollableItemReader;
import com.redis.spring.batch.reader.StreamItemReader;
import com.redis.spring.batch.step.FlushingStepBuilder;
import com.redis.spring.batch.util.BatchUtils;
import com.redis.spring.batch.util.CodecUtils;
import com.redis.spring.batch.util.IntRange;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisURI;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
@TestInstance(Lifecycle.PER_CLASS)
public abstract class AbstractTestBase {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    public static final int DEFAULT_CHUNK_SIZE = 50;

    private static final Duration DEFAULT_AWAIT_TIMEOUT = Duration.ofMillis(1000);

    protected static final Duration DEFAULT_ASYNC_DELAY = Duration.ofMillis(100);

    protected static final Duration DEFAULT_IDLE_TIMEOUT = Duration.ofMillis(300);

    private static final Duration DEFAULT_POLL_INTERVAL = Duration.ofMillis(30);

    protected static final int DEFAULT_GENERATOR_COUNT = 100;

    private static final IntRange DEFAULT_GENERATOR_KEY_RANGE = IntRange.to(10000);

    @Value("${running-timeout:PT5S}")
    private Duration runningTimeout;

    @Value("${termination-timeout:PT5S}")
    private Duration terminationTimeout;

    protected JobRepository jobRepository;

    protected AbstractRedisClient client;

    protected StatefulRedisModulesConnection<String, String> connection;

    protected RedisModulesCommands<String, String> commands;

    private SimpleJobLauncher jobLauncher;

    private SimpleJobLauncher asyncJobLauncher;

    private JobBuilderFactory jobBuilderFactory;

    private StepBuilderFactory stepBuilderFactory;

    protected abstract RedisServer getRedisServer();

    @BeforeAll
    void setup() throws Exception {
        getRedisServer().start();
        client = client(getRedisServer());
        connection = RedisModulesUtils.connection(client);
        commands = connection.sync();
        jobRepository = BatchUtils.inMemoryJobRepository();
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
        connection.close();
        client.shutdown();
        client.getResources().shutdown();
        getRedisServer().close();
    }

    @BeforeEach
    void flushAll() {
        connection.sync().flushall();
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

    protected static void assertExecutionSuccessful(int exitCode) {
        Assertions.assertEquals(0, exitCode);
    }

    protected String id() {
        return UUID.randomUUID().toString();
    }

    protected static void awaitOpen(Object object) {
        awaitUntil(() -> BatchUtils.isOpen(object));
    }

    protected static void awaitClosed(Object object) {
        awaitUntil(() -> BatchUtils.isClosed(object));
    }

    protected <I, O> SimpleStepBuilder<I, O> step(TestInfo testInfo, ItemReader<I> reader, ItemWriter<O> writer) {
        String name = name(testInfo);
        if (reader instanceof ItemStreamSupport) {
            ((ItemStreamSupport) reader).setName(name + "-reader");
        }
        SimpleStepBuilder<I, O> step = stepBuilderFactory.get(name).chunk(DEFAULT_CHUNK_SIZE);
        step.reader(reader);
        step.writer(writer);
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

    public static <T> void awaitEquals(Supplier<T> expected, Supplier<T> actual) {
        awaitUntil(() -> expected.get().equals(actual.get()));
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

    protected static void awaitUntilFalse(Callable<Boolean> conditionEvaluator) {
        awaitUntil(() -> !conditionEvaluator.call());
    }

    protected static void awaitUntil(Callable<Boolean> conditionEvaluator) {
        Awaitility.await().pollInterval(DEFAULT_POLL_INTERVAL).timeout(DEFAULT_AWAIT_TIMEOUT).until(conditionEvaluator);
    }

    protected JobBuilder job(TestInfo testInfo) {
        return jobBuilderFactory.get(name(testInfo));
    }

    protected GeneratorItemReader generator() {
        return generator(DEFAULT_GENERATOR_COUNT);
    }

    protected GeneratorItemReader generator(int count) {
        GeneratorItemReader generator = new GeneratorItemReader();
        generator.setMaxItemCount(count);
        generator.setKeyRange(DEFAULT_GENERATOR_KEY_RANGE);
        return generator;
    }

    protected void generate(TestInfo testInfo) throws JobExecutionException {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(DEFAULT_GENERATOR_COUNT);
        generate(testInfo, gen);
    }

    protected void generate(TestInfo testInfo, GeneratorItemReader reader) throws JobExecutionException {
        generate(testInfo, client, reader);
    }

    protected void generate(TestInfo testInfo, AbstractRedisClient client, GeneratorItemReader reader)
            throws JobExecutionException {
        TestInfo finalTestInfo = testInfo(testInfo, "generate", String.valueOf(client.hashCode()));
        RedisItemWriter<String, String> writer = structWriter(client);
        writer.setStreamIdPolicy(StreamIdPolicy.DROP);
        run(finalTestInfo, reader, writer);
    }

    protected RedisItemWriter<String, String> writer(AbstractRedisClient client) {
        return writer(client, StringCodec.UTF8);
    }

    protected RedisItemWriter<String, String> structWriter(AbstractRedisClient client) {
        return structWriter(client, StringCodec.UTF8);
    }

    protected <K, V> RedisItemWriter<K, V> writer(AbstractRedisClient client, RedisCodec<K, V> codec) {
        return new RedisItemWriter<>(client, codec);
    }

    protected <K, V> RedisItemWriter<K, V> structWriter(AbstractRedisClient client, RedisCodec<K, V> codec) {
        RedisItemWriter<K, V> writer = writer(client, codec);
        writer.setValueType(ValueType.STRUCT);
        return writer;
    }

    protected RedisItemReader<String, String> structReader(TestInfo testInfo, AbstractRedisClient client) {
        return structReader(testInfo, client, StringCodec.UTF8);
    }

    protected <K, V> RedisItemReader<K, V> structReader(TestInfo info, AbstractRedisClient client, RedisCodec<K, V> codec) {
        RedisItemReader<K, V> reader = reader(info, client, codec);
        reader.setValueType(ValueType.STRUCT);
        return reader;
    }

    protected RedisItemReader<String, String> reader(TestInfo info, AbstractRedisClient client) {
        return reader(info, client, StringCodec.UTF8);
    }

    protected <K, V> RedisItemReader<K, V> reader(TestInfo info, AbstractRedisClient client, RedisCodec<K, V> codec) {
        RedisItemReader<K, V> reader = new RedisItemReader<>(client, codec);
        reader.setJobRepository(jobRepository);
        reader.setName(name(info));
        return reader;
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
        return jobLauncher.run(job, new JobParameters());
    }

    protected JobExecution runAsync(Job job) throws JobExecutionException {
        JobExecution execution = asyncJobLauncher.run(job, new JobParameters());
        awaitRunning(execution);
        try {
            Thread.sleep(DEFAULT_ASYNC_DELAY.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return execution;
    }

    protected void setLive(RedisItemReader<?, ?> reader) {
        reader.setIdleTimeout(DEFAULT_IDLE_TIMEOUT);
        reader.setMode(Mode.LIVE);
    }

    protected void enableKeyspaceNotifications(AbstractRedisClient client) {
        RedisModulesUtils.connection(client).sync().configSet("notify-keyspace-events", "AK");
    }

    protected <I, O> FlushingStepBuilder<I, O> flushingStep(TestInfo testInfo, PollableItemReader<I> reader,
            ItemWriter<O> writer) {
        return new FlushingStepBuilder<>(step(testInfo, reader, writer)).idleTimeout(DEFAULT_IDLE_TIMEOUT);
    }

    protected void generateStreams(TestInfo testInfo, int messageCount) throws JobExecutionException {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setTypes(DataType.STREAM);
        gen.setMaxItemCount(3);
        StreamOptions streamOptions = new StreamOptions();
        streamOptions.setMessageCount(IntRange.is(messageCount));
        gen.setStreamOptions(streamOptions);
        generate(testInfo(testInfo, "streams"), gen);
    }

    protected StreamItemReader<String, String> streamReader(String stream, Consumer<String> consumer) {
        return new StreamItemReader<>(client, StringCodec.UTF8, stream, consumer);
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

    protected KeyValueItemProcessor<String, String> structReader() {
        KeyValueItemProcessor<String, String> reader = new KeyValueItemProcessor<>(client, StringCodec.UTF8);
        reader.setValueType(ValueType.STRUCT);
        reader.open(new ExecutionContext());
        return reader;
    }

    protected byte[] toByteArray(String key) {
        return CodecUtils.toByteArrayKeyFunction(StringCodec.UTF8).apply(key);
    }

    protected String toString(byte[] key) {
        return CodecUtils.toStringKeyFunction(ByteArrayCodec.INSTANCE).apply(key);
    }

    protected void open(ItemStream itemStream) {
        itemStream.open(new ExecutionContext());
    }

}
