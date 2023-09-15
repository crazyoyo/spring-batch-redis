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

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
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
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.Assert;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.util.ClientBuilder;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.Dump;
import com.redis.spring.batch.common.Range;
import com.redis.spring.batch.common.Struct;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.gen.StreamOptions;
import com.redis.spring.batch.reader.AbstractRedisItemReader;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.PollableItemReader;
import com.redis.spring.batch.reader.StreamItemReader;
import com.redis.spring.batch.step.FlushingStepBuilder;
import com.redis.spring.batch.util.BatchUtils;
import com.redis.spring.batch.util.CodecUtils;
import com.redis.spring.batch.util.ConnectionUtils;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisURI;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.support.ConnectionPoolSupport;

@SuppressWarnings("deprecation")
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

    private static final Range DEFAULT_GENERATOR_KEY_RANGE = Range.to(10000);

    @Value("${running-timeout:PT5S}")
    private Duration runningTimeout;

    @Value("${termination-timeout:PT5S}")
    private Duration terminationTimeout;

    protected JobRepository jobRepository;

    protected PlatformTransactionManager transactionManager;

    protected AbstractRedisClient client;

    protected GenericObjectPool<StatefulConnection<String, String>> pool;

    protected GenericObjectPool<StatefulConnection<byte[], byte[]>> bytePool;

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
        pool = ConnectionPoolSupport.createGenericObjectPool(ConnectionUtils.supplier(client), new GenericObjectPoolConfig<>());
        bytePool = ConnectionPoolSupport.createGenericObjectPool(ConnectionUtils.supplier(client, ByteArrayCodec.INSTANCE),
                new GenericObjectPoolConfig<>());
        connection = RedisModulesUtils.connection(client);
        commands = connection.sync();
        MapJobRepositoryFactoryBean bean = new MapJobRepositoryFactoryBean();
        bean.afterPropertiesSet();
        jobRepository = bean.getObject();
        transactionManager = bean.getTransactionManager();
        jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.afterPropertiesSet();
        asyncJobLauncher = new SimpleJobLauncher();
        asyncJobLauncher.setJobRepository(jobRepository);
        asyncJobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
        asyncJobLauncher.afterPropertiesSet();
        jobBuilderFactory = new JobBuilderFactory(jobRepository);
        stepBuilderFactory = new StepBuilderFactory(jobRepository, bean.getTransactionManager());
    }

    @AfterAll
    void teardown() {
        connection.close();
        bytePool.close();
        pool.close();
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

    protected <I, O> SimpleStepBuilder<I, O> step(TestInfo info, ItemReader<I> reader, ItemWriter<O> writer) {
        return step(info, reader, null, writer);
    }

    protected <I, O> SimpleStepBuilder<I, O> step(TestInfo info, ItemReader<I> reader, ItemProcessor<I, O> processor,
            ItemWriter<O> writer) {
        String name = name(info);
        if (reader instanceof ItemStreamSupport) {
            ((ItemStreamSupport) reader).setName(name + "-reader");
        }
        SimpleStepBuilder<I, O> step = stepBuilderFactory.get(name).chunk(DEFAULT_CHUNK_SIZE);
        step.reader(reader);
        step.processor(processor);
        step.writer(writer);
        return step;
    }

    public static String name(TestInfo info) {
        return info.getDisplayName().replace("(TestInfo)", "");
    }

    public static TestInfo testInfo(TestInfo info, String... suffixes) {
        return new SimpleTestInfo(info, suffixes);
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
        RedisItemWriter<String, String, Struct<String>> writer = structWriter(client);
        run(finalTestInfo, reader, writer);
    }

    protected RedisItemReader<String, String, Dump<String>> dumpReader(TestInfo info, AbstractRedisClient client) {
        return dumpReader(info, client, StringCodec.UTF8);
    }

    protected <K, V> RedisItemReader<K, V, Dump<K>> dumpReader(TestInfo info, AbstractRedisClient client,
            RedisCodec<K, V> codec) {
        return reader(info, RedisItemReader.dump(client, codec));
    }

    protected RedisItemWriter<String, String, Dump<String>> dumpWriter(AbstractRedisClient client) {
        return dumpWriter(client, StringCodec.UTF8);
    }

    protected RedisItemWriter<String, String, Struct<String>> structWriter(AbstractRedisClient client) {
        return structWriter(client, StringCodec.UTF8);
    }

    protected <K, V> RedisItemWriter<K, V, Dump<K>> dumpWriter(AbstractRedisClient client, RedisCodec<K, V> codec) {
        return RedisItemWriter.dump(client, codec);
    }

    protected <K, V> RedisItemWriter<K, V, Struct<K>> structWriter(AbstractRedisClient client, RedisCodec<K, V> codec) {
        return RedisItemWriter.struct(client, codec);
    }

    protected LiveRedisItemReader<String, String, Struct<String>> liveStructReader(TestInfo testInfo,
            AbstractRedisClient client) {
        return liveStructReader(testInfo, client, StringCodec.UTF8);
    }

    protected <K, V> LiveRedisItemReader<K, V, Struct<K>> liveStructReader(TestInfo info, AbstractRedisClient client,
            RedisCodec<K, V> codec) {
        LiveRedisItemReader<K, V, Struct<K>> reader = reader(info, LiveRedisItemReader.struct(client, codec));
        reader.setIdleTimeout(DEFAULT_IDLE_TIMEOUT);
        return reader;
    }

    protected LiveRedisItemReader<String, String, Dump<String>> liveDumpReader(TestInfo testInfo, AbstractRedisClient client) {
        return liveDumpReader(testInfo, client, StringCodec.UTF8);
    }

    protected <K, V> LiveRedisItemReader<K, V, Dump<K>> liveDumpReader(TestInfo info, AbstractRedisClient client,
            RedisCodec<K, V> codec) {
        LiveRedisItemReader<K, V, Dump<K>> reader = reader(info, LiveRedisItemReader.dump(client, codec));
        reader.setIdleTimeout(DEFAULT_IDLE_TIMEOUT);
        return reader;
    }

    protected RedisItemReader<String, String, Struct<String>> structReader(TestInfo testInfo, AbstractRedisClient client) {
        return structReader(testInfo, client, StringCodec.UTF8);
    }

    protected <K, V> RedisItemReader<K, V, Struct<K>> structReader(TestInfo info, AbstractRedisClient client,
            RedisCodec<K, V> codec) {
        return reader(info, RedisItemReader.struct(client, codec));
    }

    @SuppressWarnings("rawtypes")
    protected <R extends AbstractRedisItemReader> R reader(TestInfo info, R reader) {
        reader.setJobRepository(jobRepository);
        reader.setTransactionManager(transactionManager);
        reader.setName(name(info));
        return reader;
    }

    protected void flushAll(AbstractRedisClient client) {
        try (StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils.connection(client)) {
            connection.sync().flushall();
            awaitUntil(() -> connection.sync().dbsize() == 0);
        }
    }

    protected <I, O> JobExecution run(TestInfo info, ItemReader<I> reader, ItemWriter<O> writer) throws JobExecutionException {
        return run(info, reader, null, writer);
    }

    protected <I, O> JobExecution run(TestInfo testInfo, ItemReader<I> reader, ItemProcessor<I, O> processor,
            ItemWriter<O> writer) throws JobExecutionException {
        SimpleStepBuilder<I, O> step = step(testInfo, reader, processor, writer);
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

    protected void enableKeyspaceNotifications(AbstractRedisClient client) {
        RedisModulesUtils.connection(client).sync().configSet("notify-keyspace-events", "AK");
    }

    protected <I, O> FlushingStepBuilder<I, O> flushingStep(TestInfo info, PollableItemReader<I> reader, ItemWriter<O> writer) {
        return new FlushingStepBuilder<>(step(info, reader, writer)).idleTimeout(DEFAULT_IDLE_TIMEOUT);
    }

    protected void generateStreams(TestInfo testInfo, int messageCount) throws JobExecutionException {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setTypes(Struct.Type.STREAM);
        gen.setMaxItemCount(3);
        StreamOptions streamOptions = new StreamOptions();
        streamOptions.setMessageCount(Range.of(messageCount));
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
