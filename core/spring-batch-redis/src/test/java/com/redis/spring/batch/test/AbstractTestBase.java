package com.redis.spring.batch.test;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.retry.policy.MaxAttemptsRetryPolicy;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.Assert;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.OperationValueReader;
import com.redis.spring.batch.common.Range;
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
import com.redis.spring.batch.writer.WriteOperation;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.support.ConnectionPoolSupport;

@TestInstance(Lifecycle.PER_CLASS)
public abstract class AbstractTestBase {

    public static final DataType[] REDIS_GENERATOR_TYPES = { DataType.HASH, DataType.LIST, DataType.SET, DataType.STREAM,
            DataType.STRING, DataType.ZSET };

    public static final DataType[] REDIS_MODULES_GENERATOR_TYPES = { DataType.HASH, DataType.LIST, DataType.SET,
            DataType.STREAM, DataType.STRING, DataType.ZSET, DataType.TIMESERIES, DataType.JSON };

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

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected static final int DEFAULT_CHUNK_SIZE = 50;

    private static final Duration DEFAULT_AWAIT_TIMEOUT = Duration.ofMillis(1000);

    protected static final Duration DEFAULT_RUNNING_DELAY = Duration.ofMillis(100);

    protected static final Duration DEFAULT_IDLE_TIMEOUT = Duration.ofMillis(500);

    private static final Duration DEFAULT_POLL_INTERVAL = Duration.ofMillis(1);

    private static final Duration DEFAULT_POLL_DELAY = Duration.ZERO;

    protected static final int DEFAULT_GENERATOR_COUNT = 100;

    private Duration pollDelay = DEFAULT_POLL_DELAY;

    private Duration pollInterval = DEFAULT_POLL_INTERVAL;

    private Duration awaitTimeout = DEFAULT_AWAIT_TIMEOUT;

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

    private SimpleJobLauncher jobLauncher;

    private JobBuilderFactory jobBuilderFactory;

    private StepBuilderFactory stepBuilderFactory;

    protected abstract RedisServer getRedisServer();

    @BeforeAll
    void setup() throws Exception {
        // Source Redis setup
        getRedisServer().start();
        client = client(getRedisServer());
        pool = ConnectionPoolSupport.createGenericObjectPool(ConnectionUtils.supplier(client), new GenericObjectPoolConfig<>());
        connection = RedisModulesUtils.connection(client);
        commands = connection.sync();

        // Job infra setup
        @SuppressWarnings("deprecation")
        org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean bean = new org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean();
        bean.afterPropertiesSet();
        jobRepository = bean.getObject();
        transactionManager = bean.getTransactionManager();
        jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setTaskExecutor(new SyncTaskExecutor());
        jobLauncher.afterPropertiesSet();
        jobBuilderFactory = new JobBuilderFactory(jobRepository);
        stepBuilderFactory = new StepBuilderFactory(jobRepository, bean.getTransactionManager());
    }

    @AfterAll
    void teardown() {
        connection.close();
        pool.close();
        client.shutdown();
        client.getResources().shutdown();
        getRedisServer().close();
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
        return generator(DEFAULT_GENERATOR_COUNT, types);
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
        return step(info, DEFAULT_CHUNK_SIZE, reader, processor, writer);
    }

    protected <I, O> SimpleStepBuilder<I, O> step(TestInfo info, int chunkSize, ItemReader<I> reader,
            ItemProcessor<I, O> processor, ItemWriter<O> writer) {
        String name = name(info);
        if (reader instanceof ItemStreamSupport) {
            ((ItemStreamSupport) reader).setName(name + "-reader");
        }
        SimpleStepBuilder<I, O> step = stepBuilderFactory.get(name).chunk(chunkSize);
        step.reader(reader);
        step.processor(processor);
        step.writer(writer);
        return step;
    }

    public static String name(TestInfo info) {
        return info.getDisplayName().replace("(TestInfo)", "");
    }

    protected AbstractRedisClient client(RedisServer server) {
        if (server.isCluster()) {
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
        Awaitility.await().pollDelay(pollDelay).pollInterval(pollInterval).timeout(awaitTimeout).until(evaluator);
    }

    protected JobBuilder job(TestInfo info) {
        return jobBuilderFactory.get(name(info));
    }

    protected GeneratorItemReader generator() {
        return generator(DEFAULT_GENERATOR_COUNT);
    }

    protected void generate(TestInfo info) throws JobExecutionException {
        generate(info, generator(DEFAULT_GENERATOR_COUNT));
    }

    protected void generate(TestInfo info, GeneratorItemReader reader) throws JobExecutionException {
        generate(info, client, reader);
    }

    protected void generate(TestInfo info, AbstractRedisClient client, GeneratorItemReader reader)
            throws JobExecutionException {
        TestInfo finalTestInfo = testInfo(info, "generate", String.valueOf(client.hashCode()));
        StructItemWriter<String, String> writer = RedisItemWriter.struct(client, StringCodec.UTF8);
        run(finalTestInfo, reader, writer);
        awaitUntilFalse(reader::isOpen);
        awaitUntilFalse(writer::isOpen);
    }

    protected void configureReader(TestInfo info, RedisItemReader<?, ?, ?> reader) {
        reader.setName(name(info));
        reader.setIdleTimeout(DEFAULT_IDLE_TIMEOUT);
        reader.setJobRepository(jobRepository);
        reader.setTransactionManager(transactionManager);
    }

    protected void flushAll(AbstractRedisClient client) {
        try (StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils.connection(client)) {
            commands.flushall();
            awaitUntil(() -> commands.dbsize() == 0);
        }
    }

    protected <I, O> JobExecution run(TestInfo info, ItemReader<I> reader, ItemWriter<O> writer) throws JobExecutionException {
        return run(info, reader, null, writer);
    }

    protected <I, O> JobExecution run(TestInfo info, ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer)
            throws JobExecutionException {
        return run(info, step(info, reader, processor, writer));
    }

    protected <I, O> JobExecution run(TestInfo info, SimpleStepBuilder<I, O> step) throws JobExecutionException {
        SimpleJobBuilder job = job(info).start(faultTolerant(step).build());
        return run(job.build());
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

    protected <I, O> FlushingStepBuilder<I, O> flushingStep(TestInfo info, PollableItemReader<I> reader, ItemWriter<O> writer) {
        return new FlushingStepBuilder<>(step(info, reader, writer)).idleTimeout(DEFAULT_IDLE_TIMEOUT);
    }

    protected void generateStreams(TestInfo info, int messageCount) throws JobExecutionException {
        GeneratorItemReader gen = generator(3, DataType.STREAM);
        StreamOptions streamOptions = new StreamOptions();
        streamOptions.setMessageCount(Range.of(messageCount));
        gen.setStreamOptions(streamOptions);
        generate(testInfo(info, "streams"), gen);
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

    protected OperationValueReader<byte[], byte[], byte[], KeyValue<byte[]>> dumpOperationExecutor() {
        DumpItemReader reader = RedisItemReader.dump(client);
        OperationValueReader<byte[], byte[], byte[], KeyValue<byte[]>> executor = reader.operationValueReader();
        executor.open(new ExecutionContext());
        return executor;
    }

    protected OperationValueReader<String, String, String, KeyValue<String>> structOperationExecutor() {
        return structOperationExecutor(StringCodec.UTF8);
    }

    protected <K, V> OperationValueReader<K, V, K, KeyValue<K>> structOperationExecutor(RedisCodec<K, V> codec) {
        StructItemReader<K, V> reader = RedisItemReader.struct(client, codec);
        OperationValueReader<K, V, K, KeyValue<K>> executor = reader.operationValueReader();
        executor.open(new ExecutionContext());
        return executor;
    }

    protected <T> OperationItemWriter<String, String, T> writer(WriteOperation<String, String, T> operation) {
        return RedisItemWriter.operation(client, StringCodec.UTF8, operation);
    }

}
