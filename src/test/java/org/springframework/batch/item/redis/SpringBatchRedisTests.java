package org.springframework.batch.item.redis;

import com.redislabs.testcontainers.RedisContainer;
import io.lettuce.core.*;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import io.lettuce.core.api.sync.*;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.support.ConnectionPoolSupport;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.search.Search;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.runner.RunWith;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.DefaultBufferedReaderFactory;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.*;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.BufferedReader;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

@Testcontainers
@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
@SuppressWarnings({"rawtypes", "unchecked", "unused", "BusyWait", "SingleStatementInBlock"})
@Slf4j
public class SpringBatchRedisTests {

    @Container
    private static final RedisContainer REDIS = new RedisContainer().withKeyspaceNotifications();
    @Container
    private static final RedisContainer REDIS_CLUSTER = new RedisContainer().withClusterKeyspaceNotifications();
    @Container
    private static final RedisContainer REDIS_REPLICA = new RedisContainer();

    @AfterEach
    public void cleanupEach() {
        RedisServerCommands<String, String> sync = sync(REDIS);
        sync.flushall();
        sync = sync(REDIS_CLUSTER);
        sync.flushall();
        sync = sync(REDIS_REPLICA);
        sync.flushall();
    }

    static Stream<RedisContainer> sourceContainers() {
        return Stream.of(REDIS, REDIS_CLUSTER);
    }

    @Autowired
    private JobLauncher jobLauncher;
    @Autowired
    private JobLauncher asyncJobLauncher;
    @Autowired
    private JobBuilderFactory jobs;
    @Autowired
    private StepBuilderFactory steps;

    private Job job(RedisContainer redisContainer, String name, TaskletStep step) {
        return jobs.get(name(redisContainer, name) + "-job").start(step).build();
    }

    private <I, O> JobExecution execute(RedisContainer redisContainer, String name, ItemReader<? extends I> reader, ItemWriter<O> writer) throws Throwable {
        return execute(redisContainer, name, step(name, reader, writer).build());
    }

    private JobExecution execute(RedisContainer redisContainer, String name, TaskletStep step) throws Exception {
        return checkForFailure(jobLauncher.run(job(redisContainer, name, step), new JobParameters()));
    }

    private JobExecution checkForFailure(JobExecution execution) {
        if (!execution.getExitStatus().getExitCode().equals(ExitStatus.COMPLETED.getExitCode())) {
            Assertions.fail("Job not completed: " + execution.getExitStatus());
        }
        return execution;
    }

    private <I, O> JobExecution executeFlushing(RedisContainer redisContainer, String name, PollableItemReader<? extends I> reader, ItemWriter<O> writer) throws Throwable {
        TaskletStep step = flushing(step(name, reader, writer)).build();
        JobExecution execution = asyncJobLauncher.run(job(redisContainer, name, step), new JobParameters());
        awaitRunning(execution);
        Thread.sleep(100);
        return execution;
    }

    private <I, O> SimpleStepBuilder<I, O> step(String name, ItemReader<? extends I> reader, ItemWriter<O> writer) {
        return steps.get(name + "-step").<I, O>chunk(50).reader(reader).writer(writer);
    }

    private <I, O> FlushingStepBuilder<I, O> flushing(SimpleStepBuilder<I, O> step) {
        return new FlushingStepBuilder<>(step).idleTimeout(Duration.ofMillis(500));
    }

    private FlatFileItemReader<Map<String, String>> fileReader(Resource resource) throws IOException {
        FlatFileItemReaderBuilder<Map<String, String>> builder = new FlatFileItemReaderBuilder<>();
        builder.name("flat-file-reader");
        builder.resource(resource);
        builder.saveState(false);
        builder.linesToSkip(1);
        builder.fieldSetMapper(new MapFieldSetMapper());
        builder.recordSeparatorPolicy(new DefaultRecordSeparatorPolicy());
        FlatFileItemReaderBuilder.DelimitedBuilder<Map<String, String>> delimitedBuilder = builder.delimited();
        BufferedReader reader = new DefaultBufferedReaderFactory().create(resource, FlatFileItemReader.DEFAULT_CHARSET);
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setDelimiter(DelimitedLineTokenizer.DELIMITER_COMMA);
        String[] fieldNames = tokenizer.tokenize(reader.readLine()).getValues();
        delimitedBuilder.names(fieldNames);
        return builder.build();
    }

    @ParameterizedTest
    @MethodSource("sourceContainers")
    void testFlushingStep(RedisContainer redisContainer) throws Throwable {
        try (GenericObjectPool<StatefulConnection<String, String>> pool = pool(redisContainer); StatefulRedisPubSubConnection<String, String> pubSubConnection = pubSubConnection(redisContainer)) {
            PollableItemReader<String> reader = keyspaceNotificationItemReader(pool, pubSubConnection);
            ListItemWriter<String> writer = new ListItemWriter<>();
            JobExecution execution = executeFlushing(redisContainer, "flushing", reader, writer);
            DataGenerator.builder(redisContainer).end(3).maxExpire(0).dataType(DataType.STRING).dataType(DataType.HASH).build().call();
            awaitJobTermination(execution);
            RedisServerCommands<String, String> commands = sync(redisContainer);
            Assertions.assertEquals(commands.dbsize(), writer.getWrittenItems().size());
        }
    }

    private StatefulConnection<String, String> connection(RedisContainer container) {
        if (container.isCluster()) {
            return RedisClusterClient.create(container.getRedisUri()).connect();
        }
        return RedisClient.create(container.getRedisUri()).connect();
    }

    private <T> T sync(RedisContainer container) {
        if (container.isCluster()) {
            return (T) RedisClusterClient.create(container.getRedisUri()).connect().sync();
        }
        return (T) RedisClient.create(container.getRedisUri()).connect().sync();
    }

    private PollableItemReader<String> keyspaceNotificationItemReader(GenericObjectPool<StatefulConnection<String, String>> pool, StatefulRedisPubSubConnection<String, String> pubSubConnection) {
        if (pubSubConnection instanceof StatefulRedisClusterPubSubConnection) {
            return (PollableItemReader<String>) RedisClusterDataStructureItemReader.builder((GenericObjectPool) pool, (StatefulRedisClusterPubSubConnection<String, String>) pubSubConnection).build().getKeyReader();
        }
        return (PollableItemReader<String>) RedisDataStructureItemReader.builder((GenericObjectPool) pool, pubSubConnection).build().getKeyReader();
    }

    private static GenericObjectPool<StatefulConnection<String, String>> pool(RedisContainer container) {
        if (container.isCluster()) {
            RedisClusterClient client = RedisClusterClient.create(container.getRedisUri());
            return ConnectionPoolSupport.createGenericObjectPool(((RedisClusterClient) client)::connect, new GenericObjectPoolConfig<>());
        }
        RedisClient client = RedisClient.create(container.getRedisUri());
        return ConnectionPoolSupport.createGenericObjectPool(((RedisClient) client)::connect, new GenericObjectPoolConfig<>());
    }

    private static StatefulRedisPubSubConnection<String, String> pubSubConnection(RedisContainer container) {
        if (container.isCluster()) {
            return RedisClusterClient.create(container.getRedisUri()).connectPubSub();
        }
        return RedisClient.create(container.getRedisUri()).connectPubSub();
    }

    private void awaitJobTermination(JobExecution execution) throws Throwable {
        while (execution.isRunning()) {
            Thread.sleep(10);
        }
        checkForFailure(execution);
    }

    @ParameterizedTest
    @MethodSource("sourceContainers")
    void testDataStructureReader(RedisContainer redisContainer) throws Throwable {
        populateSource("scan-reader-populate", redisContainer);
        try (GenericObjectPool<StatefulConnection<String, String>> pool = pool(redisContainer); StatefulConnection<String, String> connection = connection(redisContainer)) {
            DataStructureItemReader<String, String, ?> reader = dataStructureReader(pool, connection);
            ListItemWriter<DataStructure<String>> writer = new ListItemWriter<>();
            JobExecution execution = execute(redisContainer, "scan-reader", reader, writer);
            Assertions.assertTrue(execution.getAllFailureExceptions().isEmpty());
            RedisServerCommands<String, String> sync = sync(redisContainer);
            Assertions.assertEquals(sync.dbsize(), writer.getWrittenItems().size());
        }
    }

    private void populateSource(String name, RedisContainer redisContainer) throws Throwable {
        FlatFileItemReader<Map<String, String>> fileReader = fileReader(new ClassPathResource("beers.csv"));
        try (StatefulConnection<String, String> connection = connection(redisContainer)) {
            Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> asyncFunction = asyncFunction(redisContainer);
            ItemWriter<Map<String, String>> hsetWriter = items -> {
                BaseRedisAsyncCommands<String, String> async = asyncFunction.apply(connection);
                async.setAutoFlushCommands(false);
                List<RedisFuture<?>> futures = new ArrayList<>();
                for (Map<String, String> item : items) {
                    futures.add(((RedisHashAsyncCommands<String, String>) async).hset(item.get(Beers.FIELD_ID), item));
                }
                async.flushCommands();
                LettuceFutures.awaitAll(RedisURI.DEFAULT_TIMEOUT_DURATION, futures.toArray(new RedisFuture[0]));
                async.setAutoFlushCommands(true);
            };
            execute(redisContainer, name, fileReader, hsetWriter);
        }
    }

    private static DataStructureItemReader<String, String, ?> dataStructureReader(GenericObjectPool<StatefulConnection<String, String>> pool, StatefulConnection<String, String> connection) {
        if (connection instanceof StatefulRedisClusterConnection) {
            return RedisClusterDataStructureItemReader.builder((GenericObjectPool) pool, (StatefulRedisClusterConnection<String, String>) connection).build();
        }
        return RedisDataStructureItemReader.builder((GenericObjectPool) pool, (StatefulRedisConnection<String, String>) connection).build();
    }

    @ParameterizedTest
    @MethodSource("sourceContainers")
    void testStreamReader(RedisContainer redisContainer) throws Throwable {
        try (GenericObjectPool<StatefulConnection<String, String>> pool = pool(redisContainer); StatefulConnection<String, String> connection = connection(redisContainer)) {
            DataGenerator.builder(redisContainer).dataType(DataType.STREAM).end(100).build().call();
            StreamItemReader<String, String, ?> reader = streamReaderBuilder(connection).offset(StreamOffset.from("stream:0", "0-0")).build();
            ListItemWriter<StreamMessage<String, String>> writer = new ListItemWriter<>();
            JobExecution execution = executeFlushing(redisContainer, "stream-reader", reader, writer);
            awaitJobTermination(execution);
            Assertions.assertEquals(10, writer.getWrittenItems().size());
            List<? extends StreamMessage<String, String>> items = writer.getWrittenItems();
            for (StreamMessage<String, String> message : items) {
                Assertions.assertTrue(message.getBody().containsKey("field1"));
                Assertions.assertTrue(message.getBody().containsKey("field2"));
            }
        }
    }

    private StreamItemReader.StreamItemReaderBuilder<String, String, ?, ?> streamReaderBuilder(StatefulConnection<String, String> connection) {
        if (connection instanceof StatefulRedisClusterConnection) {
            return RedisClusterStreamItemReader.builder((StatefulRedisClusterConnection<String, String>) connection);
        }
        return RedisStreamItemReader.builder((StatefulRedisConnection<String, String>) connection);
    }

    @ParameterizedTest
    @MethodSource("sourceContainers")
    void testMultiThreadedReader(RedisContainer redisContainer) throws Throwable {
        populateSource("multithreaded-scan-reader-populate", redisContainer);
        try (GenericObjectPool<StatefulConnection<String, String>> pool = pool(redisContainer); StatefulConnection<String, String> connection = connection(redisContainer)) {
            SynchronizedItemStreamReader<DataStructure<String>> synchronizedReader = new SynchronizedItemStreamReader<>();
            synchronizedReader.setDelegate(dataStructureReader(pool, connection));
            synchronizedReader.afterPropertiesSet();
            ListItemWriter<DataStructure<String>> writer = new ListItemWriter<>();
            SynchronizedItemWriter<DataStructure<String>> synchronizedWriter = new SynchronizedItemWriter<>(writer);
            String name = "multithreaded-scan-reader";
            int threads = 4;
            ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
            taskExecutor.setMaxPoolSize(threads);
            taskExecutor.setCorePoolSize(threads);
            taskExecutor.afterPropertiesSet();
            JobExecution execution = execute(redisContainer, name, step(name, synchronizedReader, synchronizedWriter).taskExecutor(taskExecutor).throttleLimit(threads).build());
            Assertions.assertTrue(execution.getAllFailureExceptions().isEmpty());
            RedisServerCommands<String, String> sync = sync(redisContainer);
            Assertions.assertEquals(sync.dbsize(), writer.getWrittenItems().size());
        }
    }

    @ParameterizedTest
    @MethodSource("sourceContainers")
    void testStreamWriter(RedisContainer redisContainer) throws Throwable {
        String stream = "stream:0";
        List<Map<String, String>> messages = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            Map<String, String> body = new HashMap<>();
            body.put("field1", "value1");
            body.put("field2", "value2");
            messages.add(body);
        }
        ListItemReader<Map<String, String>> reader = new ListItemReader<>(messages);
        try (GenericObjectPool<StatefulConnection<String, String>> pool = pool(redisContainer)) {
            RedisOperation<String, String, Map<String, String>> xadd = RedisOperationBuilder.<String, String, Map<String, String>>xadd().keyConverter(i -> stream).bodyConverter(i -> i).build();
            OperationItemWriter<String, String, ?, Map<String, String>> writer = operationWriter(redisContainer, pool, xadd);
            execute(redisContainer, "stream-writer", reader, writer);
            RedisStreamCommands<String, String> sync = sync(redisContainer);
            Assertions.assertEquals(messages.size(), sync.xlen(stream));
            List<StreamMessage<String, String>> xrange = sync.xrange(stream, Range.create("-", "+"));
            for (int index = 0; index < xrange.size(); index++) {
                StreamMessage<String, String> message = xrange.get(index);
                Assertions.assertEquals(messages.get(index), message.getBody());
            }
        }
    }

    private String name(RedisContainer redisContainer, String name) {
        if (redisContainer.isCluster()) {
            return "cluster-" + name;
        }
        return name;
    }

    private static <T> OperationItemWriter<String, String, ?, T> operationWriter(RedisContainer redisContainer, GenericObjectPool<StatefulConnection<String, String>> pool, RedisOperation<String, String, T> operation) {
        if (redisContainer.isCluster()) {
            return new RedisClusterOperationItemWriter<>((GenericObjectPool) pool, operation);
        }
        return new RedisOperationItemWriter<>((GenericObjectPool) pool, operation);
    }

    private void awaitRunning(JobExecution execution) throws InterruptedException {
        while (!execution.isRunning()) {
            Thread.sleep(10);
        }
    }

    @Test
    public void testStreamTransactionWriter() throws Throwable {
        String stream = "stream:1";
        List<Map<String, String>> messages = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            Map<String, String> body = new HashMap<>();
            body.put("field1", "value1");
            body.put("field2", "value2");
            messages.add(body);
        }
        ListItemReader<Map<String, String>> reader = new ListItemReader<>(messages);
        RedisOperation<String, String, Map<String, String>> xadd = RedisOperationBuilder.<String, String, Map<String, String>>xadd().keyConverter(i -> stream).bodyConverter(i -> i).build();
        try (GenericObjectPool<StatefulConnection<String, String>> pool = pool(REDIS)) {
            RedisTransactionItemWriter<String, String, Map<String, String>> writer = new RedisTransactionItemWriter<>((GenericObjectPool) pool, xadd);
            execute(REDIS, "stream-tx-writer", reader, writer);
            RedisStreamCommands<String, String> sync = sync(REDIS);
            Assertions.assertEquals(messages.size(), sync.xlen(stream));
            List<StreamMessage<String, String>> xrange = sync.xrange(stream, Range.create("-", "+"));
            for (int index = 0; index < xrange.size(); index++) {
                StreamMessage<String, String> message = xrange.get(index);
                Assertions.assertEquals(messages.get(index), message.getBody());
            }
        }
    }

    private Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> asyncFunction(RedisContainer container) {
        if (container.isCluster()) {
            return c -> ((StatefulRedisClusterConnection<String, String>) c).async();
        }
        return c -> ((StatefulRedisConnection<String, String>) c).async();
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("sourceContainers")
    public void testHashWriter(RedisContainer redisContainer) throws Throwable {
        List<Map<String, String>> maps = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            Map<String, String> body = new HashMap<>();
            body.put("id", String.valueOf(index));
            body.put("field1", "value1");
            body.put("field2", "value2");
            maps.add(body);
        }
        ListItemReader<Map<String, String>> reader = new ListItemReader<>(maps);
        KeyMaker<Map<String, String>> keyConverter = KeyMaker.<Map<String, String>>builder().prefix("hash").converters(h -> h.remove("id")).build();
        RedisOperation<String, String, Map<String, String>> hset = RedisOperationBuilder.<String, String, Map<String, String>>hset().keyConverter(keyConverter).mapConverter(m -> m).build();
        try (GenericObjectPool<StatefulConnection<String, String>> pool = pool(redisContainer)) {
            OperationItemWriter<String, String, ?, Map<String, String>> writer = new OperationItemWriter<>(pool, asyncFunction(redisContainer), hset);
            execute(redisContainer, "hash-writer", reader, writer);
            RedisKeyCommands<String, String> sync = sync(redisContainer);
            Assertions.assertEquals(maps.size(), sync.keys("hash:*").size());
            RedisHashCommands<String, String> hashCommands = sync(redisContainer);
            for (int index = 0; index < maps.size(); index++) {
                Map<String, String> hash = hashCommands.hgetall("hash:" + index);
                Assertions.assertEquals(maps.get(index), hash);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("sourceContainers")
    public void testSortedSetWriter(RedisContainer redisContainer) throws Throwable {
        List<ScoredValue<String>> values = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            values.add((ScoredValue<String>) ScoredValue.fromNullable(index % 10, String.valueOf(index)));
        }
        ListItemReader<ScoredValue<String>> reader = new ListItemReader<>(values);
        KeyMaker<ScoredValue<String>> keyConverter = KeyMaker.<ScoredValue<String>>builder().prefix("zset").build();
        RedisOperation<String, String, ScoredValue<String>> zadd = RedisOperationBuilder.<String, String, ScoredValue<String>>zadd().keyConverter(keyConverter).memberIdConverter(Value::getValue).scoreConverter(ScoredValue::getScore).build();
        try (GenericObjectPool<StatefulConnection<String, String>> pool = pool(redisContainer)) {
            OperationItemWriter<String, String, ?, ScoredValue<String>> writer = new OperationItemWriter<>(pool, asyncFunction(redisContainer), zadd);
            execute(redisContainer, "sorted-set-writer", reader, writer);
            RedisServerCommands<String, String> sync = sync(redisContainer);
            Assertions.assertEquals(1, sync.dbsize());
            Assertions.assertEquals(values.size(), ((RedisSortedSetCommands<String, String>) sync).zcard("zset"));
            List<String> range = ((RedisSortedSetCommands<String, String>) sync).zrangebyscore("zset", Range.from(Range.Boundary.including(0), Range.Boundary.including(5)));
            Assertions.assertEquals(60, range.size());
        }
    }

    @ParameterizedTest
    @MethodSource("sourceContainers")
    public void testDataStructureWriter(RedisContainer redisContainer) throws Throwable {
        List<DataStructure<String>> list = new ArrayList<>();
        long count = 100;
        for (int index = 0; index < count; index++) {
            DataStructure keyValue = new DataStructure();
            keyValue.setKey("hash:" + index);
            keyValue.setType(DataType.HASH);
            Map<String, String> map = new HashMap<>();
            map.put("field1", "value1");
            map.put("field2", "value2");
            keyValue.setValue(map);
            list.add(keyValue);
        }
        ListItemReader<DataStructure<String>> reader = new ListItemReader<>(list);
        try (GenericObjectPool<StatefulConnection<String, String>> pool = pool(redisContainer)) {
            DataStructureItemWriter<String, String, ?> writer = new DataStructureItemWriter(pool, asyncFunction(redisContainer));
            execute(redisContainer, "value-writer", reader, writer);
            RedisKeyCommands<String, String> sync = sync(redisContainer);
            List<String> keys = sync.keys("hash:*");
            Assertions.assertEquals(count, keys.size());
        }
    }


    @ParameterizedTest
    @MethodSource("sourceContainers")
    public void testLiveReader(RedisContainer redisContainer) throws Throwable {
        KeyDumpItemReader<String, String, ?> reader = liveKeyDumpReader(redisContainer).idleTimeout(Duration.ofMillis(500)).build();
        ListItemWriter<KeyValue<String, byte[]>> writer = new ListItemWriter<>();
        JobExecution execution = executeFlushing(redisContainer, "live-reader", reader, writer);
        DataGenerator.builder(redisContainer).end(123).maxExpire(0).dataType(DataType.STRING).dataType(DataType.HASH).build().call();
        awaitJobTermination(execution);
        RedisServerCommands<String, String> sync = sync(redisContainer);
        Assertions.assertEquals(sync.dbsize(), writer.getWrittenItems().size());
    }

    private AbstractLiveKeyValueItemReaderBuilder<KeyDumpItemReader<String, String, ?>, ?> liveKeyDumpReader(RedisContainer redisContainer) {
        GenericObjectPool<StatefulConnection<String, String>> pool = pool(redisContainer);
        StatefulRedisPubSubConnection<String, String> pubSubConnection = pubSubConnection(redisContainer);
        if (redisContainer.isCluster()) {
            return (AbstractLiveKeyValueItemReaderBuilder) RedisClusterKeyDumpItemReader.builder((GenericObjectPool) pool, (StatefulRedisClusterPubSubConnection<String, String>) pubSubConnection);
        }
        return (AbstractLiveKeyValueItemReaderBuilder) RedisKeyDumpItemReader.builder((GenericObjectPool) pool, pubSubConnection);
    }

    private AbstractScanKeyValueItemReaderBuilder<KeyDumpItemReader<String, String, ?>, ?> keyDumpReader(RedisContainer redisContainer) {
        GenericObjectPool<StatefulConnection<String, String>> pool = pool(redisContainer);
        StatefulConnection<String, String> connection = connection(redisContainer);
        if (redisContainer.isCluster()) {
            return (AbstractScanKeyValueItemReaderBuilder) RedisClusterKeyDumpItemReader.builder((GenericObjectPool) pool, (StatefulRedisClusterConnection<String, String>) connection);
        }
        return (AbstractScanKeyValueItemReaderBuilder) RedisKeyDumpItemReader.builder((GenericObjectPool) pool, (StatefulRedisConnection<String, String>) connection);
    }


    private AbstractScanKeyValueItemReaderBuilder<DataStructureItemReader<String, String, ?>, ?> dataStructureReader(RedisContainer redisContainer) {
        GenericObjectPool<StatefulConnection<String, String>> pool = pool(redisContainer);
        StatefulConnection<String, String> connection = connection(redisContainer);
        if (redisContainer.isCluster()) {
            return (AbstractScanKeyValueItemReaderBuilder) RedisClusterDataStructureItemReader.builder((GenericObjectPool) pool, (StatefulRedisClusterConnection<String, String>) connection);
        }
        return (AbstractScanKeyValueItemReaderBuilder) RedisDataStructureItemReader.builder((GenericObjectPool) pool, (StatefulRedisConnection<String, String>) connection);
    }

    @ParameterizedTest
    @MethodSource("sourceContainers")
    public void testDataStructureReplication(RedisContainer redisContainer) throws Throwable {
        DataGenerator.builder(redisContainer).end(10000).build().call();
        DataStructureItemReader<String, String, ?> reader = dataStructureReader(redisContainer).build();
        DataStructureItemWriter<String, String, ?> writer = new DataStructureItemWriter(pool(REDIS_REPLICA), asyncFunction(REDIS_REPLICA));
        execute(redisContainer, "ds-replication", reader, writer);
        compare(redisContainer, "ds-replication");
    }

    @ParameterizedTest
    @MethodSource("sourceContainers")
    public void testReplication(RedisContainer redisContainer) throws Throwable {
        DataGenerator.builder(redisContainer).end(10000).build().call();
        KeyDumpItemReader<String, String, ?> reader = keyDumpReader(redisContainer).build();
        OperationItemWriter<String, String, ?, KeyValue<String, byte[]>> writer = keyDumpWriter(REDIS_REPLICA);
        execute(redisContainer, "replication", reader, writer);
        compare(redisContainer, "replication");
    }

    @ParameterizedTest
    @MethodSource("sourceContainers")
    public void testLiveReplication(RedisContainer redisContainer) throws Throwable {
        DataGenerator.builder(redisContainer).end(10000).build().call();
        KeyDumpItemReader<String, String, ?> reader = keyDumpReader(redisContainer).build();
        reader.setName("reader");
        OperationItemWriter<String, String, ?, KeyValue<String, byte[]>> writer = keyDumpWriter(REDIS_REPLICA);
        writer.setName("writer");
        TaskletStep replicationStep = step("replication", reader, writer).build();
        KeyDumpItemReader<String, String, ?> liveReader = liveKeyDumpReader(redisContainer).idleTimeout(Duration.ofMillis(500)).build();
        liveReader.setName("live-reader");
        OperationItemWriter<String, String, ?, KeyValue<String, byte[]>> liveWriter = keyDumpWriter(REDIS_REPLICA);
        liveWriter.setName("live-writer");
        TaskletStep liveReplicationStep = flushing(step("live-replication", liveReader, liveWriter)).build();
        SimpleFlow replicationFlow = new FlowBuilder<SimpleFlow>("replication-flow").start(replicationStep).build();
        SimpleFlow liveReplicationFlow = new FlowBuilder<SimpleFlow>("live-replication-flow").start(liveReplicationStep).build();
        Job job = jobs.get(name(redisContainer, "live-replication-job")).start(new FlowBuilder<SimpleFlow>("live-replication-flow").split(new SimpleAsyncTaskExecutor()).add(replicationFlow, liveReplicationFlow).build()).build().build();
        JobExecution execution = asyncJobLauncher.run(job, new JobParameters());
        awaitRunning(execution);
        DataGenerator.builder(redisContainer).end(123).build().call();
        awaitJobTermination(execution);
        compare(redisContainer, "live-replication");
    }

    private OperationItemWriter<String, String, ?, KeyValue<String, byte[]>> keyDumpWriter(RedisContainer container) {
        if (container.isCluster()) {
            return new RedisClusterKeyDumpItemWriter<>((GenericObjectPool) pool(container));
        }
        return new RedisKeyDumpItemWriter<>((GenericObjectPool) pool(container));
    }


    private void compare(RedisContainer redisContainer, String name) throws Throwable {
        RedisServerCommands<String, String> sourceSync = sync(redisContainer);
        RedisServerCommands<String, String> targetSync = sync(REDIS_REPLICA);
        Assertions.assertEquals(sourceSync.dbsize(), targetSync.dbsize());
        DataStructureItemReader<String, String, ?> left = dataStructureReader(redisContainer).build();
        DataStructureItemReader<String, String, ?> right = dataStructureReader(REDIS_REPLICA).build();
        KeyComparisonItemWriter<String, String> writer = new KeyComparisonItemWriter<>(right, Duration.ofSeconds(1));
        execute(redisContainer, name + "-compare", left, writer);
        Assertions.assertEquals(sourceSync.dbsize(), writer.getResults().getOk());
        Assertions.assertFalse(writer.getResults().hasDiffs());
        Assertions.assertTrue(writer.getResults().isOk());
    }

    @ParameterizedTest
    @MethodSource("sourceContainers")
    public void testMetrics(RedisContainer redisContainer) throws Throwable {
        Metrics.globalRegistry.getMeters().forEach(Metrics.globalRegistry::remove);
        SimpleMeterRegistry registry = new SimpleMeterRegistry(new SimpleConfig() {
            @Override
            public String get(String key) {
                return null;
            }

            @Override
            public Duration step() {
                return Duration.ofMillis(1);
            }
        }, Clock.SYSTEM);
        Metrics.addRegistry(registry);
        DataGenerator.builder(redisContainer).end(100).build().call();
        DataStructureItemReader<String, String, ?> reader = dataStructureReader(redisContainer).queueCapacity(10).chunkSize(1).build();
        ItemWriter<DataStructure<String>> writer = items -> Thread.sleep(1);
        TaskletStep step = steps.get("metrics-step").<DataStructure<String>, DataStructure<String>>chunk(1).reader(reader).writer(writer).build();
        Job job = job(redisContainer, "metrics-job", step);
        JobExecution execution = asyncJobLauncher.run(job, new JobParameters());
        awaitRunning(execution);
        Thread.sleep(100);
        registry.forEachMeter(m -> log.info("Meter: {}", m.getId().getName()));
        Search search = registry.find("spring.batch.item.read");
        Assertions.assertNotNull(search.timer());
        search = registry.find("spring.batch.redis.reader.queue.size");
        Assertions.assertNotNull(search.gauge());
        awaitJobTermination(execution);
    }

    @ParameterizedTest
    @MethodSource("sourceContainers")
    public void testKeyReaderSize(RedisContainer redisContainer) throws Throwable {
        DataGenerator.builder(redisContainer).end(12345).build().call();
        RedisKeyCommands<String, String> sync = sync(redisContainer);
        long hashCount = sync.keys("hash:*").size();
        try (GenericObjectPool<StatefulConnection<String, String>> pool = pool(redisContainer)) {
            ScanSizeEstimator<?> estimator = new ScanSizeEstimator<>(pool, asyncFunction(redisContainer));
            long matchSize = estimator.estimate(ScanSizeEstimator.Options.builder().match("hash:*").sampleSize(1000).build());
            Assertions.assertEquals(hashCount, matchSize, (double) hashCount / 10);
            long typeSize = estimator.estimate(ScanSizeEstimator.Options.builder().type(DataType.HASH).sampleSize(1000).build());
            Assertions.assertEquals(hashCount, typeSize, (double) hashCount / 10);
        }
    }

}
