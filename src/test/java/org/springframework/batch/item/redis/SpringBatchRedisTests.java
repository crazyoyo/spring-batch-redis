package org.springframework.batch.item.redis;

import com.redislabs.testcontainers.RedisClusterContainer;
import com.redislabs.testcontainers.RedisContainer;
import com.redislabs.testcontainers.RedisStandaloneContainer;
import io.lettuce.core.*;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import io.lettuce.core.api.sync.*;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
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
import org.junit.jupiter.api.*;
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
import java.util.*;
import java.util.stream.Stream;

@Testcontainers
@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
@SuppressWarnings({"rawtypes", "unchecked", "unused", "BusyWait", "SingleStatementInBlock", "NullableProblems", "SameParameterValue"})
@Slf4j
public class SpringBatchRedisTests {

    @Container
    private static final RedisStandaloneContainer REDIS = new RedisStandaloneContainer().withKeyspaceNotifications();
    @Container
    private static final RedisClusterContainer REDIS_CLUSTER = new RedisClusterContainer().withKeyspaceNotifications();
    @Container
    private static final RedisStandaloneContainer REDIS_REPLICA = new RedisStandaloneContainer();

    protected static final Map<RedisContainer, AbstractRedisClient> CLIENTS = new HashMap<>();
    protected static final Map<RedisContainer, GenericObjectPool<? extends StatefulConnection<String, String>>> POOLS = new HashMap<>();
    protected static final Map<RedisContainer, StatefulConnection<String, String>> CONNECTIONS = new HashMap<>();
    protected static final Map<RedisContainer, StatefulRedisPubSubConnection<String, String>> PUBSUB_CONNECTIONS = new HashMap<>();
    protected static final Map<RedisContainer, BaseRedisAsyncCommands<String, String>> ASYNCS = new HashMap<>();
    protected static final Map<RedisContainer, BaseRedisCommands<String, String>> SYNCS = new HashMap<>();

    @BeforeAll
    public static void setup() {
        add(REDIS);
        add(REDIS_CLUSTER);
        add(REDIS_REPLICA);
    }

    private static void add(RedisContainer container) {
        if (container.isCluster()) {
            RedisClusterClient client = RedisClusterClient.create(container.getRedisURI());
            CLIENTS.put(container, client);
            StatefulRedisClusterConnection<String, String> connection = client.connect();
            CONNECTIONS.put(container, connection);
            SYNCS.put(container, connection.sync());
            ASYNCS.put(container, connection.async());
            PUBSUB_CONNECTIONS.put(container, client.connectPubSub());
            POOLS.put(container, ConnectionPoolSupport.createGenericObjectPool(client::connect, new GenericObjectPoolConfig<>()));
        } else {
            RedisClient client = RedisClient.create(container.getRedisURI());
            CLIENTS.put(container, client);
            StatefulRedisConnection<String, String> connection = client.connect();
            CONNECTIONS.put(container, connection);
            SYNCS.put(container, connection.sync());
            ASYNCS.put(container, connection.async());
            PUBSUB_CONNECTIONS.put(container, client.connectPubSub());
            POOLS.put(container, ConnectionPoolSupport.createGenericObjectPool(client::connect, new GenericObjectPoolConfig<>()));
        }
    }

    @AfterEach
    public void flushall() {
        for (BaseRedisCommands<String, String> sync : SYNCS.values()) {
            ((RedisServerCommands<String, String>) sync).flushall();
        }
    }

    @AfterAll
    public static void teardown() {
        for (StatefulConnection<String, String> connection : CONNECTIONS.values()) {
            connection.close();
        }
        for (StatefulRedisPubSubConnection<String, String> pubSubConnection : PUBSUB_CONNECTIONS.values()) {
            pubSubConnection.close();
        }
        for (GenericObjectPool<? extends StatefulConnection<String, String>> pool : POOLS.values()) {
            pool.close();
        }
        for (AbstractRedisClient client : CLIENTS.values()) {
            client.shutdown();
            client.getResources().shutdown();
        }
        SYNCS.clear();
        ASYNCS.clear();
        CONNECTIONS.clear();
        PUBSUB_CONNECTIONS.clear();
        POOLS.clear();
        CLIENTS.clear();
    }

    static Stream<RedisContainer> containers() {
        return Stream.of(REDIS, REDIS_CLUSTER);
    }

    protected static RedisClient redisClient(RedisContainer container) {
        return (RedisClient) CLIENTS.get(container);
    }

    protected static RedisClusterClient redisClusterClient(RedisContainer container) {
        return (RedisClusterClient) CLIENTS.get(container);
    }

    protected static <T> T sync(RedisContainer container) {
        return (T) SYNCS.get(container);
    }

    protected static <T> T async(RedisContainer container) {
        return (T) ASYNCS.get(container);
    }

    protected static <C extends StatefulConnection<String, String>> C connection(RedisContainer container) {
        return (C) CONNECTIONS.get(container);
    }

    protected static <C extends StatefulRedisPubSubConnection<String, String>> C pubSubConnection(RedisContainer container) {
        return (C) PUBSUB_CONNECTIONS.get(container);
    }

    protected static <C extends StatefulConnection<String, String>> GenericObjectPool<C> pool(RedisContainer container) {
        if (POOLS.containsKey(container)) {
            return (GenericObjectPool<C>) POOLS.get(container);
        }
        throw new IllegalStateException("No pool found for " + container);
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
        Thread.sleep(200);
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
    @MethodSource("containers")
    void testFlushingStep(RedisContainer container) throws Throwable {
        PollableItemReader<String> reader = keyspaceNotificationReader(container);
        ListItemWriter<String> writer = new ListItemWriter<>();
        JobExecution execution = executeFlushing(container, "flushing", reader, writer);
        dataGenerator(container).end(3).maxExpire(0).dataTypes(DataStructure.STRING, DataStructure.HASH).build().call();
        awaitJobTermination(execution);
        RedisServerCommands<String, String> commands = sync(container);
        Assertions.assertEquals(commands.dbsize(), writer.getWrittenItems().size());
    }

    private PollableItemReader<String> keyspaceNotificationReader(RedisContainer container) {
        if (container.isCluster()) {
            return RedisClusterKeyspaceNotificationItemReader.client(redisClusterClient(container)).build();
        }
        return RedisKeyspaceNotificationItemReader.client(redisClient(container)).build();
    }

    private void awaitJobTermination(JobExecution execution) throws Throwable {
        while (execution.isRunning()) {
            Thread.sleep(10);
        }
        checkForFailure(execution);
    }

    @ParameterizedTest
    @MethodSource("containers")
    void testDataStructureReader(RedisContainer container) throws Throwable {
        populateSource("scan-reader-populate", container);
        KeyValueItemReader<String, DataStructure<String>> reader = dataStructureReader(container);
        ListItemWriter<DataStructure<String>> writer = new ListItemWriter<>();
        JobExecution execution = execute(container, "scan-reader", reader, writer);
        Assertions.assertTrue(execution.getAllFailureExceptions().isEmpty());
        RedisServerCommands<String, String> sync = sync(container);
        Assertions.assertEquals(sync.dbsize(), writer.getWrittenItems().size());
    }

    private void populateSource(String name, RedisContainer container) throws Throwable {
        FlatFileItemReader<Map<String, String>> fileReader = fileReader(new ClassPathResource("beers.csv"));
        StatefulConnection<String, String> connection = connection(container);
        ItemWriter<Map<String, String>> hsetWriter = items -> {
            BaseRedisAsyncCommands<String, String> async = async(container);
            async.setAutoFlushCommands(false);
            List<RedisFuture<?>> futures = new ArrayList<>();
            for (Map<String, String> item : items) {
                futures.add(((RedisHashAsyncCommands<String, String>) async).hset(item.get(Beers.FIELD_ID), item));
            }
            async.flushCommands();
            LettuceFutures.awaitAll(RedisURI.DEFAULT_TIMEOUT_DURATION, futures.toArray(new RedisFuture[0]));
            async.setAutoFlushCommands(true);
        };
        execute(container, name, fileReader, hsetWriter);
    }

    @ParameterizedTest
    @MethodSource("containers")
    void testStreamReader(RedisContainer container) throws Throwable {
        dataGenerator(container).dataTypes(DataStructure.STREAM).end(100).build().call();
        StreamItemReader<String, String> reader = streamReaderBuilder(container).offset(StreamOffset.from("stream:0", "0-0")).build();
        ListItemWriter<StreamMessage<String, String>> writer = new ListItemWriter<>();
        JobExecution execution = executeFlushing(container, "stream-reader", reader, writer);
        awaitJobTermination(execution);
        Assertions.assertEquals(10, writer.getWrittenItems().size());
        List<? extends StreamMessage<String, String>> items = writer.getWrittenItems();
        for (StreamMessage<String, String> message : items) {
            Assertions.assertTrue(message.getBody().containsKey("field1"));
            Assertions.assertTrue(message.getBody().containsKey("field2"));
        }
    }

    private StreamItemReader.StreamItemReaderBuilder streamReaderBuilder(RedisContainer container) {
        if (container.isCluster()) {
            return StreamItemReader.client(redisClusterClient(container));
        }
        return StreamItemReader.client(redisClient(container));
    }

    @ParameterizedTest
    @MethodSource("containers")
    void testMultiThreadedReader(RedisContainer container) throws Throwable {
        populateSource("multithreaded-scan-reader-populate", container);
        SynchronizedItemStreamReader<DataStructure<String>> synchronizedReader = new SynchronizedItemStreamReader<>();
        synchronizedReader.setDelegate(dataStructureReader(container));
        synchronizedReader.afterPropertiesSet();
        SynchronizedListItemWriter<DataStructure<String>> writer = new SynchronizedListItemWriter<>();
        String name = "multithreaded-scan-reader";
        int threads = 4;
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(threads);
        taskExecutor.setCorePoolSize(threads);
        taskExecutor.afterPropertiesSet();
        JobExecution execution = execute(container, name, step(name, synchronizedReader, writer).taskExecutor(taskExecutor).throttleLimit(threads).build());
        Assertions.assertTrue(execution.getAllFailureExceptions().isEmpty());
        RedisServerCommands<String, String> sync = sync(container);
        Assertions.assertEquals(sync.dbsize(), writer.getWrittenItems().size());
    }

    private static class SynchronizedListItemWriter<T> implements ItemWriter<T> {

        private final List<T> writtenItems = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void write(List<? extends T> items) {
            writtenItems.addAll(items);
        }

        public List<? extends T> getWrittenItems() {
            return this.writtenItems;
        }
    }


    @ParameterizedTest
    @MethodSource("containers")
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
        RedisOperation<String, String, Map<String, String>> xadd = RedisOperation.<Map<String, String>>xadd().key(i -> stream).body(i -> i).build();
        OperationItemWriter<String, String, Map<String, String>> writer = operationWriter(redisContainer, xadd);
        execute(redisContainer, "stream-writer", reader, writer);
        RedisStreamCommands<String, String> sync = sync(redisContainer);
        Assertions.assertEquals(messages.size(), sync.xlen(stream));
        List<StreamMessage<String, String>> xrange = sync.xrange(stream, Range.create("-", "+"));
        for (int index = 0; index < xrange.size(); index++) {
            StreamMessage<String, String> message = xrange.get(index);
            Assertions.assertEquals(messages.get(index), message.getBody());
        }
    }

    private String name(RedisContainer container, String name) {
        if (container.isCluster()) {
            return "cluster-" + name;
        }
        return name;
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
        RedisOperation<String, String, Map<String, String>> xadd = RedisOperation.<Map<String, String>>xadd().key(i -> stream).body(i -> i).build();
        OperationItemWriter.TransactionItemWriter<String, String, Map<String, String>> writer = OperationItemWriter.operation(xadd).transactional().client(redisClient(REDIS)).build();
        execute(REDIS, "stream-tx-writer", reader, writer);
        RedisStreamCommands<String, String> sync = sync(REDIS);
        Assertions.assertEquals(messages.size(), sync.xlen(stream));
        List<StreamMessage<String, String>> xrange = sync.xrange(stream, Range.create("-", "+"));
        for (int index = 0; index < xrange.size(); index++) {
            StreamMessage<String, String> message = xrange.get(index);
            Assertions.assertEquals(messages.get(index), message.getBody());
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("containers")
    public void testHashWriter(RedisContainer container) throws Throwable {
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
        RedisOperation<String, String, Map<String, String>> hset = RedisOperation.<Map<String, String>>hset().key(keyConverter).map(m -> m).build();
        OperationItemWriter<String, String, Map<String, String>> writer = operationWriter(container, hset);
        execute(container, "hash-writer", reader, writer);
        RedisKeyCommands<String, String> sync = sync(container);
        Assertions.assertEquals(maps.size(), sync.keys("hash:*").size());
        RedisHashCommands<String, String> hashCommands = sync(container);
        for (int index = 0; index < maps.size(); index++) {
            Map<String, String> hash = hashCommands.hgetall("hash:" + index);
            Assertions.assertEquals(maps.get(index), hash);
        }
    }

    private <T> OperationItemWriter<String, String, T> operationWriter(RedisContainer container, RedisOperation<String, String, T> operation) {
        if (container.isCluster()) {
            return OperationItemWriter.operation(operation).client(redisClusterClient(container)).build();
        }
        return OperationItemWriter.operation(operation).client(redisClient(container)).build();
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void testSortedSetWriter(RedisContainer container) throws Throwable {
        List<ScoredValue<String>> values = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            values.add((ScoredValue<String>) ScoredValue.fromNullable(index % 10, String.valueOf(index)));
        }
        ListItemReader<ScoredValue<String>> reader = new ListItemReader<>(values);
        KeyMaker<ScoredValue<String>> keyConverter = KeyMaker.<ScoredValue<String>>builder().prefix("zset").build();
        RedisOperation<String, String, ScoredValue<String>> zadd = RedisOperation.<ScoredValue<String>>zadd().key(keyConverter).member(Value::getValue).score(ScoredValue::getScore).build();
        OperationItemWriter<String, String, ScoredValue<String>> writer = operationWriter(container, zadd);
        execute(container, "sorted-set-writer", reader, writer);
        RedisServerCommands<String, String> sync = sync(container);
        Assertions.assertEquals(1, sync.dbsize());
        Assertions.assertEquals(values.size(), ((RedisSortedSetCommands<String, String>) sync).zcard("zset"));
        List<String> range = ((RedisSortedSetCommands<String, String>) sync).zrangebyscore("zset", Range.from(Range.Boundary.including(0), Range.Boundary.including(5)));
        Assertions.assertEquals(60, range.size());
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void testDataStructureWriter(RedisContainer container) throws Throwable {
        List<DataStructure<String>> list = new ArrayList<>();
        long count = 100;
        for (int index = 0; index < count; index++) {
            DataStructure keyValue = new DataStructure();
            keyValue.setKey("hash:" + index);
            keyValue.setType(DataStructure.HASH);
            Map<String, String> map = new HashMap<>();
            map.put("field1", "value1");
            map.put("field2", "value2");
            keyValue.setValue(map);
            list.add(keyValue);
        }
        ListItemReader<DataStructure<String>> reader = new ListItemReader<>(list);
        DataStructureItemWriter<String, String, DataStructure<String>> writer = dataStructureWriter(container);
        execute(container, "value-writer", reader, writer);
        RedisKeyCommands<String, String> sync = sync(container);
        List<String> keys = sync.keys("hash:*");
        Assertions.assertEquals(count, keys.size());
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void testLiveReader(RedisContainer container) throws Throwable {
        KeyspaceNotificationValueItemReader<String, KeyValue<String, byte[]>> reader = liveKeyDumpReader(container);
        ListItemWriter<KeyValue<String, byte[]>> writer = new ListItemWriter<>();
        JobExecution execution = executeFlushing(container, "live-reader", reader, writer);
        log.debug("Generating keyspace notifications");
        dataGenerator(container).end(123).maxExpire(0).dataTypes(DataStructure.STRING, DataStructure.HASH).build().call();
        awaitJobTermination(execution);
        RedisServerCommands<String, String> sync = sync(container);
        Assertions.assertEquals(sync.dbsize(), writer.getWrittenItems().size());
    }

    private KeyspaceNotificationValueItemReader<String, KeyValue<String, byte[]>> liveKeyDumpReader(RedisContainer container) {
        if (container.isCluster()) {
            RedisClusterClient client = redisClusterClient(container);
            return KeyValueItemReader.valueReader(KeyDumpValueReader.client(client).build()).keyReader(RedisClusterKeyspaceNotificationItemReader.client(client).build()).idleTimeout(Duration.ofMillis(500)).build();
        }
        RedisClient client = redisClient(container);
        return KeyValueItemReader.valueReader(KeyDumpValueReader.client(client).build()).keyReader(RedisKeyspaceNotificationItemReader.client(client).build()).idleTimeout(Duration.ofMillis(500)).build();
    }

    private KeyValueItemReader<String, KeyValue<String, byte[]>> keyDumpReader(RedisContainer container) {
        if (container.isCluster()) {
            RedisClusterClient client = redisClusterClient(container);
            return KeyValueItemReader.valueReader(KeyDumpValueReader.client(client).build()).keyReader(ScanKeyItemReader.client(client).build()).build();
        }
        RedisClient client = redisClient(container);
        return KeyValueItemReader.valueReader(KeyDumpValueReader.client(client).build()).keyReader(ScanKeyItemReader.client(client).build()).build();
    }

    private KeyValueItemReader<String, DataStructure<String>> dataStructureReader(RedisContainer container) {
        if (container.isCluster()) {
            RedisClusterClient client = redisClusterClient(container);
            return KeyValueItemReader.valueReader(DataStructureValueReader.client(client).build()).keyReader(ScanKeyItemReader.client(client).build()).build();
        }
        RedisClient client = redisClient(container);
        return KeyValueItemReader.valueReader(DataStructureValueReader.client(client).build()).keyReader(ScanKeyItemReader.client(client).build()).build();
    }

    private DataStructureValueReader<String, String> dataStructureValueReader(RedisContainer container) {
        if (container.isCluster()) {
            return DataStructureValueReader.client(redisClusterClient(container)).build();
        }
        return DataStructureValueReader.client(redisClient(container)).build();
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void testDataStructureReplication(RedisContainer container) throws Throwable {
        dataGenerator(container).end(10000).build().call();
        KeyValueItemReader<String, DataStructure<String>> reader = dataStructureReader(container);
        DataStructureItemWriter<String, String, DataStructure<String>> writer = dataStructureWriter(REDIS_REPLICA);
        execute(container, "ds-replication", reader, writer);
        compare(container, "ds-replication");
    }

    private DataGenerator.DataGeneratorBuilder dataGenerator(RedisContainer container) {
        if (container.isCluster()) {
            return DataGenerator.client(redisClusterClient(container));
        }
        return DataGenerator.client(redisClient(container));
    }

    private DataStructureItemWriter<String, String, DataStructure<String>> dataStructureWriter(RedisContainer container) {
        if (container.isCluster()) {
            return DataStructureItemWriter.client(redisClusterClient(container)).build();
        }
        return DataStructureItemWriter.client(redisClient(container)).build();
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void testReplication(RedisContainer redisContainer) throws Throwable {
        dataGenerator(redisContainer).end(10000).build().call();
        KeyValueItemReader reader = keyDumpReader(redisContainer);
        OperationItemWriter<String, String, KeyValue<String, byte[]>> writer = keyDumpWriter(REDIS_REPLICA);
        execute(redisContainer, "replication", reader, writer);
        compare(redisContainer, "replication");
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void testLiveReplication(RedisContainer redisContainer) throws Throwable {
        dataGenerator(redisContainer).end(10000).build().call();
        KeyValueItemReader reader = keyDumpReader(redisContainer);
        reader.setName("reader");
        OperationItemWriter<String, String, KeyValue<String, byte[]>> writer = keyDumpWriter(REDIS_REPLICA);
        writer.setName("writer");
        TaskletStep replicationStep = step("replication", reader, writer).build();
        KeyspaceNotificationValueItemReader liveReader = liveKeyDumpReader(redisContainer);
        liveReader.setName("live-reader");
        OperationItemWriter<String, String, KeyValue<String, byte[]>> liveWriter = keyDumpWriter(REDIS_REPLICA);
        liveWriter.setName("live-writer");
        TaskletStep liveReplicationStep = flushing(step("live-replication", liveReader, liveWriter)).build();
        SimpleFlow replicationFlow = new FlowBuilder<SimpleFlow>("replication-flow").start(replicationStep).build();
        SimpleFlow liveReplicationFlow = new FlowBuilder<SimpleFlow>("live-replication-flow").start(liveReplicationStep).build();
        Job job = jobs.get(name(redisContainer, "live-replication-job")).start(new FlowBuilder<SimpleFlow>("live-replication-flow").split(new SimpleAsyncTaskExecutor()).add(replicationFlow, liveReplicationFlow).build()).build().build();
        JobExecution execution = asyncJobLauncher.run(job, new JobParameters());
        awaitRunning(execution);
        dataGenerator(redisContainer).end(123).build().call();
        awaitJobTermination(execution);
        compare(redisContainer, "live-replication");
    }

    private KeyDumpItemWriter<String, String, KeyValue<String, byte[]>> keyDumpWriter(RedisContainer container) {
        if (container.isCluster()) {
            return KeyDumpItemWriter.client(redisClusterClient(container)).build();
        }
        return KeyDumpItemWriter.client(redisClient(container)).build();
    }


    private void compare(RedisContainer redisContainer, String name) throws Throwable {
        RedisServerCommands<String, String> sourceSync = sync(redisContainer);
        RedisServerCommands<String, String> targetSync = sync(REDIS_REPLICA);
        Assertions.assertEquals(sourceSync.dbsize(), targetSync.dbsize());
        KeyValueItemReader left = dataStructureReader(redisContainer);
        DataStructureValueReader<String, String> right = dataStructureValueReader(REDIS_REPLICA);
        KeyComparisonItemWriter<String> writer = new KeyComparisonItemWriter<>(right, Duration.ofSeconds(1));
        execute(redisContainer, name + "-compare", left, writer);
        KeyComparisonResults<String> results = writer.getResults();
        Assertions.assertEquals(sourceSync.dbsize(), writer.getResults().getOk());
        Assertions.assertFalse(writer.getResults().hasDiffs());
        Assertions.assertTrue(writer.getResults().isOk());
    }

    @Test
    public void testMetrics() throws Throwable {
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
        dataGenerator(REDIS).end(100).build().call();
        KeyValueItemReader reader = KeyValueItemReader.valueReader(DataStructureValueReader.client(redisClient(REDIS)).build()).keyReader(ScanKeyItemReader.client(redisClient(REDIS)).build()).queueCapacity(10).chunkSize(1).build();
        ItemWriter<DataStructure<String>> writer = items -> Thread.sleep(1);
        TaskletStep step = steps.get("metrics-step").<DataStructure<String>, DataStructure<String>>chunk(1).reader(reader).writer(writer).build();
        Job job = job(REDIS, "metrics-job", step);
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
    @MethodSource("containers")
    public void testScanSizeEstimator(RedisContainer container) throws Throwable {
        dataGenerator(container).end(12345).dataTypes(DataStructure.HASH).build().call();
        ScanSizeEstimator estimator = sizeEstimator(container);
        long matchSize = estimator.estimate(ScanSizeEstimator.EstimateOptions.builder().sampleSize(100).match("hash:*").build());
        RedisKeyCommands<String, String> sync = sync(container);
        long hashCount = sync.keys("hash:*").size();
        Assertions.assertEquals(hashCount, matchSize, (double) hashCount / 10);
        long typeSize = estimator.estimate(ScanSizeEstimator.EstimateOptions.builder().sampleSize(1000).type(DataStructure.HASH).build());
        Assertions.assertEquals(hashCount, typeSize, (double) hashCount / 10);
    }

    private ScanSizeEstimator sizeEstimator(RedisContainer container) {
        if (container.isCluster()) {
            return ScanSizeEstimator.client(redisClusterClient(container)).build();
        }
        return ScanSizeEstimator.client(redisClient(container)).build();
    }

}
