package org.springframework.batch.item.redis;

import com.redislabs.testcontainers.RedisClusterContainer;
import com.redislabs.testcontainers.RedisContainer;
import com.redislabs.testcontainers.RedisServer;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
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
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.DefaultBufferedReaderFactory;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.*;
import org.springframework.batch.item.redis.support.operation.Hset;
import org.springframework.batch.item.redis.support.operation.NullValuePredicate;
import org.springframework.batch.item.redis.support.operation.Xadd;
import org.springframework.batch.item.redis.support.operation.Zadd;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Testcontainers
@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
@SuppressWarnings({"rawtypes", "unchecked", "unused", "BusyWait", "SingleStatementInBlock", "NullableProblems", "SameParameterValue"})
@Slf4j
public class SpringBatchRedisTests {

    @Container
    private static final RedisContainer REDIS = new RedisContainer().withKeyspaceNotifications();
    @Container
    private static final RedisClusterContainer REDIS_CLUSTER = new RedisClusterContainer().withKeyspaceNotifications();
    @Container
    private static final RedisContainer REDIS_REPLICA = new RedisContainer();

    protected static final Map<RedisServer, AbstractRedisClient> CLIENTS = new HashMap<>();
    protected static final Map<RedisServer, GenericObjectPool<? extends StatefulConnection<String, String>>> POOLS = new HashMap<>();
    protected static final Map<RedisServer, StatefulConnection<String, String>> CONNECTIONS = new HashMap<>();
    protected static final Map<RedisServer, StatefulRedisPubSubConnection<String, String>> PUBSUB_CONNECTIONS = new HashMap<>();
    protected static final Map<RedisServer, BaseRedisAsyncCommands<String, String>> ASYNCS = new HashMap<>();
    protected static final Map<RedisServer, BaseRedisCommands<String, String>> SYNCS = new HashMap<>();

    @BeforeAll
    public static void setup() {
        add(REDIS);
        add(REDIS_CLUSTER);
        add(REDIS_REPLICA);
    }

    private static void add(RedisServer server) {
        if (server.isCluster()) {
            RedisClusterClient client = RedisClusterClient.create(server.getRedisURI());
            CLIENTS.put(server, client);
            StatefulRedisClusterConnection<String, String> connection = client.connect();
            CONNECTIONS.put(server, connection);
            SYNCS.put(server, connection.sync());
            ASYNCS.put(server, connection.async());
            PUBSUB_CONNECTIONS.put(server, client.connectPubSub());
            POOLS.put(server, ConnectionPoolSupport.createGenericObjectPool(client::connect, new GenericObjectPoolConfig<>()));
        } else {
            RedisClient client = RedisClient.create(server.getRedisURI());
            CLIENTS.put(server, client);
            StatefulRedisConnection<String, String> connection = client.connect();
            CONNECTIONS.put(server, connection);
            SYNCS.put(server, connection.sync());
            ASYNCS.put(server, connection.async());
            PUBSUB_CONNECTIONS.put(server, client.connectPubSub());
            POOLS.put(server, ConnectionPoolSupport.createGenericObjectPool(client::connect, new GenericObjectPoolConfig<>()));
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

    static Stream<RedisServer> servers() {
        return Stream.of(REDIS, REDIS_CLUSTER);
    }

    protected static RedisClient redisClient(RedisServer redis) {
        return (RedisClient) CLIENTS.get(redis);
    }

    protected static RedisClusterClient redisClusterClient(RedisServer redis) {
        return (RedisClusterClient) CLIENTS.get(redis);
    }

    protected static <T> T sync(RedisServer server) {
        return (T) SYNCS.get(server);
    }

    protected static <T> T async(RedisServer server) {
        return (T) ASYNCS.get(server);
    }

    protected static <C extends StatefulConnection<String, String>> C connection(RedisServer server) {
        return (C) CONNECTIONS.get(server);
    }

    protected static <C extends StatefulRedisPubSubConnection<String, String>> C pubSubConnection(RedisServer server) {
        return (C) PUBSUB_CONNECTIONS.get(server);
    }

    protected static <C extends StatefulConnection<String, String>> GenericObjectPool<C> pool(RedisServer server) {
        if (POOLS.containsKey(server)) {
            return (GenericObjectPool<C>) POOLS.get(server);
        }
        throw new IllegalStateException("No pool found for " + server);
    }


    @Autowired
    private JobLauncher jobLauncher;
    @Autowired
    private JobLauncher asyncJobLauncher;
    @Autowired
    private JobBuilderFactory jobs;
    @Autowired
    private StepBuilderFactory steps;

    private Job job(RedisServer redisServer, String name, TaskletStep step) {
        return jobs.get(name(redisServer, name) + "-job").start(step).build();
    }

    private <I, O> JobExecution execute(RedisServer redisServer, String name, ItemReader<? extends I> reader, ItemWriter<O> writer) throws Throwable {
        return execute(redisServer, name, step(name, reader, writer).build());
    }

    private JobExecution execute(RedisServer redisServer, String name, TaskletStep step) throws Exception {
        return checkForFailure(jobLauncher.run(job(redisServer, name, step), new JobParameters()));
    }

    private JobExecution checkForFailure(JobExecution execution) {
        if (!execution.getExitStatus().getExitCode().equals(ExitStatus.COMPLETED.getExitCode())) {
            Assertions.fail("Job not completed: " + execution.getExitStatus());
        }
        return execution;
    }

    private <I, O> JobExecution executeFlushing(RedisServer redisServer, String name, PollableItemReader<? extends I> reader, ItemWriter<O> writer) throws Throwable {
        TaskletStep step = flushing(step(name, reader, writer)).build();
        JobExecution execution = asyncJobLauncher.run(job(redisServer, name, step), new JobParameters());
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
    @MethodSource("servers")
    void testFlushingStep(RedisServer server) throws Throwable {
        PollableItemReader<String> reader = keyEventReader(server);
        ListItemWriter<String> writer = new ListItemWriter<>();
        JobExecution execution = executeFlushing(server, "flushing", reader, writer);
        dataGenerator(server).end(3).maxExpire(Duration.ofMillis(0)).dataTypes(DataStructure.STRING, DataStructure.HASH).build().call();
        awaitJobTermination(execution);
        RedisServerCommands<String, String> commands = sync(server);
        Assertions.assertEquals(commands.dbsize(), writer.getWrittenItems().size());
    }

    private PollableItemReader<String> keyEventReader(RedisServer server) {
        int queueCapacity = KeyValueItemReader.LiveKeyValueItemReaderBuilder.DEFAULT_QUEUE_CAPACITY;
        if (server.isCluster()) {
            RedisClusterClient client = redisClusterClient(server);
            return new RedisClusterKeyspaceNotificationItemReader(client::connectPubSub, KeyValueItemReader.LiveKeyValueItemReaderBuilder.DEFAULT_PUBSUB_PATTERN, queueCapacity);
        }
        RedisClient client = redisClient(server);
        return new RedisKeyspaceNotificationItemReader(client::connectPubSub, KeyValueItemReader.LiveKeyValueItemReaderBuilder.DEFAULT_PUBSUB_PATTERN, queueCapacity);
    }

    private void awaitJobTermination(JobExecution execution) throws Throwable {
        while (execution.isRunning()) {
            Thread.sleep(10);
        }
        checkForFailure(execution);
    }

    @ParameterizedTest
    @MethodSource("servers")
    void testKeyspaceNotificationReader(RedisServer server) throws Throwable {
        BlockingQueue<String> queue = new LinkedBlockingDeque<>(10000);
        AbstractKeyspaceNotificationItemReader<?> reader = keyspaceNotificationReader(server, queue);
        reader.open(new ExecutionContext());
        BaseRedisAsyncCommands<String, String> async = async(server);
        async.setAutoFlushCommands(false);
        List<String> keys = new ArrayList<>();
        List<RedisFuture<?>> futures = new ArrayList<>();
        for (int index = 0; index < 4321; index++) {
            String key = "key" + index;
            futures.add(((RedisStringAsyncCommands<String, String>) async).set(key, "value"));
            if (futures.size() == 50) {
                async.flushCommands();
                LettuceFutures.awaitAll(1000, TimeUnit.MILLISECONDS, futures.toArray(new RedisFuture[0]));
                futures.clear();
            }
            keys.add(key);
        }
        async.flushCommands();
        LettuceFutures.awaitAll(1000, TimeUnit.MILLISECONDS, futures.toArray(new RedisFuture[0]));
        Thread.sleep(10);
        Assertions.assertEquals(keys.size(), queue.size());
        reader.close();
        async.setAutoFlushCommands(true);
    }

    private AbstractKeyspaceNotificationItemReader<?> keyspaceNotificationReader(RedisServer server, BlockingQueue<String> queue) {
        String pattern = KeyValueItemReader.LiveKeyValueItemReaderBuilder.pubSubPattern(0, KeyValueItemReader.LiveKeyValueItemReaderBuilder.DEFAULT_KEY_PATTERN);
        if (server.isCluster()) {
            RedisClusterClient client = redisClusterClient(server);
            return new RedisClusterKeyspaceNotificationItemReader(client::connectPubSub, pattern, queue);
        }
        RedisClient client = redisClient(server);
        return new RedisKeyspaceNotificationItemReader(client::connectPubSub, pattern, queue);
    }


    @ParameterizedTest
    @MethodSource("servers")
    void testDataStructureReader(RedisServer server) throws Throwable {
        populateSource("scan-reader-populate", server);
        KeyValueItemReader<DataStructure> reader = dataStructureReader(server);
        ListItemWriter<DataStructure> writer = new ListItemWriter<>();
        JobExecution execution = execute(server, "scan-reader", reader, writer);
        Assertions.assertTrue(execution.getAllFailureExceptions().isEmpty());
        RedisServerCommands<String, String> sync = sync(server);
        Assertions.assertEquals(sync.dbsize(), writer.getWrittenItems().size());
    }

    private void populateSource(String name, RedisServer server) throws Throwable {
        FlatFileItemReader<Map<String, String>> fileReader = fileReader(new ClassPathResource("beers.csv"));
        StatefulConnection<String, String> connection = connection(server);
        ItemWriter<Map<String, String>> hsetWriter = items -> {
            BaseRedisAsyncCommands<String, String> async = async(server);
            async.setAutoFlushCommands(false);
            List<RedisFuture<?>> futures = new ArrayList<>();
            for (Map<String, String> item : items) {
                futures.add(((RedisHashAsyncCommands<String, String>) async).hset(item.get(Beers.FIELD_ID), item));
            }
            async.flushCommands();
            LettuceFutures.awaitAll(RedisURI.DEFAULT_TIMEOUT_DURATION, futures.toArray(new RedisFuture[0]));
            async.setAutoFlushCommands(true);
        };
        execute(server, name, fileReader, hsetWriter);
    }

    @ParameterizedTest
    @MethodSource("servers")
    void testStreamReader(RedisServer server) throws Throwable {
        dataGenerator(server).dataTypes(DataStructure.STREAM).end(100).build().call();
        StreamItemReader reader = streamReader(server, XReadArgs.StreamOffset.from("stream:0", "0-0"));
        ListItemWriter<StreamMessage<String, String>> writer = new ListItemWriter<>();
        JobExecution execution = executeFlushing(server, "stream-reader", reader, writer);
        awaitJobTermination(execution);
        Assertions.assertEquals(10, writer.getWrittenItems().size());
        List<? extends StreamMessage<String, String>> items = writer.getWrittenItems();
        for (StreamMessage<String, String> message : items) {
            Assertions.assertTrue(message.getBody().containsKey("field1"));
            Assertions.assertTrue(message.getBody().containsKey("field2"));
        }
    }

    private StreamItemReader streamReader(RedisServer server, XReadArgs.StreamOffset<String> offset) {
        if (server.isCluster()) {
            return StreamItemReader.client(redisClusterClient(server)).offset(offset).build();
        }
        return StreamItemReader.client(redisClient(server)).offset(offset).build();
    }

    @ParameterizedTest
    @MethodSource("servers")
    void testMultiThreadedReader(RedisServer server) throws Throwable {
        populateSource("multithreaded-scan-reader-populate", server);
        SynchronizedItemStreamReader<DataStructure> synchronizedReader = new SynchronizedItemStreamReader<>();
        synchronizedReader.setDelegate(dataStructureReader(server));
        synchronizedReader.afterPropertiesSet();
        SynchronizedListItemWriter<DataStructure> writer = new SynchronizedListItemWriter<>();
        String name = "multithreaded-scan-reader";
        int threads = 4;
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(threads);
        taskExecutor.setCorePoolSize(threads);
        taskExecutor.afterPropertiesSet();
        JobExecution execution = execute(server, name, step(name, synchronizedReader, writer).taskExecutor(taskExecutor).throttleLimit(threads).build());
        Assertions.assertTrue(execution.getAllFailureExceptions().isEmpty());
        RedisServerCommands<String, String> sync = sync(server);
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
    @MethodSource("servers")
    void testStreamWriter(RedisServer redisServer) throws Throwable {
        String stream = "stream:0";
        List<Map<String, String>> messages = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            Map<String, String> body = new HashMap<>();
            body.put("field1", "value1");
            body.put("field2", "value2");
            messages.add(body);
        }
        ListItemReader<Map<String, String>> reader = new ListItemReader<>(messages);
        OperationItemWriter<Map<String, String>> writer = operationWriter(redisServer, new Xadd<>(stream, i -> i));
        execute(redisServer, "stream-writer", reader, writer);
        RedisStreamCommands<String, String> sync = sync(redisServer);
        Assertions.assertEquals(messages.size(), sync.xlen(stream));
        List<StreamMessage<String, String>> xrange = sync.xrange(stream, Range.create("-", "+"));
        for (int index = 0; index < xrange.size(); index++) {
            StreamMessage<String, String> message = xrange.get(index);
            Assertions.assertEquals(messages.get(index), message.getBody());
        }
    }

    private String name(RedisServer server, String name) {
        if (server.isCluster()) {
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
        OperationItemWriter<Map<String, String>> writer = OperationItemWriter.operation(new Xadd(stream, i -> i)).client(redisClient(REDIS)).transactional(true).build();
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
    @MethodSource("servers")
    public void testHashWriter(RedisServer server) throws Throwable {
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
        OperationItemWriter<Map<String, String>> writer = operationWriter(server, new Hset<>(keyConverter, m -> m));
        execute(server, "hash-writer", reader, writer);
        RedisKeyCommands<String, String> sync = sync(server);
        Assertions.assertEquals(maps.size(), sync.keys("hash:*").size());
        RedisHashCommands<String, String> hashCommands = sync(server);
        for (int index = 0; index < maps.size(); index++) {
            Map<String, String> hash = hashCommands.hgetall("hash:" + index);
            Assertions.assertEquals(maps.get(index), hash);
        }
    }

    @ParameterizedTest
    @MethodSource("servers")
    public void testHashDelWriter(RedisServer server) throws Throwable {
        List<Map.Entry<String, Map<String, String>>> hashes = new ArrayList<>();
        RedisHashCommands<String, String> commands = sync(server);
        for (int index = 0; index < 100; index++) {
            String key = String.valueOf(index);
            Map<String, String> value = new HashMap<>();
            value.put("field1", "value1");
            commands.hset("hash:" + key, value);
            Map<String, String> body = new HashMap<>();
            body.put("field2", "value2");
            hashes.add(new AbstractMap.SimpleEntry<>(key, index < 50 ? null : body));
        }
        RedisKeyCommands<String, String> sync = sync(server);
        ListItemReader<Map.Entry<String, Map<String, String>>> reader = new ListItemReader<>(hashes);
        KeyMaker<Map.Entry<String, Map<String, String>>> keyConverter = KeyMaker.<Map.Entry<String, Map<String, String>>>builder().prefix("hash").converters(Map.Entry::getKey).build();
        OperationItemWriter<Map.Entry<String, Map<String, String>>> writer = operationWriter(server, new Hset<>(keyConverter, Map.Entry::getValue, new NullValuePredicate<>(Map.Entry::getValue)));
        execute(server, "hash-del-writer", reader, writer);
        Assertions.assertEquals(50, sync.keys("hash:*").size());
        Assertions.assertEquals(2, commands.hgetall("hash:50").size());
    }

    private <T> OperationItemWriter<T> operationWriter(RedisServer server, OperationItemWriter.RedisOperation<T> operation) {
        if (server.isCluster()) {
            return OperationItemWriter.operation(operation).client(redisClusterClient(server)).build();
        }
        return OperationItemWriter.operation(operation).client(redisClient(server)).build();
    }

    @ParameterizedTest
    @MethodSource("servers")
    public void testSortedSetWriter(RedisServer server) throws Throwable {
        List<ScoredValue<String>> values = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            values.add((ScoredValue<String>) ScoredValue.fromNullable(index % 10, String.valueOf(index)));
        }
        ListItemReader<ScoredValue<String>> reader = new ListItemReader<>(values);
        KeyMaker<ScoredValue<String>> keyConverter = KeyMaker.<ScoredValue<String>>builder().prefix("zset").build();
        OperationItemWriter<ScoredValue<String>> writer = operationWriter(server, new Zadd<>("zset", Value::getValue, ScoredValue::getScore));
        execute(server, "sorted-set-writer", reader, writer);
        RedisServerCommands<String, String> sync = sync(server);
        Assertions.assertEquals(1, sync.dbsize());
        Assertions.assertEquals(values.size(), ((RedisSortedSetCommands<String, String>) sync).zcard("zset"));
        List<String> range = ((RedisSortedSetCommands<String, String>) sync).zrangebyscore("zset", Range.from(Range.Boundary.including(0), Range.Boundary.including(5)));
        Assertions.assertEquals(60, range.size());
    }

    @ParameterizedTest
    @MethodSource("servers")
    public void testDataStructureWriter(RedisServer server) throws Throwable {
        List<DataStructure> list = new ArrayList<>();
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
        ListItemReader<DataStructure> reader = new ListItemReader<>(list);
        DataStructureItemWriter<DataStructure> writer = dataStructureWriter(server);
        execute(server, "value-writer", reader, writer);
        RedisKeyCommands<String, String> sync = sync(server);
        List<String> keys = sync.keys("hash:*");
        Assertions.assertEquals(count, keys.size());
    }

    @ParameterizedTest
    @MethodSource("servers")
    public void testLiveReader(RedisServer server) throws Throwable {
        LiveKeyValueItemReader<KeyValue<byte[]>> reader = liveKeyDumpReader(server);
        ListItemWriter<KeyValue<byte[]>> writer = new ListItemWriter<>();
        JobExecution execution = executeFlushing(server, "live-reader", reader, writer);
        log.debug("Generating keyspace notifications");
        dataGenerator(server).end(123).maxExpire(Duration.ofMillis(0)).dataTypes(DataStructure.STRING, DataStructure.HASH).build().call();
        awaitJobTermination(execution);
        RedisServerCommands<String, String> sync = sync(server);
        Assertions.assertEquals(sync.dbsize(), writer.getWrittenItems().size());
    }

    private LiveKeyValueItemReader<KeyValue<byte[]>> liveKeyDumpReader(RedisServer server) {
        Duration idleTimeout = Duration.ofMillis(500);
        if (server.isCluster()) {
            return KeyDumpItemReader.client(redisClusterClient(server)).live().idleTimeout(idleTimeout).build();
        }
        return KeyDumpItemReader.client(redisClient(server)).live().idleTimeout(idleTimeout).build();
    }

    private KeyValueItemReader<KeyValue<byte[]>> keyDumpReader(RedisServer server) {
        if (server.isCluster()) {
            return KeyDumpItemReader.client(redisClusterClient(server)).build();
        }
        return KeyDumpItemReader.client(redisClient(server)).build();
    }

    private KeyValueItemReader<DataStructure> dataStructureReader(RedisServer server) {
        if (server.isCluster()) {
            return DataStructureItemReader.client(redisClusterClient(server)).build();
        }
        return DataStructureItemReader.client(redisClient(server)).build();
    }

    private DataStructureValueReader dataStructureValueReader(RedisServer server) {
        if (server.isCluster()) {
            return DataStructureValueReader.client(redisClusterClient(server)).build();
        }
        return DataStructureValueReader.client(redisClient(server)).build();
    }

    @ParameterizedTest
    @MethodSource("servers")
    public void testDataStructureReplication(RedisServer server) throws Throwable {
        dataGenerator(server).end(10000).build().call();
        KeyValueItemReader<DataStructure> reader = dataStructureReader(server);
        DataStructureItemWriter<DataStructure> writer = dataStructureWriter(REDIS_REPLICA);
        execute(server, "ds-replication", reader, writer);
        compare(server, "ds-replication");
    }

    private DataGenerator.DataGeneratorBuilder dataGenerator(RedisServer server) {
        if (server.isCluster()) {
            return DataGenerator.client(redisClusterClient(server));
        }
        return DataGenerator.client(redisClient(server));
    }

    private DataStructureItemWriter<DataStructure> dataStructureWriter(RedisServer server) {
        if (server.isCluster()) {
            return DataStructureItemWriter.client(redisClusterClient(server)).build();
        }
        return DataStructureItemWriter.client(redisClient(server)).build();
    }

    @ParameterizedTest
    @MethodSource("servers")
    public void testReplication(RedisServer redisServer) throws Throwable {
        dataGenerator(redisServer).end(10000).build().call();
        KeyValueItemReader reader = keyDumpReader(redisServer);
        OperationItemWriter<KeyValue<byte[]>> writer = keyDumpWriter(REDIS_REPLICA);
        execute(redisServer, "replication", reader, writer);
        compare(redisServer, "replication");
    }

    @ParameterizedTest
    @MethodSource("servers")
    public void testLiveReplication(RedisServer redisServer) throws Throwable {
        dataGenerator(redisServer).end(10000).build().call();
        KeyValueItemReader reader = keyDumpReader(redisServer);
        reader.setName("reader");
        OperationItemWriter< KeyValue<byte[]>> writer = keyDumpWriter(REDIS_REPLICA);
        writer.setName("writer");
        TaskletStep replicationStep = step("replication", reader, writer).build();
        LiveKeyValueItemReader liveReader = liveKeyDumpReader(redisServer);
        liveReader.setName("live-reader");
        OperationItemWriter< KeyValue< byte[]>> liveWriter = keyDumpWriter(REDIS_REPLICA);
        liveWriter.setName("live-writer");
        TaskletStep liveReplicationStep = flushing(step("live-replication", liveReader, liveWriter)).build();
        SimpleFlow replicationFlow = new FlowBuilder<SimpleFlow>("replication-flow").start(replicationStep).build();
        SimpleFlow liveReplicationFlow = new FlowBuilder<SimpleFlow>("live-replication-flow").start(liveReplicationStep).build();
        Job job = jobs.get(name(redisServer, "live-replication-job")).start(new FlowBuilder<SimpleFlow>("live-replication-flow").split(new SimpleAsyncTaskExecutor()).add(replicationFlow, liveReplicationFlow).build()).build().build();
        JobExecution execution = asyncJobLauncher.run(job, new JobParameters());
        awaitRunning(execution);
        dataGenerator(redisServer).end(123).build().call();
        awaitJobTermination(execution);
        compare(redisServer, "live-replication");
    }

    private KeyDumpItemWriter keyDumpWriter(RedisServer redis) {
        if (redis.isCluster()) {
            return KeyDumpItemWriter.client(redisClusterClient(redis)).build();
        }
        return KeyDumpItemWriter.client(redisClient(redis)).build();
    }

    private void compare(RedisServer server, String name) throws Throwable {
        RedisServerCommands<String, String> sourceSync = sync(server);
        RedisServerCommands<String, String> targetSync = sync(REDIS_REPLICA);
        Assertions.assertEquals(sourceSync.dbsize(), targetSync.dbsize());
        KeyValueItemReader left = dataStructureReader(server);
        DataStructureValueReader right = dataStructureValueReader(REDIS_REPLICA);
        KeyComparisonResultCounter counter = new KeyComparisonResultCounter();
        KeyComparisonItemWriter writer = KeyComparisonItemWriter.valueReader(right).resultHandler(counter).ttlTolerance(Duration.ofMillis(500)).build();
        execute(server, name + "-compare", left, writer);
        Assertions.assertEquals(sourceSync.dbsize(), counter.get(KeyComparisonItemWriter.Result.OK));
        Assertions.assertTrue(counter.isOK());
    }

    @ParameterizedTest
    @MethodSource("servers")
    public void testComparisonWriter(RedisServer server) throws Throwable {
        BaseRedisAsyncCommands<String, String> source = async(server);
        source.setAutoFlushCommands(false);
        BaseRedisAsyncCommands<String, String> target = async(REDIS_REPLICA);
        target.setAutoFlushCommands(false);
        List<RedisFuture<?>> sourceFutures = new ArrayList<>();
        List<RedisFuture<?>> targetFutures = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            sourceFutures.add(((RedisStringAsyncCommands<String, String>) source).set("key" + index, "value" + index));
            targetFutures.add(((RedisStringAsyncCommands<String, String>) target).set("key" + index, "value" + index));
        }
        source.flushCommands();
        LettuceFutures.awaitAll(10, TimeUnit.SECONDS, sourceFutures.toArray(new RedisFuture[0]));
        target.flushCommands();
        LettuceFutures.awaitAll(10, TimeUnit.SECONDS, targetFutures.toArray(new RedisFuture[0]));
        source.setAutoFlushCommands(true);
        target.setAutoFlushCommands(true);
        RedisHashCommands<String, String> sourceSync = sync(server);
        sourceSync.hset("zehash", "zefield", "zevalue");
        KeyValueItemReader left = dataStructureReader(server);
        DataStructureValueReader right = dataStructureValueReader(REDIS_REPLICA);
        KeyComparisonResultCounter counter = new KeyComparisonResultCounter();
        KeyComparisonItemWriter writer = KeyComparisonItemWriter.valueReader(right).resultHandler(counter).ttlTolerance(Duration.ofMillis(500)).build();
        execute(server, "test-comparison-writer-compare", left, writer);
        Assertions.assertFalse(counter.isOK());
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
        KeyValueItemReader reader = DataStructureItemReader.client(redisClient(REDIS)).queueCapacity(10).chunkSize(1).build();
        ItemWriter<DataStructure> writer = items -> Thread.sleep(1);
        TaskletStep step = steps.get("metrics-step").<DataStructure, DataStructure>chunk(1).reader(reader).writer(writer).build();
        Job job = job(REDIS, "metrics-job", step);
        JobExecution execution = asyncJobLauncher.run(job, new JobParameters());
        awaitRunning(execution);
        Thread.sleep(100);
        registry.forEachMeter(m -> log.debug("Meter: {}", m.getId().getName()));
        Search search = registry.find("spring.batch.item.read");
        Assertions.assertNotNull(search.timer());
        search = registry.find("spring.batch.redis.reader.queue.size");
        Assertions.assertNotNull(search.gauge());
        awaitJobTermination(execution);
    }

    @ParameterizedTest
    @MethodSource("servers")
    public void testScanSizeEstimator(RedisServer server) throws Throwable {
        dataGenerator(server).end(12345).dataTypes(DataStructure.HASH).build().call();
        ScanSizeEstimator estimator = sizeEstimator(server);
        long matchSize = estimator.estimate(ScanSizeEstimator.EstimateOptions.builder().sampleSize(100).match("hash:*").build());
        RedisKeyCommands<String, String> sync = sync(server);
        long hashCount = sync.keys("hash:*").size();
        Assertions.assertEquals(hashCount, matchSize, (double) hashCount / 10);
        long typeSize = estimator.estimate(ScanSizeEstimator.EstimateOptions.builder().sampleSize(1000).type(DataStructure.HASH).build());
        Assertions.assertEquals(hashCount, typeSize, (double) hashCount / 10);
    }

    private ScanSizeEstimator sizeEstimator(RedisServer server) {
        if (server.isCluster()) {
            return ScanSizeEstimator.client(redisClusterClient(server)).build();
        }
        return ScanSizeEstimator.client(redisClient(server)).build();
    }

}
