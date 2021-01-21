package org.springframework.batch.item.redis;

import io.lettuce.core.*;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
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
import org.junit.Assert;
import org.junit.jupiter.api.*;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
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
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.BufferedReader;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
@SuppressWarnings("rawtypes")
@Slf4j
public class SpringBatchRedisTests {

    private static final DockerImageName REDIS_IMAGE_NAME = DockerImageName.parse("redis:5.0.3-alpine");
    private static GenericContainer sourceRedis;
    private static GenericContainer targetRedis;
    private static RedisClient sourceRedisClient;
    private static RedisClient targetRedisClient;
    private static StatefulRedisConnection<String, String> sourceConnection;
    private static StatefulRedisConnection<String, String> targetConnection;
    private static RedisCommands<String, String> sourceSync;
    private static RedisCommands<String, String> targetSync;
    private static GenericObjectPool<StatefulRedisConnection<String, String>> sourcePool;
    private static GenericObjectPool<StatefulRedisConnection<String, String>> targetPool;

    @BeforeAll
    public static void setup() {
        sourceRedis = container();
        sourceRedisClient = RedisClient.create(RedisURI.create(sourceRedis.getHost(), sourceRedis.getFirstMappedPort()));
        sourcePool = ConnectionPoolSupport.createGenericObjectPool(sourceRedisClient::connect, new GenericObjectPoolConfig<>());
        sourceConnection = sourceRedisClient.connect();
        sourceSync = sourceConnection.sync();
        sourceSync.configSet("notify-keyspace-events", "AK");
        targetRedis = container();
        targetRedisClient = RedisClient.create(RedisURI.create(targetRedis.getHost(), targetRedis.getFirstMappedPort()));
        targetPool = ConnectionPoolSupport.createGenericObjectPool(targetRedisClient::connect, new GenericObjectPoolConfig<>());
        targetConnection = targetRedisClient.connect();
        targetSync = targetConnection.sync();
    }

    @AfterAll
    public static void teardown() {
        targetConnection.close();
        targetRedisClient.shutdown();
        targetRedis.stop();
        sourceConnection.close();
        sourceRedisClient.shutdown();
        sourceRedis.stop();
    }

    @SuppressWarnings("resource")
    private static GenericContainer container() {
        GenericContainer container = new GenericContainer<>(REDIS_IMAGE_NAME).withExposedPorts(6379);
        container.start();
        return container;
    }

    @BeforeEach
    public void flush() {
        sourceSync.flushall();
        targetSync.flushall();
    }

    @Autowired
    private JobLauncher jobLauncher;
    @Autowired
    private JobLauncher asyncJobLauncher;
    @Autowired
    private JobBuilderFactory jobs;
    @Autowired
    private StepBuilderFactory steps;


    private <I, O> JobExecution execute(String name, ItemReader<? extends I> reader, ItemWriter<O> writer) throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
        TaskletStep step = step(name, reader, writer).build();
        return jobLauncher.run(job(name, step), new JobParameters());
    }

    private <I, O> JobExecution executeFlushing(String name, PollableItemReader<? extends I> reader, ItemWriter<O> writer) throws InterruptedException, JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
        TaskletStep step = flushing(step(name, reader, writer)).build();
        JobExecution execution = asyncJobLauncher.run(job(name, step), new JobParameters());
        awaitRunning(execution);
        return execution;
    }

    private <I, O> SimpleStepBuilder<I, O> step(String name, ItemReader<? extends I> reader, ItemWriter<O> writer) {
        return steps.get(name + "-step").<I, O>chunk(50).reader(reader).writer(writer);
    }

    private <I, O> FlushingStepBuilder<I, O> flushing(SimpleStepBuilder<I, O> step) {
        return new FlushingStepBuilder<I, O>(step).idleTimeout(Duration.ofMillis(500));
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

    @Test
    public void testFlushingStep() throws Exception {
        try (StatefulRedisPubSubConnection<String, String> pubSubConnection = sourceRedisClient.connectPubSub()) {
            RedisKeyspaceNotificationItemReader<String, String> reader = (RedisKeyspaceNotificationItemReader<String, String>) DataStructureItemReader.builder(sourcePool, pubSubConnection).build().getKeyReader();
            ListItemWriter<String> writer = new ListItemWriter<>();
            JobExecution execution = executeFlushing("flushing", reader, writer);
            DataGenerator.builder().pool(sourcePool).end(3).maxExpire(0).dataType(DataType.STRING).dataType(DataType.HASH).build().call();
            awaitJobTermination(execution);
            Assertions.assertEquals(sourceSync.dbsize(), writer.getWrittenItems().size());
        }
    }

    private void awaitJobTermination(JobExecution execution) throws InterruptedException {
        while (execution.isRunning()) {
            Thread.sleep(10);
        }
    }

    @Test
    public void testDataStructureReader() throws Exception {
        FlatFileItemReader<Map<String, String>> fileReader = fileReader(new ClassPathResource("beers.csv"));
        ItemWriter<Map<String, String>> hmsetWriter = items -> {
            for (Map<String, String> item : items) {
                sourceSync.hmset(item.get(Beers.FIELD_ID), item);
            }
        };
        execute("scan-reader-populate", fileReader, hmsetWriter);
        DataStructureItemReader<String, String> reader = DataStructureItemReader.builder(sourcePool, sourceConnection).build();
        ListItemWriter<DataStructure<String>> writer = new ListItemWriter<>();
        JobExecution execution = execute("scan-reader", reader, writer);
        Assert.assertTrue(execution.getAllFailureExceptions().isEmpty());
        Assert.assertEquals(Beers.SIZE, writer.getWrittenItems().size());
    }

    @Test
    public void testStreamReader() throws Exception {
        DataGenerator.builder().dataType(DataType.STREAM).pool(sourcePool).end(100).build().call();
        StreamItemReader<String, String> reader = StreamItemReader.builder(sourceConnection).offset(StreamOffset.from("stream:0", "0-0")).build();
        reader.setMaxItemCount(10);
        ListItemWriter<StreamMessage<String, String>> writer = new ListItemWriter<>();
        JobExecution execution = executeFlushing("stream-reader", reader, writer);
        awaitJobTermination(execution);
        Assertions.assertEquals(10, writer.getWrittenItems().size());
        List<? extends StreamMessage<String, String>> items = writer.getWrittenItems();
        for (StreamMessage<String, String> message : items) {
            Assertions.assertTrue(message.getBody().containsKey("field1"));
            Assertions.assertTrue(message.getBody().containsKey("field2"));
        }
    }

    @Test
    public void testStreamWriter() throws Exception {
        String stream = "stream:0";
        List<Map<String, String>> messages = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            Map<String, String> body = new HashMap<>();
            body.put("field1", "value1");
            body.put("field2", "value2");
            messages.add(body);
        }
        ListItemReader<Map<String, String>> reader = new ListItemReader<>(messages);
        BiFunction<RedisStreamAsyncCommands<String, String>, Map<String, String>, RedisFuture<?>> command = CommandBuilder.<Map<String, String>>xadd().keyConverter(i -> stream).bodyConverter(i -> i).build();
        CommandItemWriter writer = CommandItemWriter.builder(targetPool, (BiFunction) command).build();
        execute("stream-writer", reader, writer);
        Assertions.assertEquals(messages.size(), targetSync.xlen(stream));
        List<StreamMessage<String, String>> xrange = targetSync.xrange(stream, Range.create("-", "+"));
        for (int index = 0; index < xrange.size(); index++) {
            StreamMessage<String, String> message = xrange.get(index);
            Assertions.assertEquals(messages.get(index), message.getBody());
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testHashWriter() throws Exception {
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
        BiFunction<RedisHashAsyncCommands<String, String>, Map<String, String>, RedisFuture<?>> hmset = CommandBuilder.<Map<String, String>>hmset().keyConverter(keyConverter).mapConverter(m -> m).build();
        CommandItemWriter writer = CommandItemWriter.builder(targetPool, (BiFunction) hmset).build();
        execute("hash-writer", reader, writer);
        Assertions.assertEquals(maps.size(), targetSync.keys("hash:*").size());
        for (int index = 0; index < maps.size(); index++) {
            Map<String, String> hash = targetSync.hgetall("hash:" + index);
            Assertions.assertEquals(maps.get(index), hash);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSortedSetWriter() throws Exception {
        List<ScoredValue<String>> values = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            values.add(ScoredValue.fromNullable(index % 10, String.valueOf(index)));
        }
        ListItemReader<ScoredValue<String>> reader = new ListItemReader<>(values);
        KeyMaker<ScoredValue<String>> keyConverter = KeyMaker.<ScoredValue<String>>builder().prefix("zset").build();
        BiFunction<RedisSortedSetAsyncCommands<String, String>, ScoredValue<String>, RedisFuture<?>> command = CommandBuilder.<ScoredValue<String>>zadd().keyConverter(keyConverter).memberIdConverter(Value::getValue).scoreConverter(ScoredValue::getScore).build();
        CommandItemWriter writer = CommandItemWriter.builder(targetPool, (BiFunction) command).build();
        execute("sorted-set-writer", reader, writer);
        Assertions.assertEquals(1, targetSync.dbsize());
        Assertions.assertEquals(values.size(), targetSync.zcard("zset"));
        List<String> range = targetSync.zrangebyscore("zset", Range.from(Range.Boundary.including(0), Range.Boundary.including(5)));
        Assertions.assertEquals(60, range.size());
    }

    @Test
    public void testDataStructureWriter() throws Exception {
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
        DataStructureItemWriter<String, String> writer = DataStructureItemWriter.builder(targetPool).build();
        execute("value-writer", reader, writer);
        List<String> keys = targetSync.keys("hash:*");
        Assertions.assertEquals(count, keys.size());
    }


    @Test
    public void testLiveReader() throws Exception {
        try (StatefulRedisPubSubConnection<String, String> pubSubConnection = sourceRedisClient.connectPubSub()) {
            KeyDumpItemReader<String, String> reader = KeyDumpItemReader.builder(sourcePool, pubSubConnection).idleTimeout(Duration.ofMillis(500)).build();
            ListItemWriter<KeyValue<String, byte[]>> writer = new ListItemWriter<>();
            JobExecution execution = executeFlushing("live-reader", reader, writer);
            DataGenerator.builder().pool(sourcePool).end(123).maxExpire(0).dataType(DataType.STRING).dataType(DataType.HASH).build().call();
            awaitJobTermination(execution);
            Assertions.assertEquals(sourceSync.dbsize(), writer.getWrittenItems().size());
        }
    }

    @Test
    public void testReplication() throws Exception {
        DataGenerator.builder().pool(sourcePool).end(10000).build().call();
        KeyDumpItemReader<String, String> reader = KeyDumpItemReader.builder(sourcePool, sourceConnection).build();
        KeyDumpItemWriter<String, String> writer = KeyDumpItemWriter.builder(targetPool).replace(true).build();
        execute("replication", reader, writer);
        compare("replication");
    }

    @Test
    public void testLiveReplication() throws Exception {
        DataGenerator.builder().pool(sourcePool).end(10000).build().call();
        KeyDumpItemReader<String, String> reader = KeyDumpItemReader.builder(sourcePool, sourceConnection).build();
        reader.setName("reader");
        KeyDumpItemWriter<String, String> writer = KeyDumpItemWriter.builder(targetPool).replace(true).build();
        writer.setName("writer");
        TaskletStep replicationStep = step("replication", reader, writer).build();
        try (StatefulRedisPubSubConnection<String, String> pubSubConnection = sourceRedisClient.connectPubSub()) {
            KeyDumpItemReader<String, String> liveReader = KeyDumpItemReader.builder(sourcePool, pubSubConnection).idleTimeout(Duration.ofMillis(500)).build();
            liveReader.setName("live-reader");
            KeyDumpItemWriter liveWriter = KeyDumpItemWriter.builder(targetPool).replace(true).build();
            liveWriter.setName("live-writer");
            TaskletStep liveReplicationStep = flushing(step("live-replication", liveReader, liveWriter)).build();
            SimpleFlow replicationFlow = new FlowBuilder<SimpleFlow>("replication-flow").start(replicationStep).build();
            SimpleFlow liveReplicationFlow = new FlowBuilder<SimpleFlow>("live-replication-flow").start(liveReplicationStep).build();
            Job job = jobs.get("live-replication-job").start(new FlowBuilder<SimpleFlow>("live-replication-flow").split(new SimpleAsyncTaskExecutor()).add(replicationFlow, liveReplicationFlow).build()).build().build();
            JobExecution execution = asyncJobLauncher.run(job, new JobParameters());
            awaitRunning(execution);
            DataGenerator.builder().pool(sourcePool).end(123).build().call();
            awaitJobTermination(execution);
            compare("live-replication");
        }
    }

    private void awaitRunning(JobExecution execution) throws InterruptedException {
        while (!execution.isRunning()) {
            Thread.sleep(10);
        }
    }

    private void compare(String name) throws Exception {
        Assert.assertEquals(sourceSync.dbsize(), targetSync.dbsize());
        DataStructureItemReader<String, String> left = DataStructureItemReader.builder(sourcePool, sourceConnection).build();
        KeyComparisonItemWriter<String, String> writer = new KeyComparisonItemWriter<>(DataStructureItemReader.builder(targetPool, targetConnection).build(), Duration.ofSeconds(1));
        execute(name + "-compare", left, writer);
        Assertions.assertTrue(writer.getOkCount() == sourceSync.dbsize());
        writer.getDiffs().forEach((k, v) -> Assertions.assertTrue(v.isEmpty()));
    }

    private Job job(String name, TaskletStep step) {
        return jobs.get(name + "-job").start(step).build();
    }

    @Test
    public void testMetrics() throws Exception {
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
        DataGenerator.builder().pool(sourcePool).end(100).build().call();
        DataStructureItemReader reader = DataStructureItemReader.builder(sourcePool, sourceConnection).queueCapacity(10).chunkSize(1).build();
        ThrottledWriter<DataStructure<String>> writer = ThrottledWriter.<DataStructure<String>>builder().build();
        TaskletStep step = steps.get("metrics-step").<DataStructure<String>, DataStructure<String>>chunk(1).reader(reader).writer(writer).build();
        Job job = jobs.get("metrics-job").start(step).build();
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

    @Test
    public void testKeyReaderSize() throws Exception {
        DataGenerator.builder().pool(sourcePool).end(1234).build().call();
        ScanKeyItemReader<String, String> keyReader = (ScanKeyItemReader<String, String>) DataStructureItemReader.builder(sourcePool, sourceConnection).build().getKeyReader();
        keyReader.open(new ExecutionContext());
        Long estimated = keyReader.size();
        Assertions.assertEquals(sourceSync.dbsize(), estimated);
        keyReader.close();
    }

}
