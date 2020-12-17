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
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.search.Search;
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
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ExecutionContext;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.BufferedReader;
import java.io.IOException;
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
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;

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
        StatefulRedisPubSubConnection<String, String> pubSubConnection = sourceRedisClient.connectPubSub();
        RedisKeyspaceNotificationItemReader<String, String> reader = new RedisKeyspaceNotificationItemReader<>(pubSubConnection, KeyspaceNotificationReaderOptions.builder().build());
        ListItemWriter<String> writer = new ListItemWriter<>();
        StepBuilder stepBuilder = stepBuilderFactory.get("polling-step");
        TaskletStep step = new FlushingStepBuilder<String, String>(stepBuilder).chunk(50).reader(reader).writer(writer).build();
        Job job = jobBuilderFactory.get("polling-job").start(step).build();
        JobExecution execution = asyncJobLauncher.run(job, new JobParameters());
        Thread.sleep(100);
        DataGenerator.builder().pool(sourcePool).end(3).build().run();
        Thread.sleep(100);
        Assertions.assertEquals(7, writer.getWrittenItems().size());
        execution.stop();
    }

    @Test
    public void testKeyValueItemReaderProgress() throws InterruptedException {
        DataGenerator.builder().pool(sourcePool).end(100).expire(false).build().run();
        RedisKeyItemReader<String, String> keyReader = RedisKeyItemReader.<String, String>builder().connection(sourceConnection).build();
        RedisDataStructureItemReader<String, String> reader = RedisDataStructureItemReader.<String, String>builder().pool(sourcePool).keyReader(keyReader).build();
        reader.open(new ExecutionContext());
        do {
            Thread.sleep(10);
        }
        while (!reader.isRunning());
        long total = keyReader.size();
        Assertions.assertEquals(sourceSync.dbsize(), total, 10);
        reader.close();
    }

    @Test
    public void testDataStructureReader() throws Exception {
        FlatFileItemReader<Map<String, String>> fileReader = fileReader(new ClassPathResource("beers.csv"));
        ItemWriter<Map<String, String>> hmsetWriter = items -> {
            for (Map<String, String> item : items) {
                sourceSync.hmset(item.get(Beers.FIELD_ID), item);
            }
        };
        run("scan-reader-populate", fileReader, hmsetWriter);
        RedisDataStructureItemReader<String, String> reader = RedisDataStructureItemReader.<String, String>builder().pool(sourcePool).keyReader(RedisKeyItemReader.<String, String>builder().connection(sourceConnection).build()).build();
        ListItemWriter<DataStructure<String>> writer = new ListItemWriter<>();
        JobExecution execution = run("scan-reader", reader, writer);
        Assert.assertTrue(execution.getAllFailureExceptions().isEmpty());
        Assert.assertEquals(Beers.SIZE, writer.getWrittenItems().size());
    }

    @Test
    public void testStreamReader() throws Exception {
        DataGenerator.builder().pool(sourcePool).end(100).build().run();
        AbstractStreamItemReader reader = RedisStreamItemReader.<String, String>builder().connection(sourceConnection).offset(StreamOffset.from("stream:0", "0-0")).build();
        reader.setMaxItemCount(10);
        ListItemWriter<StreamMessage<String, String>> writer = new ListItemWriter<>();
        run("stream-reader", reader, writer);
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
        BiFunction<RedisStreamAsyncCommands<String, String>, Map<String, String>, RedisFuture<?>> command = CommandBuilder.<String, String, Map<String, String>>xadd().keyConverter(i -> stream).bodyConverter(i -> i).build();
        RedisCommandItemWriter writer = RedisCommandItemWriter.<String, String, Map<String, String>>builder().pool(targetPool).command(command).build();
        run("stream-writer", reader, writer);
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
        BiFunction<RedisHashAsyncCommands<String, String>, Map<String, String>, RedisFuture<?>> hmset = CommandBuilder.<String, String, Map<String, String>>hmset().keyConverter(keyConverter).mapConverter(m -> m).build();
        RedisCommandItemWriter writer = RedisCommandItemWriter.<String, String, Map<String, String>>builder().pool(targetPool).command(hmset).build();
        run("hash-writer", reader, writer);
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
        BiFunction<RedisSortedSetAsyncCommands<String, String>, ScoredValue<String>, RedisFuture<?>> command = CommandBuilder.<String, String, ScoredValue<String>>zadd().keyConverter(keyConverter).memberIdConverter(v -> v.getValue()).scoreConverter(v -> v.getScore()).build();
        RedisCommandItemWriter writer = RedisCommandItemWriter.<String, String, ScoredValue<String>>builder().pool(targetPool).command(command).build();
        run("sorted-set-writer", reader, writer);
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
        RedisDataStructureItemWriter<String, String> writer = RedisDataStructureItemWriter.<String, String>builder().pool(targetPool).build();
        run("value-writer", reader, writer);
        List<String> keys = targetSync.keys("hash:*");
        Assertions.assertEquals(count, keys.size());
    }

    @Test
    public void testReplication() throws Exception {
        DataGenerator.builder().pool(sourcePool).end(10000).build().run();
        RedisKeyDumpItemReader<String, String> reader = RedisKeyDumpItemReader.<String, String>builder().pool(sourcePool).keyReader(RedisKeyItemReader.<String, String>builder().connection(sourceConnection).build()).build();
        RedisKeyDumpItemWriter<String, String> writer = RedisKeyDumpItemWriter.<String, String>builder().pool(targetPool).replace(true).build();
        run("replication", reader, writer);
        compare();
    }

    @Test
    public void testLiveReplication() throws Exception {
        StatefulRedisPubSubConnection<String, String> pubSubConnection = sourceRedisClient.connectPubSub();
        RedisKeyspaceNotificationItemReader<String, String> keyReader = RedisKeyspaceNotificationItemReader.<String, String>builder().connection(pubSubConnection).options(KeyspaceNotificationReaderOptions.builder().build()).build();
        RedisKeyDumpItemReader<String, String> reader = RedisKeyDumpItemReader.<String, String>builder().pool(sourcePool).keyReader(keyReader).build();
        RedisKeyDumpItemWriter writer = RedisKeyDumpItemWriter.<String, String>builder().pool(targetPool).replace(true).build();
        Job job = flushingJob("live-replication", reader, writer);
        JobExecution execution = asyncJobLauncher.run(job, new JobParameters());
        Thread.sleep(100);
        DataGenerator.builder().pool(sourcePool).end(2).sleep(1L).build().run();
        Thread.sleep(100);
        log.info("Stopping keyspace notification reader");
        keyReader.stop();
        log.info("Waiting for job to complete");
        while (execution.isRunning()) {
            log.info("Job execution status: {} - stopping: {}", execution.getStatus(), execution.isStopping());
            Thread.sleep(3000);
        }
        log.info("Comparing");
        compare();
    }

    private void compare() throws Exception {
        Assert.assertEquals(sourceSync.dbsize(), targetSync.dbsize());
        RedisDataStructureItemReader<String, String> left = RedisDataStructureItemReader.<String, String>builder().pool(sourcePool).keyReader(RedisKeyItemReader.<String, String>builder().connection(sourceConnection).build()).build();
        RedisDataStructureItemReader<String, String> right = RedisDataStructureItemReader.<String, String>builder().pool(targetPool).keyReader(RedisKeyItemReader.<String, String>builder().connection(targetConnection).build()).build();
        DatabaseComparator<String, String> comparator = DatabaseComparator.<String, String>builder().left(left).right(right).build();
        DatabaseComparison comparison = comparator.execute();
        Assertions.assertTrue(comparison.isIdentical());
    }

    private <T> JobExecution run(String name, ItemReader<T> reader, ItemWriter<T> writer) throws Exception {
        return jobLauncher.run(job(name, reader, writer), new JobParameters());
    }

    private <I, O> Job job(String name, ItemReader<? extends I> reader, ItemWriter<? super O> writer) {
        return jobBuilderFactory.get(name + "-job").start(stepBuilderFactory.get(name + "-step").<I, O>chunk(50).reader(reader).writer(writer).build()).build();
    }

    private <I, O> Job flushingJob(String name, PollableItemReader<? extends I> reader, ItemWriter<? super O> writer) {
        return jobBuilderFactory.get(name + "-job").start(new FlushingStepBuilder<I, O>(stepBuilderFactory.get(name + "-step")).<I, O>chunk(50).reader(reader).writer(writer).build()).build();
    }

    @Test
    public void testMetrics() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Metrics.addRegistry(registry);
        DataGenerator.builder().pool(sourcePool).end(10).build().run();
        RedisDataStructureItemReader reader = RedisDataStructureItemReader.<String, String>builder().pool(sourcePool).keyReader(RedisKeyItemReader.<String, String>builder().connection(sourceConnection).build()).build();
        run("metrics", reader, ThrottledWriter.<DataStructure>builder().build());
        Search search = registry.find(MetricsUtils.METRICS_PREFIX + "item.read");
        Assertions.assertFalse(search.timers().isEmpty());
        log.info("Job completed");
    }

}
