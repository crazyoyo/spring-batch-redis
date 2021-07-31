package org.springframework.batch.item.redis;

import com.redislabs.testcontainers.RedisServer;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.Value;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import io.lettuce.core.api.sync.RedisHashCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import io.lettuce.core.api.sync.RedisSortedSetCommands;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.StringCodec;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.search.Search;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.redis.support.AbstractKeyspaceNotificationItemReader;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.KeyMaker;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.KeyValueItemReader;
import org.springframework.batch.item.redis.support.LiveKeyValueItemReader;
import org.springframework.batch.item.redis.support.PollableItemReader;
import org.springframework.batch.item.redis.support.RedisClusterKeyspaceNotificationItemReader;
import org.springframework.batch.item.redis.support.RedisKeyspaceNotificationItemReader;
import org.springframework.batch.item.redis.support.operation.Hset;
import org.springframework.batch.item.redis.support.operation.NullValuePredicate;
import org.springframework.batch.item.redis.support.operation.Xadd;
import org.springframework.batch.item.redis.support.operation.Zadd;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

@Slf4j
@SuppressWarnings("unchecked")
public class BatchTests extends TestBase {

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
        KeyValueItemReader<DataStructure> reader = DataStructureItemReader.client(redisClient(REDIS)).queueCapacity(10).chunkSize(1).build();
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

    private PollableItemReader<String> keyEventReader(RedisServer server) {
        int queueCapacity = KeyValueItemReader.LiveKeyValueItemReaderBuilder.DEFAULT_QUEUE_CAPACITY;
        if (server.isCluster()) {
            RedisClusterClient client = redisClusterClient(server);
            return new RedisClusterKeyspaceNotificationItemReader(client::connectPubSub, KeyValueItemReader.LiveKeyValueItemReaderBuilder.DEFAULT_PUBSUB_PATTERNS, queueCapacity);
        }
        RedisClient client = redisClient(server);
        return new RedisKeyspaceNotificationItemReader(client::connectPubSub, KeyValueItemReader.LiveKeyValueItemReaderBuilder.DEFAULT_PUBSUB_PATTERNS, queueCapacity);
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
        if (server.isCluster()) {
            RedisClusterClient client = redisClusterClient(server);
            return new RedisClusterKeyspaceNotificationItemReader(client::connectPubSub, KeyValueItemReader.LiveKeyValueItemReaderBuilder.DEFAULT_PUBSUB_PATTERNS, queue);
        }
        RedisClient client = redisClient(server);
        return new RedisKeyspaceNotificationItemReader(client::connectPubSub, KeyValueItemReader.LiveKeyValueItemReaderBuilder.DEFAULT_PUBSUB_PATTERNS, queue);
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
        OperationItemWriter<String, String, Map<String, String>> writer = operationWriter(redisServer, new Xadd<>(stream, i -> i));
        execute(redisServer, "stream-writer", reader, writer);
        RedisStreamCommands<String, String> sync = sync(redisServer);
        Assertions.assertEquals(messages.size(), sync.xlen(stream));
        List<StreamMessage<String, String>> xrange = sync.xrange(stream, Range.create("-", "+"));
        for (int index = 0; index < xrange.size(); index++) {
            StreamMessage<String, String> message = xrange.get(index);
            Assertions.assertEquals(messages.get(index), message.getBody());
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
        OperationItemWriter<String, String, Map<String, String>> writer = OperationItemWriter.operation(new Xadd<String, String, Map<String, String>>(stream, i -> i)).codec(StringCodec.UTF8).client(redisClient(REDIS)).transactional(true).build();
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
        OperationItemWriter<String, String, Map<String, String>> writer = operationWriter(server, new Hset<>(keyConverter, m -> m));
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
        OperationItemWriter<String, String, Map.Entry<String, Map<String, String>>> writer = operationWriter(server, new Hset<>(keyConverter, Map.Entry::getValue, new NullValuePredicate<>(Map.Entry::getValue)));
        execute(server, "hash-del-writer", reader, writer);
        Assertions.assertEquals(50, sync.keys("hash:*").size());
        Assertions.assertEquals(2, commands.hgetall("hash:50").size());
    }

    private <T> OperationItemWriter<String, String, T> operationWriter(RedisServer server, OperationItemWriter.RedisOperation<String, String, T> operation) {
        if (server.isCluster()) {
            return OperationItemWriter.operation(operation).codec(StringCodec.UTF8).client(redisClusterClient(server)).build();
        }
        return OperationItemWriter.operation(operation).codec(StringCodec.UTF8).client(redisClient(server)).build();
    }

    @ParameterizedTest
    @MethodSource("servers")
    public void testSortedSetWriter(RedisServer server) throws Throwable {
        List<ScoredValue<String>> values = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            values.add((ScoredValue<String>) ScoredValue.fromNullable(index % 10, String.valueOf(index)));
        }
        ListItemReader<ScoredValue<String>> reader = new ListItemReader<>(values);
        OperationItemWriter<String, String, ScoredValue<String>> writer = operationWriter(server, new Zadd<>("zset", Value::getValue, ScoredValue::getScore));
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
        dataGenerator(server).end(123).maxExpire(Duration.ofMillis(0)).dataTypes(DataStructure.STRING, DataStructure.HASH).build().call();
        awaitJobTermination(execution);
        RedisServerCommands<String, String> sync = sync(server);
        Assertions.assertEquals(sync.dbsize(), writer.getWrittenItems().size());
    }

}
