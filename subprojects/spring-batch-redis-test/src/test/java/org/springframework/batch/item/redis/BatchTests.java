package org.springframework.batch.item.redis;

import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.testcontainers.RedisServer;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.GeoArgs;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import io.lettuce.core.api.sync.RedisGeoCommands;
import io.lettuce.core.api.sync.RedisHashCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import io.lettuce.core.api.sync.RedisSortedSetCommands;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.search.Search;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.Builder;
import lombok.Data;
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
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.redis.support.AbstractKeyspaceNotificationItemReader;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.DataStructureValueReader;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.KeyValueItemReader;
import org.springframework.batch.item.redis.support.LiveKeyValueItemReader;
import org.springframework.batch.item.redis.support.PollableItemReader;
import org.springframework.batch.item.redis.support.RedisClusterKeyspaceNotificationItemReader;
import org.springframework.batch.item.redis.support.RedisKeyspaceNotificationItemReader;
import org.springframework.batch.item.redis.support.convert.GeoValueConverter;
import org.springframework.batch.item.redis.support.convert.KeyMaker;
import org.springframework.batch.item.redis.support.convert.MapFlattener;
import org.springframework.batch.item.redis.support.operation.Geoadd;
import org.springframework.batch.item.redis.support.operation.Hset;
import org.springframework.batch.item.redis.support.operation.Xadd;
import org.springframework.batch.item.redis.support.operation.Zadd;
import org.springframework.batch.item.redis.test.Beer;
import org.springframework.batch.item.redis.test.DataGenerator;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@SuppressWarnings("unchecked")
public class BatchTests extends AbstractRedisTestBase {

    @ParameterizedTest
    @MethodSource("servers")
    void testFlushingStep(RedisServer redis) throws Exception {
        PollableItemReader<String> reader = keyEventReader(redis);
        ListItemWriter<String> writer = new ListItemWriter<>();
        JobExecution execution = executeFlushing(name(redis, "flushing"), reader, writer);
        dataGenerator(redis).end(3).maxExpire(Duration.ofMillis(0)).dataTypes(DataStructure.STRING, DataStructure.HASH).build().call();
        awaitJobTermination(execution);
        RedisServerCommands<String, String> commands = sync(redis);
        Assertions.assertEquals(commands.dbsize(), writer.getWrittenItems().size());
    }

    @Test
    @SuppressWarnings("NullableProblems")
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
        dataGenerator(REDIS).end(100).build().call();
        KeyValueItemReader<DataStructure> reader = DataStructureItemReader.client(redisClient(REDIS)).queueCapacity(10).chunkSize(1).build();
        ItemWriter<DataStructure> writer = items -> Thread.sleep(1);
        TaskletStep step = steps.get("metrics-step").<DataStructure, DataStructure>chunk(1).reader(reader).writer(writer).build();
        Job job = job(name(REDIS, "metrics-job"), step);
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

    private PollableItemReader<String> keyEventReader(RedisServer redis) {
        int queueCapacity = KeyValueItemReader.LiveKeyValueItemReaderBuilder.DEFAULT_QUEUE_CAPACITY;
        if (redis.isCluster()) {
            RedisModulesClusterClient client = redisClusterClient(redis);
            return new RedisClusterKeyspaceNotificationItemReader(client::connectPubSub, KeyValueItemReader.LiveKeyValueItemReaderBuilder.DEFAULT_PUBSUB_PATTERNS, queueCapacity);
        }
        RedisClient client = redisClient(redis);
        return new RedisKeyspaceNotificationItemReader(client::connectPubSub, KeyValueItemReader.LiveKeyValueItemReaderBuilder.DEFAULT_PUBSUB_PATTERNS, queueCapacity);
    }

    @ParameterizedTest
    @MethodSource("servers")
    void testKeyspaceNotificationReader(RedisServer redis) throws Exception {
        BlockingQueue<String> queue = new LinkedBlockingDeque<>(10000);
        AbstractKeyspaceNotificationItemReader<?> reader = keyspaceNotificationReader(redis, queue);
        reader.open(new ExecutionContext());
        BaseRedisAsyncCommands<String, String> async = async(redis);
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
            RedisModulesClusterClient client = redisClusterClient(server);
            return new RedisClusterKeyspaceNotificationItemReader(client::connectPubSub, KeyValueItemReader.LiveKeyValueItemReaderBuilder.DEFAULT_PUBSUB_PATTERNS, queue);
        }
        RedisClient client = redisClient(server);
        return new RedisKeyspaceNotificationItemReader(client::connectPubSub, KeyValueItemReader.LiveKeyValueItemReaderBuilder.DEFAULT_PUBSUB_PATTERNS, queue);
    }


    @ParameterizedTest
    @MethodSource("servers")
    void testDataStructureReader(RedisServer redis) throws Exception {
        populateSource("scan-reader-populate", redis);
        KeyValueItemReader<DataStructure> reader = dataStructureReader(redis);
        ListItemWriter<DataStructure> writer = new ListItemWriter<>();
        JobExecution execution = execute(name(redis, "scan-reader"), reader, null, writer);
        Assertions.assertTrue(execution.getAllFailureExceptions().isEmpty());
        RedisServerCommands<String, String> sync = sync(redis);
        Assertions.assertEquals(sync.dbsize(), writer.getWrittenItems().size());
    }

    private void populateSource(String name, RedisServer server) throws Exception {
        JsonItemReader<Map<String, Object>> reader = Beer.mapReader();
        OperationItemWriter<String, String, Map<String, String>> writer = OperationItemWriter.client(client(server)).operation(Hset.<String, Map<String, String>>key(t -> t.get("id")).map(t -> t).build()).build();
        execute(name(server, name), reader, new MapFlattener(), writer);
    }

    @ParameterizedTest
    @MethodSource("servers")
    void testMultiThreadedReader(RedisServer server) throws Exception {
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
        JobExecution execution = execute(name(server, name), step(name, synchronizedReader, writer).taskExecutor(taskExecutor).throttleLimit(threads).build());
        Assertions.assertTrue(execution.getAllFailureExceptions().isEmpty());
        RedisServerCommands<String, String> sync = sync(server);
        Assertions.assertEquals(sync.dbsize(), writer.getWrittenItems().size());
    }

    private static class SynchronizedListItemWriter<T> implements ItemWriter<T> {

        private final List<T> writtenItems = Collections.synchronizedList(new ArrayList<>());

        @SuppressWarnings("NullableProblems")
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
    void testStreamWriter(RedisServer redis) throws Exception {
        String stream = "stream:0";
        List<Map<String, String>> messages = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            Map<String, String> body = new HashMap<>();
            body.put("field1", "value1");
            body.put("field2", "value2");
            messages.add(body);
        }
        ListItemReader<Map<String, String>> reader = new ListItemReader<>(messages);
        OperationItemWriter<String, String, Map<String, String>> writer = OperationItemWriter.client(client(redis)).operation(Xadd.<String, Map<String, String>>key(stream).body(t -> t).build()).build();
        execute(name(redis, "stream-writer"), reader, writer);
        RedisStreamCommands<String, String> sync = sync(redis);
        Assertions.assertEquals(messages.size(), sync.xlen(stream));
        List<StreamMessage<String, String>> xrange = sync.xrange(stream, Range.create("-", "+"));
        for (int index = 0; index < xrange.size(); index++) {
            StreamMessage<String, String> message = xrange.get(index);
            Assertions.assertEquals(messages.get(index), message.getBody());
        }
    }

    @Test
    public void testStreamTransactionWriter() throws Exception {
        String stream = "stream:1";
        List<Map<String, String>> messages = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            Map<String, String> body = new HashMap<>();
            body.put("field1", "value1");
            body.put("field2", "value2");
            messages.add(body);
        }
        ListItemReader<Map<String, String>> reader = new ListItemReader<>(messages);
        OperationItemWriter<String, String, Map<String, String>> writer = OperationItemWriter.client(client(REDIS)).operation(Xadd.<String, Map<String, String>>key(stream).body(t -> t).build()).transactional().build();
        execute(name(REDIS, "stream-tx-writer"), reader, writer);
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
    public void testHashWriter(RedisServer server) throws Exception {
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
        OperationItemWriter<String, String, Map<String, String>> writer = OperationItemWriter.client(client(server)).operation(Hset.key(keyConverter).map(m -> m).build()).build();
        execute(name(server, "hash-writer"), reader, writer);
        RedisKeyCommands<String, String> sync = sync(server);
        Assertions.assertEquals(maps.size(), sync.keys("hash:*").size());
        RedisHashCommands<String, String> hashCommands = sync(server);
        for (int index = 0; index < maps.size(); index++) {
            Map<String, String> hash = hashCommands.hgetall("hash:" + index);
            Assertions.assertEquals(maps.get(index), hash);
        }
    }

    @Data
    @Builder
    private static class Geo {
        private final String member;
        private final double longitude;
        private final double latitude;
    }

    @ParameterizedTest
    @MethodSource("servers")
    public void testGeoaddWriter(RedisServer redis) throws Exception {
        AbstractRedisClient client = client(redis);
        ListItemReader<Geo> reader = new ListItemReader<>(Arrays.asList(Geo.builder().longitude(-118.476056).latitude(33.985728).member("Venice Breakwater").build(), Geo.builder().longitude(-73.667022).latitude(40.582739).member("Long Beach National").build()));
        OperationItemWriter<String, String, Geo> writer = OperationItemWriter.client(client).operation(Geoadd.<Geo>key("geoset").value(new GeoValueConverter<>(Geo::getMember, Geo::getLongitude, Geo::getLatitude)).build()).build();
        execute(name(redis, "geoadd-writer"), reader, writer);
        RedisGeoCommands<String, String> sync = sync(redis);
        Set<String> radius1 = sync.georadius("geoset", -118, 34, 100, GeoArgs.Unit.mi);
        Assertions.assertEquals(1, radius1.size());
        Assertions.assertTrue(radius1.contains("Venice Breakwater"));
    }

    @ParameterizedTest
    @MethodSource("servers")
    public void testHashDelWriter(RedisServer server) throws Exception {
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
        OperationItemWriter<String, String, Map.Entry<String, Map<String, String>>> writer = OperationItemWriter.client(client(server)).operation(Hset.key(keyConverter).map(Map.Entry::getValue).build()).build();
        execute(name(server, "hash-del-writer"), reader, writer);
        Assertions.assertEquals(50, sync.keys("hash:*").size());
        Assertions.assertEquals(2, commands.hgetall("hash:50").size());
    }

    @ParameterizedTest
    @MethodSource("servers")
    public void testSortedSetWriter(RedisServer server) throws Exception {
        List<ScoredValue<String>> values = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            values.add((ScoredValue<String>) ScoredValue.fromNullable(index % 10, String.valueOf(index)));
        }
        ListItemReader<ScoredValue<String>> reader = new ListItemReader<>(values);
        OperationItemWriter<String, String, ScoredValue<String>> writer = OperationItemWriter.client(client(server)).operation(Zadd.<ScoredValue<String>>key("zset").value(v -> v).build()).build();
        execute(name(server, "sorted-set-writer"), reader, writer);
        RedisServerCommands<String, String> sync = sync(server);
        Assertions.assertEquals(1, sync.dbsize());
        Assertions.assertEquals(values.size(), ((RedisSortedSetCommands<String, String>) sync).zcard("zset"));
        List<String> range = ((RedisSortedSetCommands<String, String>) sync).zrangebyscore("zset", Range.from(Range.Boundary.including(0), Range.Boundary.including(5)));
        Assertions.assertEquals(60, range.size());
    }

    @ParameterizedTest
    @MethodSource("servers")
    public void testDataStructureWriter(RedisServer redis) throws Exception {
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
        DataStructureItemWriter<DataStructure> writer = dataStructureWriter(redis);
        execute(name(redis, "value-writer"), reader, writer);
        RedisKeyCommands<String, String> sync = sync(redis);
        List<String> keys = sync.keys("hash:*");
        Assertions.assertEquals(count, keys.size());
    }

    @ParameterizedTest
    @MethodSource("servers")
    public void testLiveReader(RedisServer redis) throws Exception {
        LiveKeyValueItemReader<KeyValue<byte[]>> reader = liveKeyDumpReader(redis);
        ListItemWriter<KeyValue<byte[]>> writer = new ListItemWriter<>();
        JobExecution execution = executeFlushing(name(redis, "live-reader"), reader, writer);
        Thread.sleep(100);
        dataGenerator(redis).end(123).maxExpire(Duration.ofMillis(0)).dataTypes(DataStructure.STRING, DataStructure.HASH).build().call();
        awaitJobTermination(execution);
        RedisServerCommands<String, String> sync = sync(redis);
        Assertions.assertEquals(sync.dbsize(), writer.getWrittenItems().size());
    }

    @ParameterizedTest
    @MethodSource("servers")
    public void testKeyValueItemReaderFaultTolerance(RedisServer redis) throws Exception {
        dataGenerator(redis).end(1000).dataTypes(DataStructure.STRING).build().call();
        List<String> keys = IntStream.range(0, 100).boxed().map(DataGenerator::stringKey).collect(Collectors.toList());
        DelegatingPollableItemReader<String> keyReader = DelegatingPollableItemReader.<String>builder().delegate(new ListItemReader<>(keys)).exceptionSupplier(TimeoutException::new).interval(2).build();
        DataStructureValueReader valueReader = dataStructureValueReader(redis);
        KeyValueItemReader<DataStructure> reader = new KeyValueItemReader<>(keyReader, valueReader, 1, 1, 1000, Duration.ofMillis(100));
        ListItemWriter<DataStructure> writer = new ListItemWriter<>();
        execute(name(redis, "reader-ft"), reader, writer);
        Assertions.assertEquals(50, writer.getWrittenItems().size());
    }


}
