package com.redis.spring.batch.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.ListItemReader;

import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyComparison;
import com.redis.spring.batch.common.KeyComparison.Status;
import com.redis.spring.batch.common.KeyComparisonItemReader;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Range;
import com.redis.spring.batch.common.SimpleOperationExecutor;
import com.redis.spring.batch.common.ToGeoValueFunction;
import com.redis.spring.batch.common.ToSampleFunction;
import com.redis.spring.batch.common.ToScoredValueFunction;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.gen.MapOptions;
import com.redis.spring.batch.reader.KeyTypeItemReader;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader;
import com.redis.spring.batch.reader.ScanSizeEstimator;
import com.redis.spring.batch.reader.StreamItemReader;
import com.redis.spring.batch.reader.StreamItemReader.StreamAckPolicy;
import com.redis.spring.batch.reader.StructItemReader;
import com.redis.spring.batch.util.BatchUtils;
import com.redis.spring.batch.util.CodecUtils;
import com.redis.spring.batch.writer.OperationItemWriter;
import com.redis.spring.batch.writer.StructItemWriter;
import com.redis.spring.batch.writer.operation.Del;
import com.redis.spring.batch.writer.operation.Expire;
import com.redis.spring.batch.writer.operation.ExpireAt;
import com.redis.spring.batch.writer.operation.Geoadd;
import com.redis.spring.batch.writer.operation.Hset;
import com.redis.spring.batch.writer.operation.Lpush;
import com.redis.spring.batch.writer.operation.LpushAll;
import com.redis.spring.batch.writer.operation.Rpush;
import com.redis.spring.batch.writer.operation.Sadd;
import com.redis.spring.batch.writer.operation.TsAdd;
import com.redis.spring.batch.writer.operation.Xadd;
import com.redis.spring.batch.writer.operation.Zadd;

import io.lettuce.core.Consumer;
import io.lettuce.core.GeoArgs;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.models.stream.PendingMessages;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.search.Search;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

abstract class BatchTests extends AbstractTargetTestBase {

    @Test
    void writeWait(TestInfo info) throws Exception {
        List<Map<String, String>> maps = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            Map<String, String> body = new HashMap<>();
            body.put("id", String.valueOf(index));
            body.put("field1", "value1");
            body.put("field2", "value2");
            maps.add(body);
        }
        ListItemReader<Map<String, String>> reader = new ListItemReader<>(maps);
        Hset<String, String, Map<String, String>> hset = new Hset<>();
        hset.setKeyFunction(m -> "hash:" + m.remove("id"));
        hset.setMapFunction(Function.identity());
        OperationItemWriter<String, String, Map<String, String>> writer = writer(hset);
        writer.setWaitReplicas(1);
        writer.setWaitTimeout(Duration.ofMillis(300));
        JobExecution execution = run(info, reader, writer);
        List<Throwable> exceptions = execution.getAllFailureExceptions();
        assertEquals("Insufficient replication level (0/1)", exceptions.get(0).getCause().getCause().getMessage());
    }

    @Test
    void writeHash(TestInfo info) throws Exception {
        List<Map<String, String>> maps = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            Map<String, String> body = new HashMap<>();
            body.put("id", String.valueOf(index));
            body.put("field1", "value1");
            body.put("field2", "value2");
            maps.add(body);
        }
        ListItemReader<Map<String, String>> reader = new ListItemReader<>(maps);
        Hset<String, String, Map<String, String>> hset = new Hset<>();
        hset.setKeyFunction(m -> "hash:" + m.remove("id"));
        hset.setMapFunction(Function.identity());
        OperationItemWriter<String, String, Map<String, String>> writer = writer(hset);
        run(info, reader, writer);
        assertEquals(maps.size(), commands.keys("hash:*").size());
        for (int index = 0; index < maps.size(); index++) {
            Map<String, String> hash = commands.hgetall("hash:" + index);
            assertEquals(maps.get(index), hash);
        }
    }

    @Test
    void writeDel(TestInfo info) throws Exception {
        generate(info);
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(DEFAULT_GENERATOR_COUNT);
        Del<String, String, KeyValue<String>> del = new Del<>();
        del.setKeyFunction(KeyValue::getKey);
        run(info, gen, writer(del));
        assertEquals(0, commands.keys(GeneratorItemReader.DEFAULT_KEYSPACE + "*").size());
    }

    @Test
    void writeLpush(TestInfo info) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(DEFAULT_GENERATOR_COUNT);
        gen.setTypes(DataType.STRING);
        Lpush<String, String, KeyValue<String>> lpush = new Lpush<>();
        lpush.setKeyFunction(KeyValue::getKey);
        lpush.setValueFunction(v -> (String) v.getValue());
        run(info, gen, writer(lpush));
        assertEquals(DEFAULT_GENERATOR_COUNT, commands.dbsize());
        for (String key : commands.keys("*")) {
            assertEquals(DataType.LIST.getString(), commands.type(key));
        }
    }

    @Test
    void writeRpush(TestInfo info) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(DEFAULT_GENERATOR_COUNT);
        gen.setTypes(DataType.STRING);
        Rpush<String, String, KeyValue<String>> rpush = new Rpush<>();
        rpush.setKeyFunction(KeyValue::getKey);
        rpush.setValueFunction(v -> (String) v.getValue());
        run(info, gen, writer(rpush));
        assertEquals(DEFAULT_GENERATOR_COUNT, commands.dbsize());
        for (String key : commands.keys("*")) {
            assertEquals(DataType.LIST.getString(), commands.type(key));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void writeLpushAll(TestInfo info) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(DEFAULT_GENERATOR_COUNT);
        gen.setTypes(DataType.LIST);
        LpushAll<String, String, KeyValue<String>> lpushAll = new LpushAll<>();
        lpushAll.setKeyFunction(KeyValue::getKey);
        lpushAll.setValuesFunction(v -> (Collection<String>) v.getValue());
        run(info, gen, writer(lpushAll));
        assertEquals(DEFAULT_GENERATOR_COUNT, commands.dbsize());
        for (String key : commands.keys("*")) {
            assertEquals(DataType.LIST.getString(), commands.type(key));
        }
    }

    @Test
    void writeExpire(TestInfo info) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(DEFAULT_GENERATOR_COUNT);
        gen.setTypes(DataType.STRING);
        Duration ttl = Duration.ofMillis(1L);
        Expire<String, String, KeyValue<String>> expire = new Expire<>();
        expire.setKeyFunction(KeyValue::getKey);
        expire.setTtl(ttl);
        run(info, gen, writer(expire));
        awaitUntil(() -> commands.keys("*").isEmpty());
        assertEquals(0, commands.dbsize());
    }

    @Test
    void writeExpireAt(TestInfo info) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(DEFAULT_GENERATOR_COUNT);
        gen.setTypes(DataType.STRING);
        ExpireAt<String, String, KeyValue<String>> expireAt = new ExpireAt<>();
        expireAt.setKeyFunction(KeyValue::getKey);
        expireAt.setEpochFunction(v -> System.currentTimeMillis());
        run(info, gen, writer(expireAt));
        awaitUntil(() -> commands.keys("*").isEmpty());
        assertEquals(0, commands.dbsize());
    }

    private class Geo {

        private String member;

        private double longitude;

        private double latitude;

        public Geo(String member, double longitude, double latitude) {
            this.member = member;
            this.longitude = longitude;
            this.latitude = latitude;
        }

        public String getMember() {
            return member;
        }

        public double getLongitude() {
            return longitude;
        }

        public double getLatitude() {
            return latitude;
        }

    }

    @Test
    void writeGeo(TestInfo info) throws Exception {
        ListItemReader<Geo> reader = new ListItemReader<>(Arrays.asList(new Geo("Venice Breakwater", -118.476056, 33.985728),
                new Geo("Long Beach National", -73.667022, 40.582739)));
        Geoadd<String, String, Geo> geoadd = new Geoadd<>();
        geoadd.setKey("geoset");
        geoadd.setValueFunction(new ToGeoValueFunction<>(Geo::getMember, Geo::getLongitude, Geo::getLatitude));
        OperationItemWriter<String, String, Geo> writer = writer(geoadd);
        run(info, reader, writer);
        Set<String> radius1 = commands.georadius("geoset", -118, 34, 100, GeoArgs.Unit.mi);
        assertEquals(1, radius1.size());
        assertTrue(radius1.contains("Venice Breakwater"));
    }

    @Test
    void writeHashDel(TestInfo info) throws Exception {
        List<Entry<String, Map<String, String>>> hashes = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            String key = String.valueOf(index);
            Map<String, String> value = new HashMap<>();
            value.put("field1", "value1");
            commands.hset("hash:" + key, value);
            Map<String, String> body = new HashMap<>();
            body.put("field2", "value2");
            hashes.add(new AbstractMap.SimpleEntry<>(key, index < 50 ? null : body));
        }
        ListItemReader<Map.Entry<String, Map<String, String>>> reader = new ListItemReader<>(hashes);
        Hset<String, String, Entry<String, Map<String, String>>> hset = new Hset<>();
        hset.setKeyFunction(e -> "hash:" + e.getKey());
        hset.setMapFunction(Entry::getValue);
        OperationItemWriter<String, String, Entry<String, Map<String, String>>> writer = writer(hset);
        run(info, reader, writer);
        assertEquals(100, commands.keys("hash:*").size());
        assertEquals(2, commands.hgetall("hash:50").size());
    }

    private class ZValue {

        private String member;

        private double score;

        public ZValue(String member, double score) {
            super();
            this.member = member;
            this.score = score;
        }

        public String getMember() {
            return member;
        }

        public double getScore() {
            return score;
        }

    }

    @Test
    void writeZset(TestInfo info) throws Exception {
        String key = "zadd";
        List<ZValue> values = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            values.add(new ZValue(String.valueOf(index), index % 10));
        }
        ListItemReader<ZValue> reader = new ListItemReader<>(values);
        Zadd<String, String, ZValue> zadd = new Zadd<>();
        zadd.setKey(key);
        zadd.setValueFunction(new ToScoredValueFunction<>(ZValue::getMember, ZValue::getScore));
        OperationItemWriter<String, String, ZValue> writer = writer(zadd);
        run(info, reader, writer);
        assertEquals(1, commands.dbsize());
        assertEquals(values.size(), commands.zcard(key));
        assertEquals(60, commands.zrangebyscore(key, io.lettuce.core.Range.from(io.lettuce.core.Range.Boundary.including(0),
                io.lettuce.core.Range.Boundary.including(5))).size());
    }

    @Test
    void writeTimeseries() throws Exception {
        String key = "ts";
        Map<Long, Double> samples = new HashMap<>();
        for (int index = 0; index < 100; index++) {
            samples.put(Instant.now().toEpochMilli() + index, (double) index);
        }
        TsAdd<String, String, Entry<Long, Double>> tsAdd = new TsAdd<>();
        tsAdd.setKey(key);
        tsAdd.setSampleFunction(new ToSampleFunction<>(e -> e.getKey(), e -> e.getValue()));
        OperationItemWriter<String, String, Entry<Long, Double>> writer = writer(tsAdd);
        writer.open(new ExecutionContext());
        writer.write(new ArrayList<>(samples.entrySet()));
        assertEquals(1, commands.dbsize());
        writer.close();
    }

    @Test
    void writeMultiExec(TestInfo info) throws Exception {
        String key = "sadd";
        List<String> values = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            values.add(String.valueOf(index));
        }
        ListItemReader<String> reader = new ListItemReader<>(values);
        Sadd<String, String, String> sadd = new Sadd<>();
        sadd.setKey(key);
        sadd.setValueFunction(Function.identity());
        OperationItemWriter<String, String, String> writer = writer(sadd);
        writer.setMultiExec(true);
        run(info, reader, writer);
        assertEquals(1, commands.dbsize());
        assertEquals(values.size(), commands.scard(key));
    }

    @Test
    void writeSet(TestInfo info) throws Exception {
        String key = "sadd";
        List<String> values = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            values.add(String.valueOf(index));
        }
        ListItemReader<String> reader = new ListItemReader<>(values);
        Sadd<String, String, String> sadd = new Sadd<>();
        sadd.setKey(key);
        sadd.setValueFunction(Function.identity());
        OperationItemWriter<String, String, String> writer = writer(sadd);
        run(info, reader, writer);
        assertEquals(1, commands.dbsize());
        assertEquals(values.size(), commands.scard(key));
    }

    @Test
    void metrics(TestInfo info) throws Exception {
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
        generate(info);
        RedisItemReader<String, String, KeyValue<String>> reader = structReader(info, client);
        open(reader);
        Search search = registry.find("redis.batch.reader.queue.size");
        Assertions.assertNotNull(search.gauge());
        reader.close();
        registry.close();
        Metrics.globalRegistry.getMeters().forEach(Metrics.globalRegistry::remove);
    }

    @Test
    void dedupeKeyspaceNotifications() throws Exception {
        enableKeyspaceNotifications(client);
        KeyspaceNotificationItemReader<String> reader = new KeyspaceNotificationItemReader<>(client, StringCodec.UTF8);
        reader.open(new ExecutionContext());
        String key = "key1";
        commands.zadd(key, 1, "member1");
        commands.zadd(key, 2, "member2");
        commands.zadd(key, 3, "member3");
        awaitUntil(() -> reader.getQueue().size() == 1);
        Assertions.assertEquals(key, reader.read());
        reader.close();
    }

    @Test
    void reader(TestInfo info) throws Exception {
        generate(info);
        RedisItemReader<String, String, KeyValue<String>> reader = structReader(info, client);
        reader.setName(name(info) + "-reader");
        reader.open(new ExecutionContext());
        List<KeyValue<String>> list = BatchUtils.readAll(reader);
        reader.close();
        assertEquals(commands.dbsize(), list.size());
    }

    @Test
    void readThreads(TestInfo info) throws Exception {
        generate(info);
        RedisItemReader<String, String, KeyValue<String>> reader = structReader(info, client);
        SynchronizedListItemWriter<KeyValue<String>> writer = new SynchronizedListItemWriter<>();
        int threads = 4;
        run(job(info).start(step(info, reader, writer).taskExecutor(BatchUtils.threadPoolTaskExecutor(threads))
                .throttleLimit(threads).build()).build());
        awaitClosed(reader);
        awaitClosed(writer);
        assertEquals(commands.dbsize(), writer.getItems().stream().map(KeyValue::getKey).collect(Collectors.toSet()).size());
    }

    @Test
    void scanSizeEstimator(TestInfo info) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(10000);
        gen.setTypes(DataType.HASH, DataType.STRING);
        generate(info, gen);
        long expectedCount = commands.dbsize();
        ScanSizeEstimator estimator = new ScanSizeEstimator(client);
        estimator.setScanMatch(GeneratorItemReader.DEFAULT_KEYSPACE + ":*");
        estimator.setSamples(1000);
        assertEquals(expectedCount, estimator.getAsLong(), expectedCount / 10);
        estimator.setScanType(DataType.HASH.getString());
        assertEquals(expectedCount / 2, estimator.getAsLong(), expectedCount / 10);
    }

    @Test
    void writeStream(TestInfo info) throws Exception {
        String stream = "stream:0";
        List<Map<String, String>> messages = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            Map<String, String> body = new HashMap<>();
            body.put("field1", "value1");
            body.put("field2", "value2");
            messages.add(body);
        }
        ListItemReader<Map<String, String>> reader = new ListItemReader<>(messages);
        Xadd<String, String, Map<String, String>> xadd = new Xadd<>();
        xadd.setKey(stream);
        xadd.setBodyFunction(Function.identity());
        OperationItemWriter<String, String, Map<String, String>> writer = writer(xadd);
        run(info, reader, writer);
        Assertions.assertEquals(messages.size(), commands.xlen(stream));
        List<StreamMessage<String, String>> xrange = commands.xrange(stream, io.lettuce.core.Range.create("-", "+"));
        for (int index = 0; index < xrange.size(); index++) {
            StreamMessage<String, String> message = xrange.get(index);
            Assertions.assertEquals(messages.get(index), message.getBody());
        }
    }

    @Test
    void readLive(TestInfo info) throws Exception {
        enableKeyspaceNotifications(client);
        RedisItemReader<byte[], byte[], KeyValue<byte[]>> reader = liveStructReader(info, client, ByteArrayCodec.INSTANCE);
        reader.setNotificationQueueCapacity(10000);
        reader.open(new ExecutionContext());
        awaitOpen(reader);
        GeneratorItemReader gen = new GeneratorItemReader();
        int count = 123;
        gen.setMaxItemCount(count);
        gen.setTypes(DataType.HASH, DataType.STRING);
        generate(info, gen);
        List<KeyValue<byte[]>> list = BatchUtils.readAll(reader);
        Function<byte[], String> toString = CodecUtils.toStringKeyFunction(ByteArrayCodec.INSTANCE);
        Set<String> keys = list.stream().map(KeyValue::getKey).map(toString).collect(Collectors.toSet());
        Assertions.assertEquals(commands.dbsize(), keys.size());
        reader.close();
    }

    @Test
    void readWriteLive(TestInfo info) throws Exception {
        enableKeyspaceNotifications(client);
        RedisItemReader<byte[], byte[], KeyValue<byte[]>> reader = liveDumpReader(info, client, ByteArrayCodec.INSTANCE);
        reader.setNotificationQueueCapacity(100000);
        RedisItemWriter<byte[], byte[], KeyValue<byte[]>> writer = dumpWriter(targetClient, ByteArrayCodec.INSTANCE);
        JobExecution execution = runAsync(job(info).start(flushingStep(info, reader, writer).build()).build());
        awaitOpen(reader);
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(100);
        gen.setTypes(DataType.HASH, DataType.LIST, DataType.SET, DataType.STRING, DataType.ZSET);
        generate(info, gen);
        awaitTermination(execution);
        Assertions.assertTrue(compare(info));
    }

    @Test
    void readStreamAutoAck() throws InterruptedException {
        String stream = "stream1";
        String consumerGroup = "batchtests-readStreamAutoAck";
        Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
        final StreamItemReader<String, String> reader = streamReader(stream, consumer);
        reader.setAckPolicy(StreamAckPolicy.AUTO);
        open(reader);
        String field1 = "field1";
        String value1 = "value1";
        String field2 = "field2";
        String value2 = "value2";
        Map<String, String> body = map(field1, value1, field2, value2);
        String id1 = commands.xadd(stream, body);
        String id2 = commands.xadd(stream, body);
        String id3 = commands.xadd(stream, body);
        List<StreamMessage<String, String>> messages = new ArrayList<>();
        awaitUntil(() -> messages.addAll(reader.readMessages()));
        Assertions.assertEquals(3, messages.size());
        assertStreamEquals(id1, body, stream, messages.get(0));
        assertStreamEquals(id2, body, stream, messages.get(1));
        assertStreamEquals(id3, body, stream, messages.get(2));
        reader.close();
        Assertions.assertEquals(0, commands.xpending(stream, consumerGroup).getCount(), "pending messages");
    }

    @Test
    void readStreamManualAck() throws Exception {
        String stream = "stream1";
        String consumerGroup = "batchtests-readStreamManualAck";
        Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
        final StreamItemReader<String, String> reader = streamReader(stream, consumer);
        reader.setAckPolicy(StreamAckPolicy.MANUAL);
        open(reader);
        String field1 = "field1";
        String value1 = "value1";
        String field2 = "field2";
        String value2 = "value2";
        Map<String, String> body = map(field1, value1, field2, value2);
        String id1 = commands.xadd(stream, body);
        String id2 = commands.xadd(stream, body);
        String id3 = commands.xadd(stream, body);
        List<StreamMessage<String, String>> messages = new ArrayList<>();
        awaitUntil(() -> messages.addAll(reader.readMessages()));
        Assertions.assertEquals(3, messages.size());

        assertStreamEquals(id1, body, stream, messages.get(0));
        assertStreamEquals(id2, body, stream, messages.get(1));
        assertStreamEquals(id3, body, stream, messages.get(2));
        PendingMessages pendingMsgsBeforeCommit = commands.xpending(stream, consumerGroup);
        Assertions.assertEquals(3, pendingMsgsBeforeCommit.getCount(), "pending messages before commit");
        commands.xack(stream, consumerGroup, messages.get(0).getId(), messages.get(1).getId());
        PendingMessages pendingMsgsAfterCommit = commands.xpending(stream, consumerGroup);
        Assertions.assertEquals(1, pendingMsgsAfterCommit.getCount(), "pending messages after commit");
        reader.close();
    }

    @Test
    void readStreamManualAckRecover() throws InterruptedException {
        String stream = "stream1";
        Consumer<String> consumer = Consumer.from("batchtests-readStreamManualAckRecover", "consumer1");
        final StreamItemReader<String, String> reader = streamReader(stream, consumer);
        reader.setAckPolicy(StreamAckPolicy.MANUAL);
        open(reader);
        String field1 = "field1";
        String value1 = "value1";
        String field2 = "field2";
        String value2 = "value2";
        Map<String, String> body = map(field1, value1, field2, value2);
        commands.xadd(stream, body);
        commands.xadd(stream, body);
        commands.xadd(stream, body);
        List<StreamMessage<String, String>> messages = new ArrayList<>();
        awaitUntil(() -> messages.addAll(reader.readMessages()));
        Assertions.assertEquals(3, messages.size());

        List<StreamMessage<String, String>> recoveredMessages = new ArrayList<>();
        commands.xadd(stream, body);
        commands.xadd(stream, body);
        commands.xadd(stream, body);

        reader.close();

        final StreamItemReader<String, String> reader2 = streamReader(stream, consumer);
        reader2.setAckPolicy(StreamAckPolicy.MANUAL);
        open(reader2);

        awaitUntil(() -> recoveredMessages.addAll(reader2.readMessages()));
        awaitUntil(() -> !recoveredMessages.addAll(reader2.readMessages()));

        Assertions.assertEquals(6, recoveredMessages.size());
    }

    @Test
    void readStreamManualAckRecoverUncommitted() throws InterruptedException {
        String stream = "stream1";
        String consumerGroup = "batchtests-readStreamManualAckRecoverUncommitted";
        Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
        final StreamItemReader<String, String> reader = streamReader(stream, consumer);
        reader.setAckPolicy(StreamAckPolicy.MANUAL);
        open(reader);
        String field1 = "field1";
        String value1 = "value1";
        String field2 = "field2";
        String value2 = "value2";
        Map<String, String> body = map(field1, value1, field2, value2);
        commands.xadd(stream, body);
        commands.xadd(stream, body);
        String id3 = commands.xadd(stream, body);
        List<StreamMessage<String, String>> messages = new ArrayList<>();
        awaitUntil(() -> messages.addAll(reader.readMessages()));
        Assertions.assertEquals(3, messages.size());
        commands.xack(stream, consumerGroup, messages.get(0).getId(), messages.get(1).getId());

        List<StreamMessage<String, String>> recoveredMessages = new ArrayList<>();
        String id4 = commands.xadd(stream, body);
        String id5 = commands.xadd(stream, body);
        String id6 = commands.xadd(stream, body);
        reader.close();

        final StreamItemReader<String, String> reader2 = streamReader(stream, consumer);
        reader2.setAckPolicy(StreamAckPolicy.MANUAL);
        reader2.setOffset(messages.get(1).getId());
        open(reader2);

        // Wait until task.poll() doesn't return any more records
        awaitUntil(() -> recoveredMessages.addAll(reader2.readMessages()));
        awaitUntil(() -> !recoveredMessages.addAll(reader2.readMessages()));
        List<String> recoveredIds = recoveredMessages.stream().map(StreamMessage::getId).collect(Collectors.toList());
        Assertions.assertEquals(Arrays.<String> asList(id3, id4, id5, id6), recoveredIds, "recoveredIds");
        reader2.close();
    }

    @Test
    void readStreamManualAckRecoverFromOffset() throws Exception {
        String stream = "stream1";
        String consumerGroup = "batchtests-readStreamManualAckRecoverFromOffset";
        Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
        final StreamItemReader<String, String> reader = streamReader(stream, consumer);
        reader.setAckPolicy(StreamAckPolicy.MANUAL);
        open(reader);
        String field1 = "field1";
        String value1 = "value1";
        String field2 = "field2";
        String value2 = "value2";
        Map<String, String> body = map(field1, value1, field2, value2);
        commands.xadd(stream, body);
        commands.xadd(stream, body);
        String id3 = commands.xadd(stream, body);
        List<StreamMessage<String, String>> sourceRecords = new ArrayList<>();
        awaitUntil(() -> sourceRecords.addAll(reader.readMessages()));
        Assertions.assertEquals(3, sourceRecords.size());

        List<StreamMessage<String, String>> recoveredRecords = new ArrayList<>();
        String id4 = commands.xadd(stream, body);
        String id5 = commands.xadd(stream, body);
        String id6 = commands.xadd(stream, body);

        reader.close();

        final StreamItemReader<String, String> reader2 = streamReader(stream, consumer);
        reader2.setAckPolicy(StreamAckPolicy.MANUAL);
        reader2.setOffset(id3);
        open(reader2);

        // Wait until task.poll() doesn't return any more records
        awaitUntil(() -> recoveredRecords.addAll(reader2.readMessages()));
        awaitUntil(() -> !recoveredRecords.addAll(reader2.readMessages()));
        List<String> recoveredIds = recoveredRecords.stream().map(StreamMessage::getId).collect(Collectors.toList());
        Assertions.assertEquals(Arrays.<String> asList(id4, id5, id6), recoveredIds, "recoveredIds");
        reader2.close();
    }

    @Test
    void readStreamRecoverManualAckToAutoAck() throws InterruptedException {
        String stream = "stream1";
        String consumerGroup = "readStreamRecoverManualAckToAutoAck";
        Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
        final StreamItemReader<String, String> reader = streamReader(stream, consumer);
        reader.setAckPolicy(StreamAckPolicy.MANUAL);
        open(reader);
        String field1 = "field1";
        String value1 = "value1";
        String field2 = "field2";
        String value2 = "value2";
        Map<String, String> body = map(field1, value1, field2, value2);
        commands.xadd(stream, body);
        commands.xadd(stream, body);
        commands.xadd(stream, body);
        List<StreamMessage<String, String>> sourceRecords = new ArrayList<>();
        awaitUntil(() -> sourceRecords.addAll(reader.readMessages()));
        Assertions.assertEquals(3, sourceRecords.size());

        List<StreamMessage<String, String>> recoveredRecords = new ArrayList<>();
        String id4 = commands.xadd(stream, body);
        String id5 = commands.xadd(stream, body);
        String id6 = commands.xadd(stream, body);
        reader.close();

        final StreamItemReader<String, String> reader2 = streamReader(stream, consumer);
        reader2.setAckPolicy(StreamAckPolicy.AUTO);
        open(reader2);

        // Wait until task.poll() doesn't return any more records
        awaitUntil(() -> recoveredRecords.addAll(reader2.readMessages()));
        awaitUntil(() -> !recoveredRecords.addAll(reader2.readMessages()));
        List<String> recoveredIds = recoveredRecords.stream().map(StreamMessage::getId).collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList(id4, id5, id6), recoveredIds, "recoveredIds");

        PendingMessages pending = commands.xpending(stream, consumerGroup);
        Assertions.assertEquals(0, pending.getCount(), "pending message count");
        reader2.close();
    }

    @Test
    void readMessages(TestInfo info) throws Exception {
        generateStreams(info, 57);
        List<String> keys = ScanIterator.scan(commands, KeyScanArgs.Builder.type(DataType.STREAM.getString())).stream()
                .collect(Collectors.toList());
        Consumer<String> consumer = Consumer.from("batchtests-readmessages", "consumer1");
        for (String key : keys) {
            long count = commands.xlen(key);
            StreamItemReader<String, String> reader = streamReader(key, consumer);
            reader.setName(name(info) + "-reader");
            reader.open(new ExecutionContext());
            List<StreamMessage<String, String>> messages = BatchUtils.readAll(reader);
            assertEquals(count, messages.size());
            assertMessageBody(messages);
            awaitUntil(() -> reader.ack(reader.readMessages()) == 0);
            reader.close();
        }
    }

    @Test
    void streamReader(TestInfo info) throws Exception {
        generateStreams(info, 277);
        List<String> keys = ScanIterator.scan(commands, KeyScanArgs.Builder.type(DataType.STREAM.getString())).stream()
                .collect(Collectors.toList());
        Consumer<String> consumer = Consumer.from("batchtests-readstreamjob", "consumer1");
        for (String key : keys) {
            long count = commands.xlen(key);
            StreamItemReader<String, String> reader = streamReader(key, consumer);
            reader.setName(name(info) + "-reader");
            reader.open(new ExecutionContext());
            List<StreamMessage<String, String>> messages = BatchUtils.readAll(reader);
            reader.close();
            Assertions.assertEquals(count, messages.size());
            assertMessageBody(messages);
        }
    }

    @Test
    void luaHash() throws Exception {
        String key = "myhash";
        Map<String, String> hash = new HashMap<>();
        hash.put("field1", "value1");
        hash.put("field2", "value2");
        commands.hset(key, hash);
        long ttl = System.currentTimeMillis() + 123456;
        commands.pexpireat(key, ttl);
        SimpleOperationExecutor<String, String, String, KeyValue<String>> reader = structOperationExecutor();
        KeyValue<String> ds = reader.process(Arrays.asList(key)).get(0);
        Assertions.assertEquals(key, ds.getKey());
        Assertions.assertEquals(ttl, ds.getTtl());
        Assertions.assertEquals(DataType.HASH, ds.getType());
        Assertions.assertEquals(hash, ds.getValue());
        reader.close();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    void luaZset() throws Exception {
        String key = "myzset";
        ScoredValue[] values = { ScoredValue.just(123.456, "value1"), ScoredValue.just(654.321, "value2") };
        commands.zadd(key, values);
        SimpleOperationExecutor<String, String, String, KeyValue<String>> executor = structOperationExecutor();
        KeyValue<String> ds = executor.process(Arrays.asList(key)).get(0);
        Assertions.assertEquals(key, ds.getKey());
        Assertions.assertEquals(DataType.ZSET, ds.getType());
        Assertions.assertEquals(new HashSet<>(Arrays.asList(values)), ds.getValue());
        executor.close();
    }

    @Test
    void luaList() throws Exception {
        String key = "mylist";
        List<String> values = Arrays.asList("value1", "value2");
        commands.rpush(key, values.toArray(new String[0]));
        SimpleOperationExecutor<String, String, String, KeyValue<String>> executor = structOperationExecutor();
        KeyValue<String> ds = executor.process(Arrays.asList(key)).get(0);
        Assertions.assertEquals(key, ds.getKey());
        Assertions.assertEquals(DataType.LIST, ds.getType());
        Assertions.assertEquals(values, ds.getValue());
        executor.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    void luaStream() throws Exception {
        String key = "mystream";
        Map<String, String> body = new HashMap<>();
        body.put("field1", "value1");
        body.put("field2", "value2");
        commands.xadd(key, body);
        commands.xadd(key, body);
        SimpleOperationExecutor<String, String, String, KeyValue<String>> executor = structOperationExecutor();
        KeyValue<String> ds = executor.process(Arrays.asList(key)).get(0);
        Assertions.assertEquals(key, ds.getKey());
        Assertions.assertEquals(DataType.STREAM, ds.getType());
        List<StreamMessage<String, String>> messages = (List<StreamMessage<String, String>>) ds.getValue();
        Assertions.assertEquals(2, messages.size());
        for (StreamMessage<String, String> message : messages) {
            Assertions.assertEquals(body, message.getBody());
            Assertions.assertNotNull(message.getId());
        }
        executor.close();
    }

    @Test
    void luaStreamDump() throws Exception {
        String key = "mystream";
        Map<String, String> body = new HashMap<>();
        body.put("field1", "value1");
        body.put("field2", "value2");
        commands.xadd(key, body);
        commands.xadd(key, body);
        long ttl = System.currentTimeMillis() + 123456;
        commands.pexpireat(key, ttl);
        SimpleOperationExecutor<byte[], byte[], byte[], KeyValue<byte[]>> executor = dumpOperationExecutor(
                ByteArrayCodec.INSTANCE);
        KeyValue<byte[]> dump = executor.process(Arrays.asList(toByteArray(key))).get(0);
        Assertions.assertArrayEquals(toByteArray(key), dump.getKey());
        Assertions.assertTrue(Math.abs(ttl - dump.getTtl()) <= 3);
        commands.del(key);
        commands.restore(key, (byte[]) dump.getValue(), RestoreArgs.Builder.ttl(ttl).absttl());
        Assertions.assertEquals(DataType.STREAM.getString(), commands.type(key));
        executor.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    void luaStreamByteArray() throws Exception {
        String key = "mystream";
        Map<String, String> body = new HashMap<>();
        body.put("field1", "value1");
        body.put("field2", "value2");
        commands.xadd(key, body);
        commands.xadd(key, body);
        SimpleOperationExecutor<byte[], byte[], byte[], KeyValue<byte[]>> executor = structOperationExecutor(
                ByteArrayCodec.INSTANCE);
        KeyValue<byte[]> ds = executor.process(Arrays.asList(toByteArray(key))).get(0);
        Assertions.assertArrayEquals(toByteArray(key), ds.getKey());
        Assertions.assertEquals(DataType.STREAM, ds.getType());
        List<StreamMessage<byte[], byte[]>> messages = (List<StreamMessage<byte[], byte[]>>) ds.getValue();
        Assertions.assertEquals(2, messages.size());
        for (StreamMessage<byte[], byte[]> message : messages) {
            Map<byte[], byte[]> actual = message.getBody();
            Assertions.assertEquals(2, actual.size());
            Map<String, String> actualString = new HashMap<>();
            actual.forEach((k, v) -> actualString.put(toString(k), toString(v)));
            Assertions.assertEquals(body, actualString);
        }
        executor.close();
    }

    @Test
    void luaHLL() throws Exception {
        String key1 = "hll:1";
        commands.pfadd(key1, "member:1", "member:2");
        String key2 = "hll:2";
        commands.pfadd(key2, "member:1", "member:2", "member:3");
        SimpleOperationExecutor<String, String, String, KeyValue<String>> executor = structOperationExecutor();
        KeyValue<String> ds1 = executor.process(Arrays.asList(key1)).get(0);
        Assertions.assertEquals(key1, ds1.getKey());
        Assertions.assertEquals(DataType.STRING, ds1.getType());
        Assertions.assertEquals(commands.get(key1), ds1.getValue());
        executor.close();
    }

    @Test
    void writeStructOverwrite(TestInfo info) throws Exception {
        GeneratorItemReader gen1 = new GeneratorItemReader();
        gen1.setMaxItemCount(100);
        gen1.setTypes(DataType.HASH);
        gen1.setHashOptions(hashOptions(Range.of(5)));
        generate(info, client, gen1);
        GeneratorItemReader gen2 = new GeneratorItemReader();
        gen2.setMaxItemCount(100);
        gen2.setTypes(DataType.HASH);
        gen2.setHashOptions(hashOptions(Range.of(10)));
        generate(info, targetClient, gen2);
        RedisItemReader<String, String, KeyValue<String>> reader = structReader(info, client);
        RedisItemWriter<String, String, KeyValue<String>> writer = structWriter(targetClient);
        run(info, reader, writer);
        awaitClosed(reader);
        awaitClosed(writer);
        assertEquals(commands.hgetall("gen:1"), targetCommands.hgetall("gen:1"));
    }

    private MapOptions hashOptions(Range fieldCount) {
        MapOptions options = new MapOptions();
        options.setFieldCount(fieldCount);
        return options;
    }

    @Test
    void writeStructMerge(TestInfo info) throws Exception {
        GeneratorItemReader gen1 = new GeneratorItemReader();
        gen1.setMaxItemCount(100);
        gen1.setTypes(DataType.HASH);
        gen1.setHashOptions(hashOptions(Range.of(5)));
        generate(info, client, gen1);
        GeneratorItemReader gen2 = new GeneratorItemReader();
        gen2.setMaxItemCount(100);
        gen2.setTypes(DataType.HASH);
        gen2.setHashOptions(hashOptions(Range.of(10)));
        generate(info, targetClient, gen2);
        StructItemReader<String, String> reader = structReader(info, client);
        StructItemWriter<String, String> writer = RedisItemWriter.struct(targetClient);
        writer.setMerge(true);
        run(info, reader, writer);
        awaitClosed(reader);
        awaitClosed(writer);
        Map<String, String> actual = targetCommands.hgetall("gen:1");
        assertEquals(10, actual.size());
    }

    @Test
    void compareSet(TestInfo info) throws Exception {
        commands.sadd("set:1", "value1", "value2");
        targetCommands.sadd("set:1", "value2", "value1");
        KeyComparisonItemReader reader = comparisonReader(info);
        reader.setName(name(info));
        reader.open(new ExecutionContext());
        awaitOpen(reader);
        log.info("readAll");
        List<KeyComparison> comparisons = BatchUtils.readAll(reader);
        log.info("closing");
        reader.close();
        log.info("assert");
        Assertions.assertEquals(KeyComparison.Status.OK, comparisons.get(0).getStatus());
    }

    @Test
    void compareQuick(TestInfo info) throws Exception {
        int sourceCount = 100;
        for (int index = 1; index <= sourceCount; index++) {
            commands.set("key:" + index, "value:" + index);
        }
        int targetCount = 90;
        for (int index = 1; index <= targetCount; index++) {
            targetCommands.set("key:" + index, "value:" + index);
        }
        KeyTypeItemReader<String, String> source = keyTypeReader(info, client);
        KeyTypeItemReader<String, String> target = keyTypeReader(info, targetClient);
        KeyComparisonItemReader reader = new KeyComparisonItemReader(source, target);
        reader.setName(name(info));
        reader.open(new ExecutionContext());
        awaitOpen(reader);
        List<KeyComparison> comparisons = BatchUtils.readAll(reader);
        reader.close();
        List<KeyComparison> missing = comparisons.stream().filter(c -> c.getStatus() == Status.MISSING)
                .collect(Collectors.toList());
        Assertions.assertEquals(sourceCount - targetCount, missing.size());
    }

    @Test
    void byteArrayCodec(TestInfo info) throws Exception {
        try (StatefulRedisConnection<byte[], byte[]> connection = RedisModulesUtils.connection(client,
                ByteArrayCodec.INSTANCE)) {
            connection.setAutoFlushCommands(false);
            RedisAsyncCommands<byte[], byte[]> async = connection.async();
            List<RedisFuture<?>> futures = new ArrayList<>();
            Random random = new Random();
            for (int index = 0; index < 100; index++) {
                String key = "binary:" + index;
                byte[] value = new byte[1000];
                random.nextBytes(value);
                futures.add(async.set(key.getBytes(), value));
            }
            connection.flushCommands();
            LettuceFutures.awaitAll(connection.getTimeout(), futures.toArray(new RedisFuture[0]));
            connection.setAutoFlushCommands(true);
        }
        RedisItemReader<byte[], byte[], KeyValue<byte[]>> reader = structReader(info, client, ByteArrayCodec.INSTANCE);
        RedisItemWriter<byte[], byte[], KeyValue<byte[]>> writer = structWriter(targetClient, ByteArrayCodec.INSTANCE);
        run(info, reader, writer);
        awaitClosed(reader);
        awaitClosed(writer);
        Assertions.assertEquals(commands.dbsize(), targetCommands.dbsize());
    }

    @Test
    void replicateDumpLive(TestInfo info) throws Exception {
        enableKeyspaceNotifications(client);
        RedisItemReader<byte[], byte[], KeyValue<byte[]>> reader = dumpReader(info, client, ByteArrayCodec.INSTANCE);
        RedisItemWriter<byte[], byte[], KeyValue<byte[]>> writer = dumpWriter(targetClient, ByteArrayCodec.INSTANCE);
        RedisItemReader<byte[], byte[], KeyValue<byte[]>> liveReader = liveDumpReader(info, client, ByteArrayCodec.INSTANCE);
        RedisItemWriter<byte[], byte[], KeyValue<byte[]>> liveWriter = dumpWriter(targetClient, ByteArrayCodec.INSTANCE);
        Assertions.assertTrue(replicateLive(info, reader, writer, liveReader, liveWriter));
    }

    @Test
    void replicateSetLive(TestInfo info) throws Exception {
        enableKeyspaceNotifications(client);
        String key = "myset";
        commands.sadd(key, "1", "2", "3", "4", "5");
        RedisItemReader<String, String, KeyValue<String>> reader = liveStructReader(info, client);
        reader.setNotificationQueueCapacity(100);
        RedisItemWriter<String, String, KeyValue<String>> writer = structWriter(targetClient);
        JobExecution execution = runAsync(job(info).start(flushingStep(info, reader, writer).build()).build());
        awaitOpen(reader);
        awaitOpen(writer);
        commands.srem(key, "5");
        awaitTermination(execution);
        assertEquals(commands.smembers(key), targetCommands.smembers(key));
    }

    @Test
    void replicateHLL(TestInfo info) throws Exception {
        String key1 = "hll:1";
        commands.pfadd(key1, "member:1", "member:2");
        String key2 = "hll:2";
        commands.pfadd(key2, "member:1", "member:2", "member:3");
        RedisItemReader<byte[], byte[], KeyValue<byte[]>> reader = structReader(info, client, ByteArrayCodec.INSTANCE);
        RedisItemWriter<byte[], byte[], KeyValue<byte[]>> writer = structWriter(targetClient, ByteArrayCodec.INSTANCE);
        run(info, reader, writer);
        awaitClosed(reader);
        awaitClosed(writer);
        RedisModulesCommands<String, String> sourceSync = commands;
        RedisModulesCommands<String, String> targetSync = targetCommands;
        assertEquals(sourceSync.pfcount(key1), targetSync.pfcount(key1));
    }

    @Test
    void compareStatus(TestInfo info) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(120);
        generate(info, gen);
        RedisItemReader<byte[], byte[], KeyValue<byte[]>> reader = dumpReader(info, client, ByteArrayCodec.INSTANCE);
        RedisItemWriter<byte[], byte[], KeyValue<byte[]>> writer = dumpWriter(targetClient, ByteArrayCodec.INSTANCE);
        run(new SimpleTestInfo(info, "replicate"), reader, writer);
        awaitClosed(reader);
        awaitClosed(writer);
        long deleted = 0;
        for (int index = 0; index < 13; index++) {
            deleted += targetCommands.del(targetCommands.randomkey());
        }
        Set<String> ttlChanges = new HashSet<>();
        for (int index = 0; index < 23; index++) {
            String key = targetCommands.randomkey();
            if (key == null) {
                continue;
            }
            long ttl = targetCommands.ttl(key) + 12345;
            if (targetCommands.expire(key, ttl)) {
                ttlChanges.add(key);
            }
        }
        Set<String> typeChanges = new HashSet<>();
        Set<String> valueChanges = new HashSet<>();
        for (int index = 0; index < 17; index++) {
            String key;
            do {
                key = targetCommands.randomkey();
            } while (key == null);
            DataType type = DataType.of(targetCommands.type(key));
            if (type == DataType.STRING) {
                if (!typeChanges.contains(key)) {
                    valueChanges.add(key);
                }
                ttlChanges.remove(key);
            } else {
                typeChanges.add(key);
                valueChanges.remove(key);
                ttlChanges.remove(key);
            }
            targetCommands.set(key, "blah");
        }
        KeyComparisonItemReader comparator = comparisonReader(info);
        comparator.setName(name(info));
        comparator.open(new ExecutionContext());
        List<KeyComparison> comparisons = BatchUtils.readAll(comparator);
        comparator.close();
        long sourceCount = commands.dbsize();
        assertEquals(sourceCount, comparisons.size());
        assertEquals(sourceCount, targetCommands.dbsize() + deleted);
        List<KeyComparison> actualTypeChanges = comparisons.stream().filter(c -> c.getStatus() == Status.TYPE)
                .collect(Collectors.toList());
        assertEquals(typeChanges.size(), actualTypeChanges.size());
        assertEquals(valueChanges.size(), comparisons.stream().filter(c -> c.getStatus() == Status.VALUE).count());
        assertEquals(ttlChanges.size(), comparisons.stream().filter(c -> c.getStatus() == Status.TTL).count());
        assertEquals(deleted, comparisons.stream().filter(c -> c.getStatus() == Status.MISSING).count());
    }

}
