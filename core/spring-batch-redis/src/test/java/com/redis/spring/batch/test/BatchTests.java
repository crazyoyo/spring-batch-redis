package com.redis.spring.batch.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.ListItemReader;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.DataStructureType;
import com.redis.spring.batch.common.Dump;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.common.SimpleOperationExecutor;
import com.redis.spring.batch.common.Struct;
import com.redis.spring.batch.common.ToGeoValueFunction;
import com.redis.spring.batch.common.ToScoredValueFunction;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader;
import com.redis.spring.batch.reader.ScanSizeEstimator;
import com.redis.spring.batch.reader.StreamItemReader;
import com.redis.spring.batch.reader.StreamItemReader.StreamAckPolicy;
import com.redis.spring.batch.util.BatchUtils;
import com.redis.spring.batch.util.CodecUtils;
import com.redis.spring.batch.writer.operation.Del;
import com.redis.spring.batch.writer.operation.Expire;
import com.redis.spring.batch.writer.operation.ExpireAt;
import com.redis.spring.batch.writer.operation.Geoadd;
import com.redis.spring.batch.writer.operation.Hset;
import com.redis.spring.batch.writer.operation.Lpush;
import com.redis.spring.batch.writer.operation.LpushAll;
import com.redis.spring.batch.writer.operation.Rpush;
import com.redis.spring.batch.writer.operation.Sadd;
import com.redis.spring.batch.writer.operation.Xadd;
import com.redis.spring.batch.writer.operation.Zadd;

import io.lettuce.core.Consumer;
import io.lettuce.core.GeoArgs;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.models.stream.PendingMessages;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.search.Search;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

abstract class BatchTests extends AbstractTestBase {

    @Test
    void writeWait(TestInfo testInfo) throws Exception {
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
        RedisItemWriter<String, String, Map<String, String>> writer = writer(hset);
        writer.setWaitReplicas(1);
        writer.setWaitTimeout(Duration.ofMillis(300));
        JobExecution execution = run(testInfo, reader, writer);
        List<Throwable> exceptions = execution.getAllFailureExceptions();
        assertEquals("Insufficient replication level (0/1)", exceptions.get(0).getCause().getMessage());
    }

    private RedisItemWriter<String, String, Map<String, String>> writer(
            Operation<String, String, Map<String, String>, Object> operation) {
        return new RedisItemWriter<>(client, StringCodec.UTF8, operation);
    }

    @Test
    void writeHash(TestInfo testInfo) throws Exception {
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
        RedisItemWriter<String, String, Map<String, String>> writer = writer(hset);
        run(testInfo, reader, writer);
        assertEquals(maps.size(), commands.keys("hash:*").size());
        for (int index = 0; index < maps.size(); index++) {
            Map<String, String> hash = commands.hgetall("hash:" + index);
            assertEquals(maps.get(index), hash);
        }
    }

    @Test
    void writeComposite(TestInfo testInfo) throws Exception {
        List<Map<String, String>> maps = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            Map<String, String> body = new HashMap<>();
            body.put("id", String.valueOf(index));
            body.put("field1", "value1");
            body.put("field2", "value2");
            maps.add(body);
        }
        ListItemReader<Map<String, String>> reader = new ListItemReader<>(maps);
        Function<Map<String, String>, String> keyFunction = m -> "hash:" + m.get("id");
        Hset<String, String, Map<String, String>> hset = new Hset<>();
        hset.setKeyFunction(keyFunction);
        hset.setMapFunction(Function.identity());
        Expire<String, String, Map<String, String>> expire = new Expire<>();
        expire.setKeyFunction(keyFunction);
        Duration ttl = Duration.ofSeconds(10);
        expire.setTtl(ttl);
        RedisItemWriter<String, String, Map<String, String>> writer = writer(hset);
        run(testInfo, reader, writer);
        assertEquals(maps.size(), commands.keys("hash:*").size());
        for (int index = 0; index < maps.size(); index++) {
            String key = "hash:" + index;
            Map<String, String> hash = commands.hgetall(key);
            assertEquals(maps.get(index), hash);
            assertEquals(ttl, Duration.ofMillis(commands.pttl(key)));
        }
    }

    @Test
    void writeDel(TestInfo testInfo) throws Exception {
        generate(testInfo);
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(DEFAULT_GENERATOR_COUNT);
        Del<String, String, Struct<String>> del = new Del<>();
        del.setKeyFunction(Struct::getKey);
        run(testInfo, gen, new RedisItemWriter<>(client, StringCodec.UTF8, del));
        assertEquals(0, commands.keys(GeneratorItemReader.DEFAULT_KEYSPACE + "*").size());
    }

    @Test
    void writeLpush(TestInfo testInfo) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(DEFAULT_GENERATOR_COUNT);
        gen.setTypes(DataStructureType.STRING);
        Lpush<String, String, Struct<String>> lpush = new Lpush<>();
        lpush.setKeyFunction(Struct::getKey);
        lpush.setValueFunction(v -> (String) v.getValue());
        run(testInfo, gen, new RedisItemWriter<>(client, StringCodec.UTF8, lpush));
        assertEquals(DEFAULT_GENERATOR_COUNT, commands.dbsize());
        for (String key : commands.keys("*")) {
            assertEquals(DataStructureType.LIST.getString(), commands.type(key));
        }
    }

    @Test
    void writeRpush(TestInfo testInfo) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(DEFAULT_GENERATOR_COUNT);
        gen.setTypes(DataStructureType.STRING);
        Rpush<String, String, Struct<String>> rpush = new Rpush<>();
        rpush.setKeyFunction(Struct::getKey);
        rpush.setValueFunction(v -> (String) v.getValue());
        run(testInfo, gen, new RedisItemWriter<>(client, StringCodec.UTF8, rpush));
        assertEquals(DEFAULT_GENERATOR_COUNT, commands.dbsize());
        for (String key : commands.keys("*")) {
            assertEquals(DataStructureType.LIST.getString(), commands.type(key));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void writeLpushAll(TestInfo testInfo) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(DEFAULT_GENERATOR_COUNT);
        gen.setTypes(DataStructureType.LIST);
        LpushAll<String, String, Struct<String>> lpushAll = new LpushAll<>();
        lpushAll.setKeyFunction(Struct::getKey);
        lpushAll.setValuesFunction(v -> (Collection<String>) v.getValue());
        run(testInfo, gen, new RedisItemWriter<>(client, StringCodec.UTF8, lpushAll));
        assertEquals(DEFAULT_GENERATOR_COUNT, commands.dbsize());
        for (String key : commands.keys("*")) {
            assertEquals(DataStructureType.LIST.getString(), commands.type(key));
        }
    }

    @Test
    void writeExpire(TestInfo testInfo) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(DEFAULT_GENERATOR_COUNT);
        gen.setTypes(DataStructureType.STRING);
        Duration ttl = Duration.ofMillis(1L);
        Expire<String, String, Struct<String>> expire = new Expire<>();
        expire.setKeyFunction(Struct::getKey);
        expire.setTtl(ttl);
        run(testInfo, gen, new RedisItemWriter<>(client, StringCodec.UTF8, expire));
        awaitUntil(() -> commands.keys("*").isEmpty());
        assertEquals(0, commands.dbsize());
    }

    @Test
    void writeExpireAt(TestInfo testInfo) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(DEFAULT_GENERATOR_COUNT);
        gen.setTypes(DataStructureType.STRING);
        ExpireAt<String, String, Struct<String>> expireAt = new ExpireAt<>();
        expireAt.setKeyFunction(Struct::getKey);
        expireAt.setEpochFunction(v -> System.currentTimeMillis());
        run(testInfo, gen, new RedisItemWriter<>(client, StringCodec.UTF8, expireAt));
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
    void writeGeo(TestInfo testInfo) throws Exception {
        ListItemReader<Geo> reader = new ListItemReader<>(Arrays.asList(new Geo("Venice Breakwater", -118.476056, 33.985728),
                new Geo("Long Beach National", -73.667022, 40.582739)));
        Geoadd<String, String, Geo> geoadd = new Geoadd<>();
        geoadd.setKey("geoset");
        geoadd.setValueFunction(new ToGeoValueFunction<>(Geo::getMember, Geo::getLongitude, Geo::getLatitude));
        RedisItemWriter<String, String, Geo> writer = new RedisItemWriter<>(client, StringCodec.UTF8, geoadd);
        run(testInfo, reader, writer);
        Set<String> radius1 = commands.georadius("geoset", -118, 34, 100, GeoArgs.Unit.mi);
        assertEquals(1, radius1.size());
        assertTrue(radius1.contains("Venice Breakwater"));
    }

    @Test
    void writeHashDel(TestInfo testInfo) throws Exception {
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
        RedisItemWriter<String, String, Entry<String, Map<String, String>>> writer = new RedisItemWriter<>(client,
                StringCodec.UTF8, hset);
        run(testInfo, reader, writer);
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
    void writeZset(TestInfo testInfo) throws Exception {
        String key = "zadd";
        List<ZValue> values = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            values.add(new ZValue(String.valueOf(index), index % 10));
        }
        ListItemReader<ZValue> reader = new ListItemReader<>(values);
        Zadd<String, String, ZValue> zadd = new Zadd<>();
        zadd.setKey(key);
        zadd.setValueFunction(new ToScoredValueFunction<>(ZValue::getMember, ZValue::getScore));
        RedisItemWriter<String, String, ZValue> writer = new RedisItemWriter<>(client, StringCodec.UTF8, zadd);
        run(testInfo, reader, writer);
        assertEquals(1, commands.dbsize());
        assertEquals(values.size(), commands.zcard(key));
        assertEquals(60, commands.zrangebyscore(key, io.lettuce.core.Range.from(io.lettuce.core.Range.Boundary.including(0),
                io.lettuce.core.Range.Boundary.including(5))).size());
    }

    @Test
    void writeSet(TestInfo testInfo) throws Exception {
        String key = "sadd";
        List<String> values = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            values.add(String.valueOf(index));
        }
        ListItemReader<String> reader = new ListItemReader<>(values);
        Sadd<String, String, String> sadd = new Sadd<>();
        sadd.setKey(key);
        sadd.setValueFunction(Function.identity());
        RedisItemWriter<String, String, String> writer = new RedisItemWriter<>(client, StringCodec.UTF8, sadd);
        run(testInfo, reader, writer);
        assertEquals(1, commands.dbsize());
        assertEquals(values.size(), commands.scard(key));
    }

    @Test
    void writeStructs(TestInfo testInfo) throws Exception {
        List<Struct<String>> list = new ArrayList<>();
        long count = 100;
        for (int index = 0; index < count; index++) {
            Map<String, String> map = new HashMap<>();
            map.put("field1", "value1");
            map.put("field2", "value2");
            Struct<String> ds = Struct.key("hash:" + index).type(DataStructureType.HASH).value(map).build();
            list.add(ds);
        }
        ListItemReader<Struct<String>> reader = new ListItemReader<>(list);
        RedisItemWriter<String, String, Struct<String>> writer = structWriter(client);
        run(testInfo, reader, writer);
        List<String> keys = commands.keys("hash:*");
        assertEquals(count, keys.size());
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
        RedisItemReader<String, String, Struct<String>> reader = structReader(info, client);
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
        RedisItemReader<String, String, Struct<String>> reader = structReader(info, client);
        reader.setName(name(info) + "-reader");
        reader.open(new ExecutionContext());
        List<Struct<String>> list = BatchUtils.readAll(reader);
        reader.close();
        assertEquals(commands.dbsize(), list.size());
    }

    @Test
    void readThreads(TestInfo info) throws Exception {
        generate(info);
        RedisItemReader<String, String, Struct<String>> reader = structReader(info, client);
        SynchronizedListItemWriter<Struct<String>> writer = new SynchronizedListItemWriter<>();
        int threads = 4;
        run(job(info).start(step(info, reader, writer).taskExecutor(BatchUtils.threadPoolTaskExecutor(threads))
                .throttleLimit(threads).build()).build());
        awaitClosed(reader);
        awaitClosed(writer);
        assertEquals(commands.dbsize(), writer.getItems().stream().map(Struct::getKey).collect(Collectors.toSet()).size());
    }

    @Test
    void scanSizeEstimator(TestInfo testInfo) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(10000);
        gen.setTypes(DataStructureType.HASH, DataStructureType.STRING);
        generate(testInfo, gen);
        long expectedCount = commands.dbsize();
        ScanSizeEstimator estimator = new ScanSizeEstimator(client);
        estimator.setScanMatch(GeneratorItemReader.DEFAULT_KEYSPACE + ":*");
        estimator.setSamples(1000);
        assertEquals(expectedCount, estimator.getAsLong(), expectedCount / 10);
        estimator.setScanType(DataStructureType.HASH.getString());
        assertEquals(expectedCount / 2, estimator.getAsLong(), expectedCount / 10);
    }

    @Test
    void writeStream(TestInfo testInfo) throws Exception {
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
        RedisItemWriter<String, String, Map<String, String>> writer = new RedisItemWriter<>(client, StringCodec.UTF8, xadd);
        run(testInfo, reader, writer);
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
        RedisItemReader<byte[], byte[], Struct<byte[]>> reader = liveStructReader(info, client, ByteArrayCodec.INSTANCE);
        reader.setNotificationQueueCapacity(10000);
        GeneratorItemReader gen = new GeneratorItemReader();
        int count = 123;
        gen.setMaxItemCount(count);
        gen.setTypes(DataStructureType.HASH, DataStructureType.STRING);
        generate(info, gen);
        List<Struct<byte[]>> list = BatchUtils.readAll(reader);
        Function<byte[], String> toString = CodecUtils.toStringKeyFunction(ByteArrayCodec.INSTANCE);
        Set<String> keys = list.stream().map(Struct::getKey).map(toString).collect(Collectors.toSet());
        Assertions.assertEquals(commands.dbsize(), keys.size());
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
    void readMessages(TestInfo testInfo) throws Exception {
        generateStreams(testInfo, 57);
        List<String> keys = ScanIterator.scan(commands, KeyScanArgs.Builder.type(DataStructureType.STREAM.getString())).stream()
                .collect(Collectors.toList());
        Consumer<String> consumer = Consumer.from("batchtests-readmessages", "consumer1");
        for (String key : keys) {
            long count = commands.xlen(key);
            StreamItemReader<String, String> reader = streamReader(key, consumer);
            reader.setName(name(testInfo) + "-reader");
            reader.open(new ExecutionContext());
            List<StreamMessage<String, String>> messages = BatchUtils.readAll(reader);
            reader.close();
            assertEquals(count, messages.size());
            assertMessageBody(messages);
            awaitUntil(() -> reader.ack(reader.readMessages()) == 0);
            reader.close();
        }
    }

    @Test
    void streamReaderJob(TestInfo testInfo) throws Exception {
        generateStreams(testInfo, 277);
        List<String> keys = ScanIterator.scan(commands, KeyScanArgs.Builder.type(DataStructureType.STREAM.getString())).stream()
                .collect(Collectors.toList());
        Consumer<String> consumer = Consumer.from("batchtests-readstreamjob", "consumer1");
        for (String key : keys) {
            long count = commands.xlen(key);
            StreamItemReader<String, String> reader = streamReader(key, consumer);
            reader.setName(name(testInfo) + "-reader");
            reader.open(new ExecutionContext());
            List<StreamMessage<String, String>> messages = BatchUtils.readAll(reader);
            reader.close();
            Assertions.assertEquals(count, messages.size());
            assertMessageBody(messages);
        }
    }

    @Test
    void invalidConnection(TestInfo info) throws Exception {
        try (RedisModulesClient badSourceClient = RedisModulesClient.create("redis://badhost:6379")) {
            RedisItemReader<String, String, Struct<String>> reader = structReader(info, badSourceClient);
            reader.setName(name(info) + "-reader");
            Assertions.assertThrows(RedisConnectionException.class, () -> open(reader));
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
        SimpleOperationExecutor<String, String, String, Struct<String>> reader = structOperationExecutor();
        Struct<String> ds = reader.process(Arrays.asList(key)).get(0);
        Assertions.assertEquals(key, ds.getKey());
        Assertions.assertEquals(ttl, ds.getTtl());
        Assertions.assertEquals(DataStructureType.HASH, ds.getType());
        Assertions.assertEquals(hash, ds.getValue());
        reader.close();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    void luaZset() throws Exception {
        String key = "myzset";
        ScoredValue[] values = { ScoredValue.just(123.456, "value1"), ScoredValue.just(654.321, "value2") };
        commands.zadd(key, values);
        SimpleOperationExecutor<String, String, String, Struct<String>> executor = structOperationExecutor();
        Struct<String> ds = executor.process(Arrays.asList(key)).get(0);
        Assertions.assertEquals(key, ds.getKey());
        Assertions.assertEquals(DataStructureType.ZSET, ds.getType());
        Assertions.assertEquals(new HashSet<>(Arrays.asList(values)), ds.getValue());
        executor.close();
    }

    @Test
    void luaList() throws Exception {
        String key = "mylist";
        List<String> values = Arrays.asList("value1", "value2");
        commands.rpush(key, values.toArray(new String[0]));
        SimpleOperationExecutor<String, String, String, Struct<String>> executor = structOperationExecutor();
        Struct<String> ds = executor.process(Arrays.asList(key)).get(0);
        Assertions.assertEquals(key, ds.getKey());
        Assertions.assertEquals(DataStructureType.LIST, ds.getType());
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
        SimpleOperationExecutor<String, String, String, Struct<String>> executor = structOperationExecutor();
        Struct<String> ds = executor.process(Arrays.asList(key)).get(0);
        Assertions.assertEquals(key, ds.getKey());
        Assertions.assertEquals(DataStructureType.STREAM, ds.getType());
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
        SimpleOperationExecutor<byte[], byte[], byte[], Dump<byte[]>> executor = dumpOperationExecutor(ByteArrayCodec.INSTANCE);
        Dump<byte[]> dump = executor.process(Arrays.asList(toByteArray(key))).get(0);
        Assertions.assertArrayEquals(toByteArray(key), dump.getKey());
        Assertions.assertTrue(Math.abs(ttl - dump.getTtl()) <= 3);
        commands.del(key);
        commands.restore(key, dump.getValue(), RestoreArgs.Builder.ttl(ttl).absttl());
        Assertions.assertEquals(DataStructureType.STREAM.getString(), commands.type(key));
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
        SimpleOperationExecutor<byte[], byte[], byte[], Struct<byte[]>> executor = structOperationExecutor(
                ByteArrayCodec.INSTANCE);
        Struct<byte[]> ds = executor.process(Arrays.asList(toByteArray(key))).get(0);
        Assertions.assertArrayEquals(toByteArray(key), ds.getKey());
        Assertions.assertEquals(DataStructureType.STREAM, ds.getType());
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
        SimpleOperationExecutor<String, String, String, Struct<String>> executor = structOperationExecutor();
        Struct<String> ds1 = executor.process(Arrays.asList(key1)).get(0);
        Assertions.assertEquals(key1, ds1.getKey());
        Assertions.assertEquals(DataStructureType.STRING, ds1.getType());
        Assertions.assertEquals(commands.get(key1), ds1.getValue());
        executor.close();
    }

}
