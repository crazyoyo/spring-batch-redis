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
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.test.context.junit4.SpringRunner;

import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyComparison;
import com.redis.spring.batch.common.KeyComparison.Status;
import com.redis.spring.batch.common.KeyComparisonItemReader;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.OperationValueReader;
import com.redis.spring.batch.common.Range;
import com.redis.spring.batch.common.ToGeoValueFunction;
import com.redis.spring.batch.common.ToScoredValueFunction;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.gen.MapOptions;
import com.redis.spring.batch.reader.KeyTypeItemReader;
import com.redis.spring.batch.reader.ScanSizeEstimator;
import com.redis.spring.batch.reader.StreamItemReader;
import com.redis.spring.batch.reader.StreamItemReader.StreamAckPolicy;
import com.redis.spring.batch.reader.StructItemReader;
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
import io.lettuce.core.models.stream.PendingMessages;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
abstract class BatchTests extends AbstractTargetTestBase {

    @Override
    protected DataType[] generatorDataTypes() {
        return REDIS_GENERATOR_TYPES;
    }

    @Test
    void compareSet(TestInfo info) throws Exception {
        commands.sadd("set:1", "value1", "value2");
        targetCommands.sadd("set:1", "value2", "value1");
        KeyComparisonItemReader reader = comparisonReader(info);
        reader.setName(name(info));
        reader.open(new ExecutionContext());
        List<KeyComparison> comparisons = readAll(reader);
        reader.close();
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
        KeyTypeItemReader<String, String> source = RedisItemReader.type(client);
        KeyTypeItemReader<String, String> target = RedisItemReader.type(targetClient);
        KeyComparisonItemReader reader = new KeyComparisonItemReader(source, target);
        reader.setName(name(info));
        reader.open(new ExecutionContext());
        List<KeyComparison> comparisons = readAll(reader);
        reader.close();
        List<KeyComparison> missing = comparisons.stream().filter(c -> c.getStatus() == Status.MISSING)
                .collect(Collectors.toList());
        Assertions.assertEquals(sourceCount - targetCount, missing.size());
    }

    @Test
    void compareStatus(TestInfo info) throws Exception {
        GeneratorItemReader gen = generator(120);
        generate(info, gen);
        replicate(info, RedisItemReader.dump(client), RedisItemWriter.dump(targetClient));
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
        List<KeyComparison> comparisons = readAll(comparator);
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

    @Test
    void estimateScanSize(TestInfo info) throws Exception {
        GeneratorItemReader gen = generator(10000, DataType.HASH, DataType.STRING);
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
    void readStruct(TestInfo info) throws Exception {
        generate(info);
        StructItemReader<String, String> reader = RedisItemReader.struct(client);
        configureReader(info, reader);
        reader.open(new ExecutionContext());
        List<KeyValue<String>> list = readAll(reader);
        reader.close();
        assertEquals(commands.dbsize(), list.size());
    }

    @Test
    void readStructMultiThreaded(TestInfo info) throws Exception {
        generate(info);
        int threads = 4;
        StructItemReader<String, String> reader = RedisItemReader.struct(client);
        configureReader(info, reader);
        SynchronizedListItemWriter<KeyValue<String>> writer = new SynchronizedListItemWriter<>();
        SimpleStepBuilder<KeyValue<String>, KeyValue<String>> step = step(info, reader, writer);
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(threads);
        taskExecutor.setCorePoolSize(threads);
        taskExecutor.setQueueCapacity(threads);
        taskExecutor.afterPropertiesSet();
        step.taskExecutor(taskExecutor);
        step.throttleLimit(threads);
        run(info, step);
        awaitUntilFalse(reader::isOpen);
        awaitUntilFalse(writer::isOpen);
        assertEquals(commands.dbsize(), writer.getItems().stream().map(KeyValue::getKey).collect(Collectors.toSet()).size());
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
    void readStreamAck(TestInfo info) throws Exception {
        generateStreams(info, 57);
        List<String> keys = ScanIterator.scan(commands, KeyScanArgs.Builder.type(DataType.STREAM.getString())).stream()
                .collect(Collectors.toList());
        Consumer<String> consumer = Consumer.from("batchtests-readmessages", "consumer1");
        for (String key : keys) {
            long count = commands.xlen(key);
            StreamItemReader<String, String> reader = streamReader(key, consumer);
            reader.setName(name(info) + "-reader");
            reader.open(new ExecutionContext());
            List<StreamMessage<String, String>> messages = readAll(reader);
            assertEquals(count, messages.size());
            assertMessageBody(messages);
            awaitUntil(() -> reader.ack(reader.readMessages()) == 0);
            reader.close();
        }
    }

    @Test
    void readStream(TestInfo info) throws Exception {
        generateStreams(info, 277);
        List<String> keys = ScanIterator.scan(commands, KeyScanArgs.Builder.type(DataType.STREAM.getString())).stream()
                .collect(Collectors.toList());
        Consumer<String> consumer = Consumer.from("batchtests-readstreamjob", "consumer1");
        for (String key : keys) {
            long count = commands.xlen(key);
            StreamItemReader<String, String> reader = streamReader(key, consumer);
            reader.setName(name(info) + "-reader");
            reader.open(new ExecutionContext());
            List<StreamMessage<String, String>> messages = readAll(reader);
            reader.close();
            Assertions.assertEquals(count, messages.size());
            assertMessageBody(messages);
        }
    }

    @Test
    void readStructHash() throws Exception {
        String key = "myhash";
        Map<String, String> hash = new HashMap<>();
        hash.put("field1", "value1");
        hash.put("field2", "value2");
        commands.hset(key, hash);
        long ttl = System.currentTimeMillis() + 123456;
        commands.pexpireat(key, ttl);
        OperationValueReader<String, String, String, KeyValue<String>> reader = structOperationExecutor();
        KeyValue<String> ds = reader.process(Arrays.asList(key)).get(0);
        Assertions.assertEquals(key, ds.getKey());
        Assertions.assertEquals(ttl, ds.getTtl());
        Assertions.assertEquals(DataType.HASH, ds.getType());
        Assertions.assertEquals(hash, ds.getValue());
        reader.close();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    void readStructZset() throws Exception {
        String key = "myzset";
        ScoredValue[] values = { ScoredValue.just(123.456, "value1"), ScoredValue.just(654.321, "value2") };
        commands.zadd(key, values);
        OperationValueReader<String, String, String, KeyValue<String>> executor = structOperationExecutor();
        KeyValue<String> ds = executor.process(Arrays.asList(key)).get(0);
        Assertions.assertEquals(key, ds.getKey());
        Assertions.assertEquals(DataType.ZSET, ds.getType());
        Assertions.assertEquals(new HashSet<>(Arrays.asList(values)), ds.getValue());
        executor.close();
    }

    @Test
    void readStructList() throws Exception {
        String key = "mylist";
        List<String> values = Arrays.asList("value1", "value2");
        commands.rpush(key, values.toArray(new String[0]));
        OperationValueReader<String, String, String, KeyValue<String>> executor = structOperationExecutor();
        KeyValue<String> ds = executor.process(Arrays.asList(key)).get(0);
        Assertions.assertEquals(key, ds.getKey());
        Assertions.assertEquals(DataType.LIST, ds.getType());
        Assertions.assertEquals(values, ds.getValue());
        executor.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    void readStructStream() throws Exception {
        String key = "mystream";
        Map<String, String> body = new HashMap<>();
        body.put("field1", "value1");
        body.put("field2", "value2");
        commands.xadd(key, body);
        commands.xadd(key, body);
        OperationValueReader<String, String, String, KeyValue<String>> executor = structOperationExecutor();
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
    void readDumpStream() throws Exception {
        String key = "mystream";
        Map<String, String> body = new HashMap<>();
        body.put("field1", "value1");
        body.put("field2", "value2");
        commands.xadd(key, body);
        commands.xadd(key, body);
        long ttl = System.currentTimeMillis() + 123456;
        commands.pexpireat(key, ttl);
        OperationValueReader<byte[], byte[], byte[], KeyValue<byte[]>> executor = dumpOperationExecutor();
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
    void readStructStreamByteArray() throws Exception {
        String key = "mystream";
        Map<String, String> body = new HashMap<>();
        body.put("field1", "value1");
        body.put("field2", "value2");
        commands.xadd(key, body);
        commands.xadd(key, body);
        OperationValueReader<byte[], byte[], byte[], KeyValue<byte[]>> executor = structOperationExecutor(
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
    void readStructHLL() throws Exception {
        String key1 = "hll:1";
        commands.pfadd(key1, "member:1", "member:2");
        String key2 = "hll:2";
        commands.pfadd(key2, "member:1", "member:2", "member:3");
        OperationValueReader<String, String, String, KeyValue<String>> executor = structOperationExecutor();
        KeyValue<String> ds1 = executor.process(Arrays.asList(key1)).get(0);
        Assertions.assertEquals(key1, ds1.getKey());
        Assertions.assertEquals(DataType.STRING, ds1.getType());
        Assertions.assertEquals(commands.get(key1), ds1.getValue());
        executor.close();
    }

    @Test
    void replicateDump(TestInfo info) throws Exception {
        GeneratorItemReader gen = generator(100);
        generate(info, gen);
        List<KeyComparison> diffs = replicate(info, RedisItemReader.dump(client), RedisItemWriter.dump(targetClient));
        Assertions.assertTrue(diffs.isEmpty());
    }

    @Test
    void replicateStruct(TestInfo info) throws Exception {
        GeneratorItemReader gen = generator(100);
        generate(info, gen);
        List<KeyComparison> diffs = replicate(info, RedisItemReader.struct(client), RedisItemWriter.struct(targetClient));
        Assertions.assertTrue(diffs.isEmpty());
    }

    @Test
    void replicateStructByteArray(TestInfo info) throws Exception {
        GeneratorItemReader gen = generator(1000);
        generate(info, gen);
        StructItemReader<byte[], byte[]> reader = RedisItemReader.struct(client, ByteArrayCodec.INSTANCE);
        StructItemWriter<byte[], byte[]> writer = RedisItemWriter.struct(targetClient, ByteArrayCodec.INSTANCE);
        List<KeyComparison> diffs = replicate(info, reader, writer);
        Assertions.assertTrue(diffs.isEmpty());
    }

    @Test
    void replicateStructBinaryStrings(TestInfo info) throws Exception {
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

        assertEmpty(replicate(info, RedisItemReader.struct(client, ByteArrayCodec.INSTANCE),
                RedisItemWriter.struct(targetClient, ByteArrayCodec.INSTANCE)));
    }

    protected <K, V> List<KeyComparison> replicate(TestInfo info, RedisItemReader<K, V, KeyValue<K>> reader,
            RedisItemWriter<K, V, KeyValue<K>> writer) throws Exception {
        configureReader(info, reader);
        run(info, reader, writer);
        awaitUntilFalse(reader::isOpen);
        awaitUntilFalse(writer::isOpen);
        return compare(info);

    }

    @Test
    void replicateStructEmptyCollections(TestInfo info) throws Exception {
        GeneratorItemReader gen = generator(1000);
        Range cardinality = Range.of(0);
        gen.getHashOptions().setFieldCount(cardinality);
        gen.getSetOptions().setMemberCount(cardinality);
        gen.getStreamOptions().setMessageCount(cardinality);
        gen.getTimeSeriesOptions().setSampleCount(cardinality);
        gen.getZsetOptions().setMemberCount(cardinality);
        generate(info, gen);
        assertEmpty(replicate(info, RedisItemReader.struct(client), RedisItemWriter.struct(targetClient)));
    }

    @Test
    void replicateHLL(TestInfo info) throws Exception {
        String key1 = "hll:1";
        commands.pfadd(key1, "member:1", "member:2");
        String key2 = "hll:2";
        commands.pfadd(key2, "member:1", "member:2", "member:3");
        List<KeyComparison> diffs = replicate(info, RedisItemReader.struct(client, ByteArrayCodec.INSTANCE),
                RedisItemWriter.struct(targetClient, ByteArrayCodec.INSTANCE));
        assertTrue(diffs.isEmpty());
        assertEquals(commands.pfcount(key1), targetCommands.pfcount(key1));
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
        awaitUntilFalse(writer::isOpen);
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
        awaitUntilFalse(writer::isOpen);
        assertEquals(100, commands.keys("hash:*").size());
        assertEquals(2, commands.hgetall("hash:50").size());
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
        awaitUntilFalse(writer::isOpen);
        assertEquals(1, commands.dbsize());
        assertEquals(values.size(), commands.zcard(key));
        assertEquals(60, commands.zrangebyscore(key, io.lettuce.core.Range.from(io.lettuce.core.Range.Boundary.including(0),
                io.lettuce.core.Range.Boundary.including(5))).size());
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
        awaitUntilFalse(writer::isOpen);
        assertEquals(1, commands.dbsize());
        assertEquals(values.size(), commands.scard(key));
    }

    @Test
    void writeStructOverwrite(TestInfo info) throws Exception {
        GeneratorItemReader gen1 = generator(100, DataType.HASH);
        gen1.setHashOptions(hashOptions(Range.of(5)));
        generate(info, client, gen1);
        GeneratorItemReader gen2 = generator(100, DataType.HASH);
        gen2.setHashOptions(hashOptions(Range.of(10)));
        generate(info, targetClient, gen2);
        assertEmpty(replicate(info, RedisItemReader.struct(client), RedisItemWriter.struct(targetClient)));
        assertEquals(commands.hgetall("gen:1"), targetCommands.hgetall("gen:1"));
    }

    protected void assertEmpty(List<?> list) {
        assertTrue(list.isEmpty());
    }

    private MapOptions hashOptions(Range fieldCount) {
        MapOptions options = new MapOptions();
        options.setFieldCount(fieldCount);
        return options;
    }

    @Test
    void writeStructMerge(TestInfo info) throws Exception {
        GeneratorItemReader gen1 = generator(100, DataType.HASH);
        gen1.setHashOptions(hashOptions(Range.of(5)));
        generate(info, client, gen1);
        GeneratorItemReader gen2 = generator(100, DataType.HASH);
        gen2.setHashOptions(hashOptions(Range.of(10)));
        generate(info, targetClient, gen2);
        StructItemReader<String, String> reader = RedisItemReader.struct(client);
        StructItemWriter<String, String> writer = RedisItemWriter.struct(targetClient);
        writer.setMerge(true);
        replicate(info, reader, writer);
        Map<String, String> actual = targetCommands.hgetall("gen:1");
        assertEquals(10, actual.size());
    }

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
        awaitUntilFalse(writer::isOpen);
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
        awaitUntilFalse(writer::isOpen);
        assertEquals(maps.size(), commands.keys("hash:*").size());
        for (int index = 0; index < maps.size(); index++) {
            Map<String, String> hash = commands.hgetall("hash:" + index);
            assertEquals(maps.get(index), hash);
        }
    }

    @Test
    void writeDel(TestInfo info) throws Exception {
        generate(info);
        GeneratorItemReader gen = generator(DEFAULT_GENERATOR_COUNT);
        Del<String, String, KeyValue<String>> del = new Del<>();
        del.setKeyFunction(KeyValue::getKey);
        OperationItemWriter<String, String, KeyValue<String>> writer = writer(del);
        run(info, gen, writer);
        awaitUntilFalse(writer::isOpen);
        assertEquals(0, commands.keys(GeneratorItemReader.DEFAULT_KEYSPACE + "*").size());
    }

    @Test
    void writeLpush(TestInfo info) throws Exception {
        GeneratorItemReader gen = generator(DataType.STRING);
        Lpush<String, String, KeyValue<String>> lpush = new Lpush<>();
        lpush.setKeyFunction(KeyValue::getKey);
        lpush.setValueFunction(v -> (String) v.getValue());
        OperationItemWriter<String, String, KeyValue<String>> writer = writer(lpush);
        run(info, gen, writer);
        awaitUntilFalse(writer::isOpen);
        assertEquals(DEFAULT_GENERATOR_COUNT, commands.dbsize());
        for (String key : commands.keys("*")) {
            assertEquals(DataType.LIST.getString(), commands.type(key));
        }
    }

    @Test
    void writeRpush(TestInfo info) throws Exception {
        GeneratorItemReader gen = generator(DataType.STRING);
        Rpush<String, String, KeyValue<String>> rpush = new Rpush<>();
        rpush.setKeyFunction(KeyValue::getKey);
        rpush.setValueFunction(v -> (String) v.getValue());
        OperationItemWriter<String, String, KeyValue<String>> writer = writer(rpush);
        run(info, gen, writer);
        awaitUntilFalse(writer::isOpen);
        assertEquals(DEFAULT_GENERATOR_COUNT, commands.dbsize());
        for (String key : commands.keys("*")) {
            assertEquals(DataType.LIST.getString(), commands.type(key));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void writeLpushAll(TestInfo info) throws Exception {
        GeneratorItemReader gen = generator(DataType.LIST);
        LpushAll<String, String, KeyValue<String>> lpushAll = new LpushAll<>();
        lpushAll.setKeyFunction(KeyValue::getKey);
        lpushAll.setValuesFunction(v -> (Collection<String>) v.getValue());
        OperationItemWriter<String, String, KeyValue<String>> writer = writer(lpushAll);
        run(info, gen, writer);
        awaitUntilFalse(writer::isOpen);
        assertEquals(DEFAULT_GENERATOR_COUNT, commands.dbsize());
        for (String key : commands.keys("*")) {
            assertEquals(DataType.LIST.getString(), commands.type(key));
        }
    }

    @Test
    void writeExpire(TestInfo info) throws Exception {
        GeneratorItemReader gen = generator(DataType.STRING);
        Duration ttl = Duration.ofMillis(1L);
        Expire<String, String, KeyValue<String>> expire = new Expire<>();
        expire.setKeyFunction(KeyValue::getKey);
        expire.setTtl(ttl);
        OperationItemWriter<String, String, KeyValue<String>> writer = writer(expire);
        run(info, gen, writer);
        awaitUntilFalse(writer::isOpen);
        awaitUntil(() -> commands.keys("*").isEmpty());
        assertEquals(0, commands.dbsize());
    }

    @Test
    void writeExpireAt(TestInfo info) throws Exception {
        GeneratorItemReader gen = generator(DataType.STRING);
        ExpireAt<String, String, KeyValue<String>> expireAt = new ExpireAt<>();
        expireAt.setKeyFunction(KeyValue::getKey);
        expireAt.setEpochFunction(v -> System.currentTimeMillis());
        OperationItemWriter<String, String, KeyValue<String>> writer = writer(expireAt);
        run(info, gen, writer);
        awaitUntilFalse(writer::isOpen);
        awaitUntil(() -> commands.keys("*").isEmpty());
        assertEquals(0, commands.dbsize());
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
        awaitUntilFalse(writer::isOpen);
        Assertions.assertEquals(messages.size(), commands.xlen(stream));
        List<StreamMessage<String, String>> xrange = commands.xrange(stream, io.lettuce.core.Range.create("-", "+"));
        for (int index = 0; index < xrange.size(); index++) {
            StreamMessage<String, String> message = xrange.get(index);
            Assertions.assertEquals(messages.get(index), message.getBody());
        }
    }

    @Test
    void writeStruct(TestInfo info) throws Exception {
        int count = 1000;
        GeneratorItemReader reader = generator(count);
        generate(info, client, reader);
        StructItemWriter<String, String> writer = RedisItemWriter.struct(client);
        run(info, reader, writer);
        List<String> keys = commands.keys("gen:*");
        assertEquals(count, keys.size());
    }

}
