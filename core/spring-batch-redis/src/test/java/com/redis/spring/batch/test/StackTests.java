package com.redis.spring.batch.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.util.unit.DataSize;

import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.ValueType;
import com.redis.spring.batch.gen.DataType;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.reader.KeyValueItemProcessor;
import com.redis.spring.batch.reader.StreamItemReader;
import com.redis.spring.batch.reader.StreamItemReader.StreamAckPolicy;
import com.redis.spring.batch.util.BatchUtils;
import com.redis.spring.batch.writer.OperationItemWriter;
import com.redis.spring.batch.writer.operation.Xadd;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;

import io.lettuce.core.Consumer;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.Range;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;

class StackTests extends ModulesTests {

    private static final RedisStackContainer SOURCE = RedisContainerFactory.stack();

    private static final RedisStackContainer TARGET = RedisContainerFactory.stack();

    @Override
    protected RedisServer getRedisServer() {
        return SOURCE;
    }

    @Override
    protected RedisServer getTargetRedisServer() {
        return TARGET;
    }

    @Test
    void structs(TestInfo info) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(100);
        generate(info, gen);
        RedisItemReader<String, String> reader = structSourceReader(info);
        run(info, reader, structTargetWriter());
        Assertions.assertTrue(compare(info));
    }

    private RedisItemWriter<String, String> structTargetWriter() {
        return structWriter(targetClient);
    }

    private RedisItemReader<String, String> structSourceReader(TestInfo info) {
        return structReader(info, client);
    }

    @Test
    void dumpAndRestore(TestInfo info) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(100);
        generate(info, gen);
        RedisItemReader<byte[], byte[]> reader = reader(info, client, ByteArrayCodec.INSTANCE);
        RedisItemWriter<byte[], byte[]> writer = writer(targetClient, ByteArrayCodec.INSTANCE);
        run(info, reader, writer);
        Assertions.assertTrue(compare(info));
    }

    @Test
    void liveTypeBasedReplication(TestInfo info) throws Exception {
        enableKeyspaceNotifications(client);
        RedisItemReader<String, String> reader = structSourceReader(info);
        RedisItemWriter<String, String> writer = structTargetWriter();
        RedisItemReader<String, String> liveReader = structReader(info, client);
        RedisItemWriter<String, String> liveWriter = structTargetWriter();
        Assertions.assertTrue(liveReplication(info, reader, writer, liveReader, liveWriter));
    }

    private static final String DEFAULT_CONSUMER_GROUP = "consumerGroup";

    @Test
    void readMultipleStreams(TestInfo testInfo) throws Exception {
        generateStreams(testInfo(testInfo, "streams"), 277);
        final List<String> keys = ScanIterator.scan(connection.sync(), KeyScanArgs.Builder.type(KeyValue.STREAM)).stream()
                .collect(Collectors.toList());
        for (String key : keys) {
            long count = connection.sync().xlen(key);
            StreamItemReader<String, String> reader1 = streamReader(key, Consumer.from(DEFAULT_CONSUMER_GROUP, "consumer1"));
            reader1.setAckPolicy(StreamAckPolicy.MANUAL);
            StreamItemReader<String, String> reader2 = streamReader(key, Consumer.from(DEFAULT_CONSUMER_GROUP, "consumer2"));
            reader2.setAckPolicy(StreamAckPolicy.MANUAL);
            ListItemWriter<StreamMessage<String, String>> writer1 = new ListItemWriter<>();
            TestInfo testInfo1 = testInfo(testInfo, key, "1");
            JobExecution execution1 = runAsync(job(testInfo1).start(flushingStep(testInfo1, reader1, writer1).build()).build());
            ListItemWriter<StreamMessage<String, String>> writer2 = new ListItemWriter<>();
            TestInfo testInfo2 = testInfo(testInfo, key, "2");
            JobExecution execution2 = runAsync(job(testInfo2).start(flushingStep(testInfo2, reader2, writer2).build()).build());
            awaitTermination(execution1);
            awaitClosed(reader1);
            awaitClosed(writer1);
            awaitTermination(execution2);
            awaitClosed(reader2);
            awaitClosed(writer2);
            Assertions.assertEquals(count, writer1.getWrittenItems().size() + writer2.getWrittenItems().size());
            assertMessageBody(writer1.getWrittenItems());
            assertMessageBody(writer2.getWrittenItems());
            RedisModulesCommands<String, String> sync = connection.sync();
            Assertions.assertEquals(count, sync.xpending(key, DEFAULT_CONSUMER_GROUP).getCount());
            reader1 = streamReader(key, Consumer.from(DEFAULT_CONSUMER_GROUP, "consumer1"));
            reader1.setAckPolicy(StreamAckPolicy.MANUAL);
            reader1.open(new ExecutionContext());
            reader1.ack(writer1.getWrittenItems());
            reader1.close();
            reader2 = streamReader(key, Consumer.from(DEFAULT_CONSUMER_GROUP, "consumer2"));
            reader2.setAckPolicy(StreamAckPolicy.MANUAL);
            reader2.open(new ExecutionContext());
            reader2.ack(writer2.getWrittenItems());
            reader2.close();
            Assertions.assertEquals(0, sync.xpending(key, DEFAULT_CONSUMER_GROUP).getCount());
        }
    }

    @Test
    void writeStreamMultiExec(TestInfo testInfo) throws Exception {
        String stream = "stream:1";
        List<Map<String, String>> messages = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            Map<String, String> body = new HashMap<>();
            body.put("field1", "value1");
            body.put("field2", "value2");
            messages.add(body);
        }
        ListItemReader<Map<String, String>> reader = new ListItemReader<>(messages);
        Xadd<String, String, Map<String, String>> xadd = new Xadd<>();
        xadd.key(t -> stream);
        xadd.body(Function.identity());
        xadd.args(m -> null);
        OperationItemWriter<String, String, Map<String, String>> writer = new OperationItemWriter<>(client, StringCodec.UTF8,
                xadd);
        writer.setMultiExec(true);
        run(testInfo, reader, writer);
        RedisStreamCommands<String, String> sync = connection.sync();
        Assertions.assertEquals(messages.size(), sync.xlen(stream));
        List<StreamMessage<String, String>> xrange = sync.xrange(stream, Range.create("-", "+"));
        for (int index = 0; index < xrange.size(); index++) {
            StreamMessage<String, String> message = xrange.get(index);
            Assertions.assertEquals(messages.get(index), message.getBody());
        }
    }

    @Test
    void luaHashMem() throws Exception {
        String key = "myhash";
        Map<String, String> hash = new HashMap<>();
        hash.put("field1", "value1");
        hash.put("field2", "value2");
        connection.sync().hset(key, hash);
        long ttl = System.currentTimeMillis() + 123456;
        connection.sync().pexpireat(key, ttl);
        KeyValueItemProcessor<String, String> operation = new KeyValueItemProcessor<>(client, StringCodec.UTF8);
        operation.setValueType(ValueType.STRUCT);
        operation.setMemoryUsageLimit(DataSize.ofBytes(-1));
        operation.open(new ExecutionContext());
        KeyValue<String> ds = operation.process(key).get(0);
        Assertions.assertEquals(key, ds.getKey());
        Assertions.assertEquals(ttl, ds.getTtl());
        Assertions.assertEquals(KeyValue.HASH, ds.getType());
        Assertions.assertTrue(ds.getMemoryUsage() > 0);
        operation.close();
    }

    @Test
    void structsMemUsage(TestInfo info) throws Exception {
        generate(info);
        long memLimit = 200;
        RedisItemReader<String, String> reader = structReader(info, client);
        reader.setName(name(info) + "-reader");
        reader.setMemoryUsageLimit(DataSize.ofBytes(memLimit));
        reader.open(new ExecutionContext());
        List<KeyValue<String>> keyValues = BatchUtils.readAll(reader);
        reader.close();
        Assertions.assertFalse(keyValues.isEmpty());
        for (KeyValue<String> keyValue : keyValues) {
            Assertions.assertTrue(keyValue.getMemoryUsage() > 0);
            if (keyValue.getMemoryUsage() > memLimit) {
                Assertions.assertNull(keyValue.getValue());
            }
        }
    }

    @Test
    void replicateStructsMemLimit(TestInfo info) throws Exception {
        generate(info);
        RedisItemReader<String, String> reader = structReader(info, client);
        reader.setMemoryUsageLimit(DataSize.ofMegabytes(100));
        RedisItemWriter<String, String> writer = structTargetWriter();
        run(info, reader, writer);
        Assertions.assertTrue(compare(info));
    }

    @Test
    void replicateDumpsMemLimitHigh(TestInfo info) throws Exception {
        generate(info);
        RedisItemReader<byte[], byte[]> reader = reader(info, client, ByteArrayCodec.INSTANCE);
        reader.setMemoryUsageLimit(DataSize.ofMegabytes(100));
        RedisItemWriter<byte[], byte[]> writer = writer(targetClient, ByteArrayCodec.INSTANCE);
        run(info, reader, writer);
        Assertions.assertTrue(compare(info));
    }

    @Test
    void replicateDumpsMemLimitLow(TestInfo info) throws Exception {
        generate(info);
        Assertions.assertTrue(connection.sync().dbsize() > 10);
        long memLimit = 1500;
        RedisItemReader<byte[], byte[]> reader = reader(info, client, ByteArrayCodec.INSTANCE);
        reader.setMemoryUsageLimit(DataSize.ofBytes(memLimit));
        RedisItemWriter<byte[], byte[]> writer = writer(targetClient, ByteArrayCodec.INSTANCE);
        run(info, reader, writer);
        RedisItemReader<String, String> fullReader = structReader(info, client);
        fullReader.setName(name(info) + "-fullReader");
        fullReader.setJobRepository(jobRepository);
        fullReader.setMemoryUsageLimit(DataSize.ofBytes(-1));
        fullReader.open(new ExecutionContext());
        List<KeyValue<String>> items = BatchUtils.readAll(fullReader);
        fullReader.close();
        List<KeyValue<String>> bigkeys = items.stream().filter(ds -> ds.getMemoryUsage() > memLimit)
                .collect(Collectors.toList());
        Assertions.assertEquals(connection.sync().dbsize(), bigkeys.size() + targetConnection.sync().dbsize());
    }

    @Test
    void replicateMemLimit(TestInfo info) throws Exception {
        DataSize limit = DataSize.ofBytes(500);
        String key1 = "key:1";
        connection.sync().set(key1, "bar");
        String key2 = "key:2";
        connection.sync().set(key2, GeneratorItemReader.randomString(Math.toIntExact(limit.toBytes() * 2)));
        RedisItemReader<String, String> reader = structReader(info, client);
        reader.setName(name(info) + "-reader");
        reader.setMemoryUsageLimit(limit);
        reader.open(new ExecutionContext());
        List<KeyValue<String>> keyValues = BatchUtils.readAll(reader);
        reader.close();
        Map<String, KeyValue<String>> map = keyValues.stream().collect(Collectors.toMap(s -> s.getKey(), s -> s));
        Assertions.assertNull(map.get(key2).getValue());
    }

    @Test
    void blockBigKeys(TestInfo info) throws Exception {
        enableKeyspaceNotifications(client);
        RedisItemReader<String, String> reader = structReader(info, client);
        setLive(reader);
        reader.setMemoryUsageLimit(DataSize.ofBytes(300));
        reader.setName("blockBigKeys");
        reader.open(new ExecutionContext());
        RedisModulesCommands<String, String> commands = connection.sync();
        String key = "key:1";
        AtomicInteger index = new AtomicInteger();
        Awaitility.await().until(() -> {
            commands.sadd(key, "value:" + index.incrementAndGet());
            return reader.getBlockedKeys().contains(key);
        });
        reader.close();
    }

    @Test
    void replicateDs(TestInfo info) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(10000);
        gen.setTypes(DataType.HASH, DataType.LIST, DataType.SET, DataType.STREAM, DataType.STRING, DataType.ZSET);
        generate(info, gen);
        RedisItemReader<byte[], byte[]> reader = structReader(info, client, ByteArrayCodec.INSTANCE);
        RedisItemWriter<byte[], byte[]> writer = structWriter(targetClient, ByteArrayCodec.INSTANCE);
        run(info, reader, writer);
        Assertions.assertTrue(compare(info));
    }

    @Test
    void replicateDsEmptyCollections(TestInfo info) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(10000);
        gen.setTypes(DataType.HASH, DataType.LIST, DataType.SET, DataType.STREAM, DataType.STRING, DataType.ZSET);
        generate(info, gen);
        RedisItemReader<byte[], byte[]> reader = structReader(info, client, ByteArrayCodec.INSTANCE);
        RedisItemWriter<byte[], byte[]> writer = structWriter(targetClient, ByteArrayCodec.INSTANCE);
        run(info, reader, writer);
        Assertions.assertTrue(compare(info));
    }

}
