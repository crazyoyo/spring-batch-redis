package com.redis.spring.batch.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.util.unit.DataSize;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.Dump;
import com.redis.spring.batch.common.SimpleOperationExecutor;
import com.redis.spring.batch.common.Struct;
import com.redis.spring.batch.common.Struct.Type;
import com.redis.spring.batch.gen.GeneratorItemReader;
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
    void readWriteStruct(TestInfo info) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(100);
        generate(info, gen);
        RedisItemReader<String, String, Struct<String>> reader = structReader(info, client);
        RedisItemWriter<String, String, Struct<String>> writer = structWriter(targetClient);
        run(info, reader, writer);
        awaitClosed(reader);
        awaitClosed(writer);
        Assertions.assertTrue(compare(info));
    }

    @Test
    void dumpAndRestore(TestInfo info) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(100);
        generate(info, gen);
        RedisItemReader<byte[], byte[], Dump<byte[]>> reader = dumpReader(info, client, ByteArrayCodec.INSTANCE);
        RedisItemWriter<byte[], byte[], Dump<byte[]>> writer = dumpWriter(targetClient, ByteArrayCodec.INSTANCE);
        run(info, reader, writer);
        awaitClosed(reader);
        awaitClosed(writer);
        Assertions.assertTrue(compare(info));
    }

    @Test
    void replicateLiveStruct(TestInfo info) throws Exception {
        enableKeyspaceNotifications(client);
        RedisItemReader<String, String, Struct<String>> reader = structReader(info, client);
        RedisItemWriter<String, String, Struct<String>> writer = structWriter(targetClient);
        RedisItemReader<String, String, Struct<String>> liveReader = liveStructReader(info, client);
        RedisItemWriter<String, String, Struct<String>> liveWriter = structWriter(targetClient);
        Assertions.assertTrue(replicateLive(info, reader, writer, liveReader, liveWriter));
    }

    private static final String DEFAULT_CONSUMER_GROUP = "consumerGroup";

    @Test
    void readMultipleStreams(TestInfo testInfo) throws Exception {
        generateStreams(testInfo(testInfo, "streams"), 277);
        final List<String> keys = ScanIterator.scan(commands, KeyScanArgs.Builder.type(Type.STREAM.getString())).stream()
                .collect(Collectors.toList());
        for (String key : keys) {
            long count = commands.xlen(key);
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
            Assertions.assertEquals(count, commands.xpending(key, DEFAULT_CONSUMER_GROUP).getCount());
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
            Assertions.assertEquals(0, commands.xpending(key, DEFAULT_CONSUMER_GROUP).getCount());
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
        xadd.setKey(stream);
        xadd.setBodyFunction(Function.identity());
        OperationItemWriter<String, String, Map<String, String>> writer = new OperationItemWriter<>(client, StringCodec.UTF8,
                xadd);
        writer.setMultiExec(true);
        run(testInfo, reader, writer);
        Assertions.assertEquals(messages.size(), commands.xlen(stream));
        List<StreamMessage<String, String>> xrange = commands.xrange(stream, Range.create("-", "+"));
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
        commands.hset(key, hash);
        long ttl = System.currentTimeMillis() + 123456;
        commands.pexpireat(key, ttl);
        RedisItemReader<String, String, Struct<String>> reader = RedisItemReader.struct(client, StringCodec.UTF8);
        reader.setMemoryUsageLimit(DataSize.ofBytes(-1));
        SimpleOperationExecutor<String, String, String, Struct<String>> executor = reader.operationExecutor();
        executor.open(new ExecutionContext());
        Struct<String> ds = executor.execute(Arrays.asList(key)).get(0);
        Assertions.assertEquals(key, ds.getKey());
        Assertions.assertEquals(ttl, ds.getTtl());
        Assertions.assertEquals(Type.HASH, ds.getType());
        Assertions.assertTrue(ds.getMemoryUsage().toBytes() > 0);
        executor.close();
    }

    @Test
    void structMemUsage(TestInfo info) throws Exception {
        generate(info);
        long memLimit = 200;
        RedisItemReader<String, String, Struct<String>> reader = structReader(info, client);
        reader.setName(name(info) + "-reader");
        reader.setMemoryUsageLimit(DataSize.ofBytes(memLimit));
        reader.open(new ExecutionContext());
        List<Struct<String>> keyValues = BatchUtils.readAll(reader);
        reader.close();
        Assertions.assertFalse(keyValues.isEmpty());
        for (Struct<String> keyValue : keyValues) {
            Assertions.assertTrue(keyValue.getMemoryUsage().toBytes() > 0);
            if (keyValue.getMemoryUsage().toBytes() > memLimit) {
                Assertions.assertNull(keyValue.getValue());
            }
        }
    }

    @Test
    void replicateStructMemLimit(TestInfo info) throws Exception {
        generate(info);
        RedisItemReader<String, String, Struct<String>> reader = structReader(info, client);
        reader.setMemoryUsageLimit(DataSize.ofMegabytes(100));
        RedisItemWriter<String, String, Struct<String>> writer = structWriter(targetClient);
        run(info, reader, writer);
        awaitClosed(reader);
        awaitClosed(writer);
        Assertions.assertTrue(compare(info));
    }

    @Test
    void replicateDumpMemLimitHigh(TestInfo info) throws Exception {
        generate(info);
        RedisItemReader<byte[], byte[], Dump<byte[]>> reader = dumpReader(info, client, ByteArrayCodec.INSTANCE);
        reader.setMemoryUsageLimit(DataSize.ofMegabytes(100));
        RedisItemWriter<byte[], byte[], Dump<byte[]>> writer = dumpWriter(targetClient, ByteArrayCodec.INSTANCE);
        run(info, reader, writer);
        awaitClosed(reader);
        awaitClosed(writer);
        Assertions.assertTrue(compare(info));
    }

    @Test
    void replicateDumpMemLimitLow(TestInfo info) throws Exception {
        generate(info);
        Assertions.assertTrue(commands.dbsize() > 10);
        long memLimit = 1500;
        RedisItemReader<byte[], byte[], Dump<byte[]>> reader = dumpReader(info, client, ByteArrayCodec.INSTANCE);
        reader.setMemoryUsageLimit(DataSize.ofBytes(memLimit));
        RedisItemWriter<byte[], byte[], Dump<byte[]>> writer = dumpWriter(targetClient, ByteArrayCodec.INSTANCE);
        run(info, reader, writer);
        awaitClosed(reader);
        awaitClosed(writer);
        RedisItemReader<String, String, Struct<String>> fullReader = structReader(info, client);
        fullReader.setName(name(info) + "-fullReader");
        fullReader.setJobRepository(jobRepository);
        fullReader.setTransactionManager(transactionManager);
        fullReader.setMemoryUsageLimit(DataSize.ofBytes(-1));
        fullReader.open(new ExecutionContext());
        List<Struct<String>> items = BatchUtils.readAll(fullReader);
        fullReader.close();
        Predicate<Struct<String>> isMemKey = v -> v.getMemoryUsage().toBytes() > memLimit;
        List<Struct<String>> bigkeys = items.stream().filter(isMemKey).collect(Collectors.toList());
        Assertions.assertEquals(commands.dbsize(), bigkeys.size() + targetCommands.dbsize());
    }

    @Test
    void replicateMemLimit(TestInfo info) throws Exception {
        DataSize limit = DataSize.ofBytes(500);
        String key1 = "key:1";
        commands.set(key1, "bar");
        String key2 = "key:2";
        commands.set(key2, GeneratorItemReader.string(Math.toIntExact(limit.toBytes() * 2)));
        RedisItemReader<String, String, Struct<String>> reader = structReader(info, client);
        reader.setName(name(info) + "-reader");
        reader.setMemoryUsageLimit(limit);
        reader.open(new ExecutionContext());
        List<Struct<String>> keyValues = BatchUtils.readAll(reader);
        reader.close();
        Map<String, Struct<String>> map = keyValues.stream().collect(Collectors.toMap(s -> s.getKey(), s -> s));
        Assertions.assertNull(map.get(key2).getValue());
    }

    @Test
    void replicateStruct(TestInfo info) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(10000);
        gen.setTypes(Type.HASH, Type.LIST, Type.SET, Type.STREAM, Type.STRING, Type.ZSET);
        generate(info, gen);
        RedisItemReader<byte[], byte[], Struct<byte[]>> reader = structReader(info, client, ByteArrayCodec.INSTANCE);
        RedisItemWriter<byte[], byte[], Struct<byte[]>> writer = structWriter(targetClient, ByteArrayCodec.INSTANCE);
        run(info, reader, writer);
        awaitClosed(reader);
        awaitClosed(writer);
        Assertions.assertTrue(compare(info));
    }

    @Test
    void replicateStructEmptyCollections(TestInfo info) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(10000);
        gen.setTypes(Type.HASH, Type.LIST, Type.SET, Type.STREAM, Type.STRING, Type.ZSET);
        generate(info, gen);
        RedisItemReader<byte[], byte[], Struct<byte[]>> reader = structReader(info, client, ByteArrayCodec.INSTANCE);
        RedisItemWriter<byte[], byte[], Struct<byte[]>> writer = structWriter(targetClient, ByteArrayCodec.INSTANCE);
        run(info, reader, writer);
        awaitClosed(reader);
        awaitClosed(writer);
        Assertions.assertTrue(compare(info));
    }

}
