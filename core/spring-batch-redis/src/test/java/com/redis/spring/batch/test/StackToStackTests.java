package com.redis.spring.batch.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.util.unit.DataSize;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyComparison;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.OperationValueReader;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.reader.DumpItemReader;
import com.redis.spring.batch.reader.StreamItemReader;
import com.redis.spring.batch.reader.StreamItemReader.StreamAckPolicy;
import com.redis.spring.batch.reader.StructItemReader;
import com.redis.spring.batch.writer.DumpItemWriter;
import com.redis.spring.batch.writer.OperationItemWriter;
import com.redis.spring.batch.writer.StructItemWriter;
import com.redis.spring.batch.writer.operation.Xadd;
import com.redis.testcontainers.RedisStackContainer;

import io.lettuce.core.Consumer;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.StreamMessage;

class StackToStackTests extends ModulesTests {

    private static final RedisStackContainer SOURCE = RedisContainerFactory.stack();

    private static final RedisStackContainer TARGET = RedisContainerFactory.stack();

    @Override
    protected RedisStackContainer getRedisContainer() {
        return SOURCE;
    }

    @Override
    protected RedisStackContainer getTargetRedisContainer() {
        return TARGET;
    }

    @Test
    void readStructMemoryUsage(TestInfo info) throws Exception {
        generate(info);
        long memLimit = 200;
        StructItemReader<String, String> reader = RedisItemReader.struct(client);
        reader.setMemoryUsageLimit(DataSize.ofBytes(memLimit));
        reader.open(new ExecutionContext());
        List<KeyValue<String>> keyValues = readAll(reader);
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
    void readStructMemoryUsageTTL(TestInfo info) throws Exception {
        String key = "myhash";
        Map<String, String> hash = new HashMap<>();
        hash.put("field1", "value1");
        hash.put("field2", "value2");
        commands.hset(key, hash);
        long ttl = System.currentTimeMillis() + 123456;
        commands.pexpireat(key, ttl);
        StructItemReader<String, String> reader = RedisItemReader.struct(client);
        reader.setMemoryUsageLimit(DataSize.ofBytes(-1));
        OperationValueReader<String, String, String, KeyValue<String>> executor = reader.operationValueReader();
        executor.open(new ExecutionContext());
        KeyValue<String> ds = executor.process(Arrays.asList(key)).get(0);
        Assertions.assertEquals(key, ds.getKey());
        Assertions.assertEquals(ttl, ds.getTtl());
        Assertions.assertEquals(DataType.HASH, ds.getType());
        Assertions.assertTrue(ds.getMemoryUsage() > 0);
        executor.close();
    }

    @Test
    void readStructMemLimit(TestInfo info) throws Exception {
        DataSize limit = DataSize.ofBytes(500);
        String key1 = "key:1";
        commands.set(key1, "bar");
        String key2 = "key:2";
        commands.set(key2, GeneratorItemReader.string(Math.toIntExact(limit.toBytes() * 2)));
        StructItemReader<String, String> reader = RedisItemReader.struct(client);
        reader.setMemoryUsageLimit(limit);
        reader.open(new ExecutionContext());
        List<KeyValue<String>> keyValues = readAll(reader);
        reader.close();
        Map<String, KeyValue<String>> map = keyValues.stream().collect(Collectors.toMap(s -> s.getKey(), s -> s));
        Assertions.assertNull(map.get(key2).getValue());
    }

    @Test
    void replicateStructMemLimit(TestInfo info) throws Exception {
        generate(info);
        StructItemReader<String, String> reader = RedisItemReader.struct(client);
        reader.setMemoryUsageLimit(DataSize.ofMegabytes(100));
        StructItemWriter<String, String> writer = RedisItemWriter.struct(targetClient);
        List<KeyComparison> diffs = replicate(info, reader, writer);
        assertEmpty(diffs);
    }

    @Test
    void replicateDumpMemLimitHigh(TestInfo info) throws Exception {
        generate(info);
        DumpItemReader reader = RedisItemReader.dump(client);
        reader.setMemoryUsageLimit(DataSize.ofMegabytes(100));
        DumpItemWriter writer = RedisItemWriter.dump(targetClient);
        assertEmpty(replicate(info, reader, writer));
    }

    @Test
    void replicateDumpMemLimitLow(TestInfo info) throws Exception {
        generate(info);
        Assertions.assertTrue(commands.dbsize() > 10);
        long memLimit = 1500;
        DumpItemReader reader = RedisItemReader.dump(client);
        reader.setMemoryUsageLimit(DataSize.ofBytes(memLimit));
        DumpItemWriter writer = RedisItemWriter.dump(targetClient);
        run(info, reader, writer);
        awaitUntilFalse(reader::isOpen);
        awaitUntilFalse(writer::isOpen);
        StructItemReader<String, String> fullReader = RedisItemReader.struct(client);
        fullReader.setMemoryUsageLimit(DataSize.ofBytes(-1));
        fullReader.open(new ExecutionContext());
        List<KeyValue<String>> items = readAll(fullReader);
        fullReader.close();
        Predicate<KeyValue<String>> isMemKey = v -> v.getMemoryUsage() > memLimit;
        List<KeyValue<String>> bigkeys = items.stream().filter(isMemKey).collect(Collectors.toList());
        Assertions.assertEquals(commands.dbsize(), bigkeys.size() + targetCommands.dbsize());
    }

    @Test
    void writeStructMultiExec(TestInfo info) throws Exception {
        int count = 10;
        GeneratorItemReader reader = generator(count);
        StructItemWriter<String, String> writer = RedisItemWriter.struct(client);
        writer.setMultiExec(true);
        SimpleStepBuilder<KeyValue<String>, KeyValue<String>> step = step(info, 1, reader, null, writer);
        run(info, step);
        assertEquals(count, commands.dbsize());
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
        OperationItemWriter<String, String, Map<String, String>> writer = writer(xadd);
        writer.setMultiExec(true);
        run(testInfo, reader, writer);
        Assertions.assertEquals(messages.size(), commands.xlen(stream));
        List<StreamMessage<String, String>> xrange = commands.xrange(stream, io.lettuce.core.Range.create("-", "+"));
        for (int index = 0; index < xrange.size(); index++) {
            StreamMessage<String, String> message = xrange.get(index);
            Assertions.assertEquals(messages.get(index), message.getBody());
        }
    }

    @Test
    void readMultipleStreams(TestInfo testInfo) throws Exception {
        String consumerGroup = "consumerGroup";
        generateStreams(testInfo(testInfo, "streams"), 277);
        KeyScanArgs args = KeyScanArgs.Builder.type(DataType.STREAM.getString());
        final List<String> keys = ScanIterator.scan(commands, args).stream().collect(Collectors.toList());
        for (String key : keys) {
            long count = commands.xlen(key);
            StreamItemReader<String, String> reader1 = streamReader(key, Consumer.from(consumerGroup, "consumer1"));
            reader1.setAckPolicy(StreamAckPolicy.MANUAL);
            StreamItemReader<String, String> reader2 = streamReader(key, Consumer.from(consumerGroup, "consumer2"));
            reader2.setAckPolicy(StreamAckPolicy.MANUAL);
            ListItemWriter<StreamMessage<String, String>> writer1 = new ListItemWriter<>();
            TestInfo testInfo1 = new SimpleTestInfo(testInfo, key, "1");
            TaskletStep step1 = faultTolerant(flushingStep(testInfo1, reader1, writer1)).build();
            TestInfo testInfo2 = new SimpleTestInfo(testInfo, key, "2");
            ListItemWriter<StreamMessage<String, String>> writer2 = new ListItemWriter<>();
            TaskletStep step2 = faultTolerant(flushingStep(testInfo2, reader2, writer2)).build();
            SimpleFlow flow1 = flow("flow1").start(step1).build();
            SimpleFlow flow2 = flow("flow2").start(step2).build();
            SimpleFlow flow = flow("replicate").split(new SimpleAsyncTaskExecutor()).add(flow1, flow2).build();
            run(job(testInfo1).start(flow).build().build());
            awaitUntilFalse(reader1::isOpen);
            awaitUntilFalse(reader2::isOpen);
            Assertions.assertEquals(count, writer1.getWrittenItems().size() + writer2.getWrittenItems().size());
            assertMessageBody(writer1.getWrittenItems());
            assertMessageBody(writer2.getWrittenItems());
            Assertions.assertEquals(count, commands.xpending(key, consumerGroup).getCount());
            reader1 = streamReader(key, Consumer.from(consumerGroup, "consumer1"));
            reader1.setAckPolicy(StreamAckPolicy.MANUAL);
            reader1.open(new ExecutionContext());
            reader1.ack(writer1.getWrittenItems());
            reader1.close();
            reader2 = streamReader(key, Consumer.from(consumerGroup, "consumer2"));
            reader2.setAckPolicy(StreamAckPolicy.MANUAL);
            reader2.open(new ExecutionContext());
            reader2.ack(writer2.getWrittenItems());
            reader2.close();
            Assertions.assertEquals(0, commands.xpending(key, consumerGroup).getCount());
        }
    }

    private static FlowBuilder<SimpleFlow> flow(String name) {
        return new FlowBuilder<>(name);
    }

}
