package com.redis.spring.batch.test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.util.unit.DataSize;

import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.SimpleOperationExecutor;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.reader.DumpItemReader;
import com.redis.spring.batch.reader.StructItemReader;
import com.redis.spring.batch.util.BatchUtils;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;

import io.lettuce.core.codec.ByteArrayCodec;

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
    void readStructMemoryUsage(TestInfo info) throws Exception {
        generate(info);
        long memLimit = 200;
        StructItemReader<String, String> reader = structReader(info, client);
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
    void readStructMemoryUsageTTL(TestInfo info) throws Exception {
        String key = "myhash";
        Map<String, String> hash = new HashMap<>();
        hash.put("field1", "value1");
        hash.put("field2", "value2");
        commands.hset(key, hash);
        long ttl = System.currentTimeMillis() + 123456;
        commands.pexpireat(key, ttl);
        StructItemReader<String, String> reader = structReader(info, client);
        reader.setMemoryUsageLimit(DataSize.ofBytes(-1));
        SimpleOperationExecutor<String, String, String, KeyValue<String>> executor = reader.operationExecutor();
        executor.open(new ExecutionContext());
        KeyValue<String> ds = executor.process(Arrays.asList(key)).get(0);
        Assertions.assertEquals(key, ds.getKey());
        Assertions.assertEquals(ttl, ds.getTtl());
        Assertions.assertEquals(DataType.HASH, ds.getType());
        Assertions.assertTrue(ds.getMemoryUsage() > 0);
        executor.close();
    }

    @Test
    void replicateStructMemLimit(TestInfo info) throws Exception {
        generate(info);
        StructItemReader<String, String> reader = structReader(info, client);
        reader.setMemoryUsageLimit(DataSize.ofMegabytes(100));
        RedisItemWriter<String, String, KeyValue<String>> writer = structWriter(targetClient);
        run(info, reader, writer);
        awaitClosed(reader);
        awaitClosed(writer);
        Assertions.assertTrue(compare(info));
    }

    @Test
    void replicateDumpMemLimitHigh(TestInfo info) throws Exception {
        generate(info);
        DumpItemReader<byte[], byte[]> reader = dumpReader(info, client, ByteArrayCodec.INSTANCE);
        reader.setMemoryUsageLimit(DataSize.ofMegabytes(100));
        RedisItemWriter<byte[], byte[], KeyValue<byte[]>> writer = dumpWriter(targetClient, ByteArrayCodec.INSTANCE);
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
        DumpItemReader<byte[], byte[]> reader = dumpReader(info, client, ByteArrayCodec.INSTANCE);
        reader.setMemoryUsageLimit(DataSize.ofBytes(memLimit));
        RedisItemWriter<byte[], byte[], KeyValue<byte[]>> writer = dumpWriter(targetClient, ByteArrayCodec.INSTANCE);
        run(info, reader, writer);
        awaitClosed(reader);
        awaitClosed(writer);
        StructItemReader<String, String> fullReader = structReader(info, client);
        fullReader.setName(name(info) + "-fullReader");
        fullReader.setJobRepository(jobRepository);
        fullReader.setTransactionManager(transactionManager);
        fullReader.setMemoryUsageLimit(DataSize.ofBytes(-1));
        fullReader.open(new ExecutionContext());
        List<KeyValue<String>> items = BatchUtils.readAll(fullReader);
        fullReader.close();
        Predicate<KeyValue<String>> isMemKey = v -> v.getMemoryUsage() > memLimit;
        List<KeyValue<String>> bigkeys = items.stream().filter(isMemKey).collect(Collectors.toList());
        Assertions.assertEquals(commands.dbsize(), bigkeys.size() + targetCommands.dbsize());
    }

    @Test
    void replicateMemLimit(TestInfo info) throws Exception {
        DataSize limit = DataSize.ofBytes(500);
        String key1 = "key:1";
        commands.set(key1, "bar");
        String key2 = "key:2";
        commands.set(key2, GeneratorItemReader.string(Math.toIntExact(limit.toBytes() * 2)));
        StructItemReader<String, String> reader = structReader(info, client);
        reader.setName(name(info) + "-reader");
        reader.setMemoryUsageLimit(limit);
        reader.open(new ExecutionContext());
        List<KeyValue<String>> keyValues = BatchUtils.readAll(reader);
        reader.close();
        Map<String, KeyValue<String>> map = keyValues.stream().collect(Collectors.toMap(s -> s.getKey(), s -> s));
        Assertions.assertNull(map.get(key2).getValue());
    }

}
