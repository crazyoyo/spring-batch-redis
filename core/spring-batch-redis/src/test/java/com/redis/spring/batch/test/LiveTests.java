package com.redis.spring.batch.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.item.ExecutionContext;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.ReaderMode;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.reader.DumpItemReader;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader;
import com.redis.spring.batch.reader.StructItemReader;
import com.redis.spring.batch.util.BatchUtils;
import com.redis.spring.batch.util.CodecUtils;
import com.redis.spring.batch.writer.DumpItemWriter;
import com.redis.spring.batch.writer.StructItemWriter;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;

abstract class LiveTests extends BatchTests {

    @Test
    void readKeyspaceNotificationsDedupe() throws Exception {
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
    void readLiveType(TestInfo info) throws Exception {
        enableKeyspaceNotifications(client);
        StructItemReader<String, String> reader = RedisItemReader.struct(client);
        configureReader(info, reader);
        reader.setMode(ReaderMode.LIVE);
        reader.setKeyType(DataType.HASH);
        reader.open(new ExecutionContext());
        GeneratorItemReader gen = generator(100);
        generate(info, gen);
        reader.open(new ExecutionContext());
        List<KeyValue<String>> keyValues = BatchUtils.readAll(reader);
        reader.close();
        Assertions.assertTrue(keyValues.stream().allMatch(v -> v.getType() == DataType.HASH));
    }

    @Test
    void readStructLive(TestInfo info) throws Exception {
        enableKeyspaceNotifications(client);
        StructItemReader<byte[], byte[]> reader = RedisItemReader.struct(client, ByteArrayCodec.INSTANCE);
        configureReader(info, reader);
        reader.setMode(ReaderMode.LIVE);
        reader.setNotificationQueueCapacity(10000);
        reader.open(new ExecutionContext());
        int count = 123;
        GeneratorItemReader gen = generator(count, DataType.HASH, DataType.STRING);
        generate(info, gen);
        List<KeyValue<byte[]>> list = BatchUtils.readAll(reader);
        Function<byte[], String> toString = CodecUtils.toStringKeyFunction(ByteArrayCodec.INSTANCE);
        Set<String> keys = list.stream().map(KeyValue::getKey).map(toString).collect(Collectors.toSet());
        Assertions.assertEquals(count, keys.size());
        reader.close();
    }

    @Test
    void replicateDumpLive(TestInfo info) throws Exception {
        enableKeyspaceNotifications(client);
        DumpItemReader reader = RedisItemReader.dump(client);
        DumpItemWriter writer = RedisItemWriter.dump(targetClient);
        DumpItemReader liveReader = RedisItemReader.dump(client);
        DumpItemWriter liveWriter = RedisItemWriter.dump(targetClient);
        assertEmpty(replicateLive(info, reader, writer, liveReader, liveWriter));
    }

    @Test
    void replicateStructLive(TestInfo info) throws Exception {
        enableKeyspaceNotifications(client);
        StructItemReader<String, String> reader = RedisItemReader.struct(client);
        StructItemWriter<String, String> writer = RedisItemWriter.struct(targetClient);
        StructItemReader<String, String> liveReader = RedisItemReader.struct(client);
        StructItemWriter<String, String> liveWriter = RedisItemWriter.struct(targetClient);
        assertEmpty(replicateLive(info, reader, writer, liveReader, liveWriter));
    }

    @Test
    void replicateDumpLiveOnly(TestInfo info) throws Exception {
        enableKeyspaceNotifications(client);
        DumpItemReader reader = RedisItemReader.dump(client);
        configureReader(info, reader);
        reader.setMode(ReaderMode.LIVE);
        reader.setNotificationQueueCapacity(100000);
        DumpItemWriter writer = RedisItemWriter.dump(targetClient);
        Executors.newSingleThreadScheduledExecutor().execute(() -> {
            awaitUntil(reader::isOpen);
            GeneratorItemReader gen = generator(100, DataType.HASH, DataType.LIST, DataType.SET, DataType.STRING,
                    DataType.ZSET);
            try {
                generate(info, gen);
            } catch (JobExecutionException e) {
                throw new RuntimeException("Could not run data gen", e);
            }
        });
        run(info, flushingStep(info, reader, writer));
        awaitUntilFalse(reader::isOpen);
        awaitUntilFalse(writer::isOpen);
        assertEmpty(compare(info));
    }

    @Test
    void replicateSetLiveOnly(TestInfo info) throws Exception {
        enableKeyspaceNotifications(client);
        String key = "myset";
        commands.sadd(key, "1", "2", "3", "4", "5");
        StructItemReader<String, String> reader = RedisItemReader.struct(client);
        configureReader(info, reader);
        reader.setMode(ReaderMode.LIVE);
        reader.setNotificationQueueCapacity(100);
        StructItemWriter<String, String> writer = RedisItemWriter.struct(targetClient);
        Executors.newSingleThreadScheduledExecutor().execute(() -> {
            awaitUntil(reader::isOpen);
            awaitUntil(writer::isOpen);
            commands.srem(key, "5");
        });
        run(info, flushingStep(info, reader, writer));
        awaitUntilFalse(reader::isOpen);
        awaitUntilFalse(writer::isOpen);
        assertEquals(commands.smembers(key), targetCommands.smembers(key));
    }

}
