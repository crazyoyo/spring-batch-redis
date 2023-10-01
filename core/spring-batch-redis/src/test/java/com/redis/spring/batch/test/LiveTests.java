package com.redis.spring.batch.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.item.ExecutionContext;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.ReaderMode;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader;
import com.redis.spring.batch.reader.StructItemReader;
import com.redis.spring.batch.util.BatchUtils;
import com.redis.spring.batch.util.CodecUtils;
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
        RedisItemReader<String, String, KeyValue<String>> reader = RedisItemReader.struct(client);
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
    void replicateDumpLive(TestInfo info) throws Exception {
        enableKeyspaceNotifications(client);
        RedisItemReader<byte[], byte[], KeyValue<byte[]>> reader = RedisItemReader.dump(client);
        configureReader(info, reader);
        RedisItemWriter<byte[], byte[], KeyValue<byte[]>> writer = RedisItemWriter.dump(targetClient);
        RedisItemReader<byte[], byte[], KeyValue<byte[]>> liveReader = RedisItemReader.dump(client);
        configureReader(info, liveReader);
        liveReader.setMode(ReaderMode.LIVE);
        RedisItemWriter<byte[], byte[], KeyValue<byte[]>> liveWriter = RedisItemWriter.dump(targetClient);
        assertEmpty(replicateLive(info, reader, writer, liveReader, liveWriter));
    }

    @Test
    void replicateDumpLiveOnly(TestInfo info) throws Exception {
        enableKeyspaceNotifications(client);
        RedisItemReader<byte[], byte[], KeyValue<byte[]>> reader = RedisItemReader.dump(client);
        configureReader(info, reader);
        reader.setMode(ReaderMode.LIVE);
        reader.setNotificationQueueCapacity(100000);
        RedisItemWriter<byte[], byte[], KeyValue<byte[]>> writer = RedisItemWriter.dump(targetClient);
        JobExecution execution = runAsync(job(info).start(flushingStep(info, reader, writer).build()).build());
        awaitOpen(reader);
        GeneratorItemReader gen = generator(100, DataType.HASH, DataType.LIST, DataType.SET, DataType.STRING, DataType.ZSET);
        generate(info, gen);
        awaitTermination(execution);
        assertEmpty(compare(info));
    }

    @Test
    void readStructLive(TestInfo info) throws Exception {
        enableKeyspaceNotifications(client);
        RedisItemReader<byte[], byte[], KeyValue<byte[]>> reader = RedisItemReader.struct(client, ByteArrayCodec.INSTANCE);
        configureReader(info, reader);
        reader.setMode(ReaderMode.LIVE);
        reader.setNotificationQueueCapacity(10000);
        reader.open(new ExecutionContext());
        awaitOpen(reader);
        int count = 123;
        GeneratorItemReader gen = generator(count, DataType.HASH, DataType.STRING);
        generate(info, gen);
        List<KeyValue<byte[]>> list = BatchUtils.readAll(reader);
        Function<byte[], String> toString = CodecUtils.toStringKeyFunction(ByteArrayCodec.INSTANCE);
        Set<String> keys = list.stream().map(KeyValue::getKey).map(toString).collect(Collectors.toSet());
        Assertions.assertEquals(commands.dbsize(), keys.size());
        reader.close();
    }

    @Test
    void replicateSetLiveOnly(TestInfo info) throws Exception {
        enableKeyspaceNotifications(client);
        String key = "myset";
        commands.sadd(key, "1", "2", "3", "4", "5");
        RedisItemReader<String, String, KeyValue<String>> reader = RedisItemReader.struct(client);
        configureReader(info, reader);
        reader.setMode(ReaderMode.LIVE);
        reader.setNotificationQueueCapacity(100);
        RedisItemWriter<String, String, KeyValue<String>> writer = RedisItemWriter.struct(targetClient);
        JobExecution execution = runAsync(job(info).start(flushingStep(info, reader, writer).build()).build());
        awaitOpen(reader);
        awaitOpen(writer);
        commands.srem(key, "5");
        awaitTermination(execution);
        assertEquals(commands.smembers(key), targetCommands.smembers(key));
    }

    @Test
    void replicateStructLive(TestInfo info) throws Exception {
        enableKeyspaceNotifications(client);
        StructItemReader<String, String> reader = RedisItemReader.struct(client);
        configureReader(info, reader);
        RedisItemWriter<String, String, KeyValue<String>> writer = RedisItemWriter.struct(targetClient);
        StructItemReader<String, String> liveReader = RedisItemReader.struct(client);
        configureReader(info, liveReader);
        liveReader.setMode(ReaderMode.LIVE);
        StructItemWriter<String, String> liveWriter = RedisItemWriter.struct(targetClient);
        assertEmpty(replicateLive(info, reader, writer, liveReader, liveWriter));
    }

}
