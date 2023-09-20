package com.redis.spring.batch.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.item.ExecutionContext;

import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.Dump;
import com.redis.spring.batch.common.KeyComparison;
import com.redis.spring.batch.common.KeyComparison.Status;
import com.redis.spring.batch.common.KeyComparisonItemReader;
import com.redis.spring.batch.common.Range;
import com.redis.spring.batch.common.Struct;
import com.redis.spring.batch.common.Struct.Type;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.gen.MapOptions;
import com.redis.spring.batch.util.BatchUtils;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;

abstract class ReplicationTests extends AbstractTargetTestBase {

    @Test
    void writeStructOverwrite(TestInfo info) throws Exception {
        GeneratorItemReader gen1 = new GeneratorItemReader();
        gen1.setMaxItemCount(100);
        gen1.setTypes(Type.HASH);
        gen1.setHashOptions(hashOptions(Range.of(5)));
        generate(info, client, gen1);
        GeneratorItemReader gen2 = new GeneratorItemReader();
        gen2.setMaxItemCount(100);
        gen2.setTypes(Type.HASH);
        gen2.setHashOptions(hashOptions(Range.of(10)));
        generate(info, targetClient, gen2);
        RedisItemReader<String, String, Struct<String>> reader = structReader(info, client);
        RedisItemWriter<String, String, Struct<String>> writer = structWriter(targetClient);
        run(info, reader, writer);
        awaitClosed(reader);
        awaitClosed(writer);
        assertEquals(connection.sync().hgetall("gen:1"), targetConnection.sync().hgetall("gen:1"));
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
        gen1.setTypes(Type.HASH);
        gen1.setHashOptions(hashOptions(Range.of(5)));
        generate(info, client, gen1);
        GeneratorItemReader gen2 = new GeneratorItemReader();
        gen2.setMaxItemCount(100);
        gen2.setTypes(Type.HASH);
        gen2.setHashOptions(hashOptions(Range.of(10)));
        generate(info, targetClient, gen2);
        RedisItemReader<String, String, Struct<String>> reader = structReader(info, client);
        RedisItemWriter<String, String, Struct<String>> writer = RedisItemWriter.structMerge(targetClient, StringCodec.UTF8);
        run(info, reader, writer);
        awaitClosed(reader);
        awaitClosed(writer);
        Map<String, String> actual = targetConnection.sync().hgetall("gen:1");
        assertEquals(10, actual.size());
    }

    @Test
    void compareSet(TestInfo info) throws Exception {
        connection.sync().sadd("set:1", "value1", "value2");
        targetConnection.sync().sadd("set:1", "value2", "value1");
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
    void byteArrayCodec(TestInfo info) throws Exception {
        Assumptions.assumeFalse(RedisVersion.of(connection).getMajor() == 7);
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
        RedisItemReader<byte[], byte[], Struct<byte[]>> reader = structReader(info, client, ByteArrayCodec.INSTANCE);
        RedisItemWriter<byte[], byte[], Struct<byte[]>> writer = structWriter(targetClient, ByteArrayCodec.INSTANCE);
        run(info, reader, writer);
        awaitClosed(reader);
        awaitClosed(writer);
        Assertions.assertEquals(connection.sync().dbsize(), targetConnection.sync().dbsize());
    }

    @Test
    void readWriteLive(TestInfo info) throws Exception {
        Assumptions.assumeFalse(RedisVersion.of(connection).getMajor() == 7);
        enableKeyspaceNotifications(client);
        RedisItemReader<byte[], byte[], Dump<byte[]>> reader = liveDumpReader(info, client, ByteArrayCodec.INSTANCE);
        reader.setNotificationQueueCapacity(100000);
        RedisItemWriter<byte[], byte[], Dump<byte[]>> writer = dumpWriter(targetClient, ByteArrayCodec.INSTANCE);
        JobExecution execution = runAsync(job(info).start(flushingStep(info, reader, writer).build()).build());
        awaitOpen(reader);
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(100);
        gen.setTypes(Type.HASH, Type.LIST, Type.SET, Type.STRING, Type.ZSET);
        generate(info, gen);
        awaitTermination(execution);
        Assertions.assertTrue(compare(info));
    }

    @Test
    void replicateDumpLive(TestInfo info) throws Exception {
        Assumptions.assumeFalse(RedisVersion.of(connection).getMajor() == 7);
        enableKeyspaceNotifications(client);
        RedisItemReader<byte[], byte[], Dump<byte[]>> reader = dumpReader(info, client, ByteArrayCodec.INSTANCE);
        RedisItemWriter<byte[], byte[], Dump<byte[]>> writer = dumpWriter(targetClient, ByteArrayCodec.INSTANCE);
        RedisItemReader<byte[], byte[], Dump<byte[]>> liveReader = liveDumpReader(info, client, ByteArrayCodec.INSTANCE);
        RedisItemWriter<byte[], byte[], Dump<byte[]>> liveWriter = dumpWriter(targetClient, ByteArrayCodec.INSTANCE);
        Assertions.assertTrue(replicateLive(info, reader, writer, liveReader, liveWriter));
    }

    @Test
    void replicateSetLive(TestInfo info) throws Exception {
        enableKeyspaceNotifications(client);
        String key = "myset";
        connection.sync().sadd(key, "1", "2", "3", "4", "5");
        RedisItemReader<String, String, Struct<String>> reader = liveStructReader(info, client);
        reader.setNotificationQueueCapacity(100);
        RedisItemWriter<String, String, Struct<String>> writer = structWriter(targetClient);
        JobExecution execution = runAsync(job(info).start(flushingStep(info, reader, writer).build()).build());
        awaitOpen(reader);
        awaitOpen(writer);
        connection.sync().srem(key, "5");
        awaitTermination(execution);
        assertEquals(connection.sync().smembers(key), targetConnection.sync().smembers(key));
    }

    @Test
    void replicateHLL(TestInfo info) throws Exception {
        String key1 = "hll:1";
        connection.sync().pfadd(key1, "member:1", "member:2");
        String key2 = "hll:2";
        connection.sync().pfadd(key2, "member:1", "member:2", "member:3");
        RedisItemReader<byte[], byte[], Struct<byte[]>> reader = structReader(info, client, ByteArrayCodec.INSTANCE);
        RedisItemWriter<byte[], byte[], Struct<byte[]>> writer = structWriter(targetClient, ByteArrayCodec.INSTANCE);
        run(info, reader, writer);
        awaitClosed(reader);
        awaitClosed(writer);
        RedisModulesCommands<String, String> sourceSync = connection.sync();
        RedisModulesCommands<String, String> targetSync = targetConnection.sync();
        assertEquals(sourceSync.pfcount(key1), targetSync.pfcount(key1));
    }

    @Test
    void compareStatus(TestInfo info) throws Exception {
        Assumptions.assumeFalse(RedisVersion.of(connection).getMajor() == 7);
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(120);
        generate(info, gen);
        RedisItemReader<byte[], byte[], Dump<byte[]>> reader = dumpReader(info, client, ByteArrayCodec.INSTANCE);
        RedisItemWriter<byte[], byte[], Dump<byte[]>> writer = dumpWriter(targetClient, ByteArrayCodec.INSTANCE);
        run(testInfo(info, "replicate"), reader, writer);
        awaitClosed(reader);
        awaitClosed(writer);
        long deleted = 0;
        for (int index = 0; index < 13; index++) {
            deleted += targetConnection.sync().del(targetConnection.sync().randomkey());
        }
        Set<String> ttlChanges = new HashSet<>();
        for (int index = 0; index < 23; index++) {
            String key = targetConnection.sync().randomkey();
            if (key == null) {
                continue;
            }
            long ttl = targetConnection.sync().ttl(key) + 12345;
            if (targetConnection.sync().expire(key, ttl)) {
                ttlChanges.add(key);
            }
        }
        Set<String> typeChanges = new HashSet<>();
        Set<String> valueChanges = new HashSet<>();
        for (int index = 0; index < 17; index++) {
            String key = targetConnection.sync().randomkey();
            String type = targetConnection.sync().type(key);
            if (type.equalsIgnoreCase(Type.STRING.getString())) {
                if (!typeChanges.contains(key)) {
                    valueChanges.add(key);
                }
                ttlChanges.remove(key);
            } else {
                typeChanges.add(key);
                valueChanges.remove(key);
                ttlChanges.remove(key);
            }
            targetConnection.sync().set(key, "blah");
        }
        KeyComparisonItemReader comparator = comparisonReader(info);
        comparator.setName(name(info));
        comparator.open(new ExecutionContext());
        List<KeyComparison> comparisons = BatchUtils.readAll(comparator);
        comparator.close();
        long sourceCount = connection.sync().dbsize();
        assertEquals(sourceCount, comparisons.size());
        assertEquals(sourceCount, targetConnection.sync().dbsize() + deleted);
        assertEquals(typeChanges.size(), comparisons.stream().filter(c -> c.getStatus() == Status.TYPE).count());
        assertEquals(valueChanges.size(), comparisons.stream().filter(c -> c.getStatus() == Status.VALUE).count());
        assertEquals(ttlChanges.size(), comparisons.stream().filter(c -> c.getStatus() == Status.TTL).count());
        assertEquals(deleted, comparisons.stream().filter(c -> c.getStatus() == Status.MISSING).count());
    }

}
