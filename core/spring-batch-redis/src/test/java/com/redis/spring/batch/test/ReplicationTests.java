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
import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.ValueType;
import com.redis.spring.batch.RedisItemWriter.MergePolicy;
import com.redis.spring.batch.gen.DataType;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.gen.MapOptions;
import com.redis.spring.batch.util.BatchUtils;
import com.redis.spring.batch.util.IntRange;
import com.redis.spring.batch.util.KeyComparison;
import com.redis.spring.batch.util.KeyComparison.Status;
import com.redis.spring.batch.util.KeyComparisonItemReader;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;

abstract class ReplicationTests extends AbstractTargetTestBase {

    @Test
    void writeStructsOverwrite(TestInfo info) throws Exception {
        GeneratorItemReader gen1 = new GeneratorItemReader();
        gen1.setMaxItemCount(100);
        gen1.setTypes(DataType.HASH);
        gen1.setHashOptions(hashOptions(IntRange.is(5)));
        generate(info, client, gen1);
        GeneratorItemReader gen2 = new GeneratorItemReader();
        gen2.setMaxItemCount(100);
        gen2.setTypes(DataType.HASH);
        gen2.setHashOptions(hashOptions(IntRange.is(10)));
        generate(info, targetClient, gen2);
        RedisItemReader<String, String> reader = structReader(info, client);
        RedisItemWriter<String, String> writer = structWriter(targetClient);
        writer.setMergePolicy(MergePolicy.OVERWRITE);
        run(info, reader, writer);
        assertEquals(connection.sync().hgetall("gen:1"), targetConnection.sync().hgetall("gen:1"));
    }

    private MapOptions hashOptions(IntRange fieldCount) {
        MapOptions options = new MapOptions();
        options.setFieldCount(fieldCount);
        return options;
    }

    @Test
    void writeStructsMerge(TestInfo info) throws Exception {
        GeneratorItemReader gen1 = new GeneratorItemReader();
        gen1.setMaxItemCount(100);
        gen1.setTypes(DataType.HASH);
        gen1.setHashOptions(hashOptions(IntRange.is(5)));
        generate(info, client, gen1);
        GeneratorItemReader gen2 = new GeneratorItemReader();
        gen2.setMaxItemCount(100);
        gen2.setTypes(DataType.HASH);
        gen2.setHashOptions(hashOptions(IntRange.is(10)));
        generate(info, targetClient, gen2);
        RedisItemReader<String, String> reader = structReader(info, client);
        RedisItemWriter<String, String> writer = structWriter(targetClient);
        writer.setMergePolicy(MergePolicy.MERGE);
        run(info, reader, writer);
        Map<String, String> actual = targetConnection.sync().hgetall("gen:1");
        assertEquals(10, actual.size());
    }

    @Test
    void setComparator(TestInfo info) throws Exception {
        connection.sync().sadd("set:1", "value1", "value2");
        targetConnection.sync().sadd("set:1", "value2", "value1");
        KeyComparisonItemReader reader = comparisonReader(info);
        reader.setName(name(info));
        reader.open(new ExecutionContext());
        List<KeyComparison> comparisons = BatchUtils.readAll(reader);
        reader.close();
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
        RedisItemReader<byte[], byte[]> reader = reader(info, client, ByteArrayCodec.INSTANCE);
        RedisItemWriter<byte[], byte[]> writer = new RedisItemWriter<>(targetClient, ByteArrayCodec.INSTANCE);
        run(info, reader, writer);
        Assertions.assertEquals(connection.sync().dbsize(), targetConnection.sync().dbsize());
    }

    @Test
    void liveOnlyReplication(TestInfo info) throws Exception {
        Assumptions.assumeFalse(RedisVersion.of(connection).getMajor() == 7);
        enableKeyspaceNotifications(client);
        RedisItemReader<byte[], byte[]> reader = reader(info, client, ByteArrayCodec.INSTANCE);
        reader.setNotificationQueueCapacity(100000);
        setLive(reader);
        RedisItemWriter<byte[], byte[]> writer = new RedisItemWriter<>(targetClient, ByteArrayCodec.INSTANCE);
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
    void liveDumpAndRestoreReplication(TestInfo info) throws Exception {
        Assumptions.assumeFalse(RedisVersion.of(connection).getMajor() == 7);
        enableKeyspaceNotifications(client);
        RedisItemReader<byte[], byte[]> reader = reader(info, client, ByteArrayCodec.INSTANCE);
        RedisItemWriter<byte[], byte[]> writer = new RedisItemWriter<>(targetClient, ByteArrayCodec.INSTANCE);
        RedisItemReader<byte[], byte[]> liveReader = reader(info, client, ByteArrayCodec.INSTANCE);
        RedisItemWriter<byte[], byte[]> liveWriter = new RedisItemWriter<>(targetClient, ByteArrayCodec.INSTANCE);
        Assertions.assertTrue(liveReplication(info, reader, writer, liveReader, liveWriter));
    }

    @Test
    void liveSetReplication(TestInfo info) throws Exception {
        enableKeyspaceNotifications(client);
        String key = "myset";
        connection.sync().sadd(key, "1", "2", "3", "4", "5");
        RedisItemReader<String, String> reader = reader(info, client);
        reader.setValueType(ValueType.STRUCT);
        reader.setNotificationQueueCapacity(100);
        setLive(reader);
        RedisItemWriter<String, String> writer = structWriter(targetClient);
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
        RedisItemReader<byte[], byte[]> reader = structReader(info, client, ByteArrayCodec.INSTANCE);
        RedisItemWriter<byte[], byte[]> writer = new RedisItemWriter<>(targetClient, ByteArrayCodec.INSTANCE);
        writer.setValueType(ValueType.STRUCT);
        run(info, reader, writer);
        RedisModulesCommands<String, String> sourceSync = connection.sync();
        RedisModulesCommands<String, String> targetSync = targetConnection.sync();
        assertEquals(sourceSync.pfcount(key1), targetSync.pfcount(key1));
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

    @Test
    void comparator(TestInfo info) throws Exception {
        Assumptions.assumeFalse(RedisVersion.of(connection).getMajor() == 7);
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(120);
        generate(info, gen);
        run(testInfo(info, "replicate"), reader(info, client, ByteArrayCodec.INSTANCE),
                writer(targetClient, ByteArrayCodec.INSTANCE));
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
            if (type.equalsIgnoreCase(KeyValue.STRING)) {
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
        KeyComparisonItemReader reader = comparisonReader(info);
        reader.setName(name(info));
        reader.open(new ExecutionContext());
        List<KeyComparison> comparisons = BatchUtils.readAll(reader);
        reader.close();
        long sourceCount = connection.sync().dbsize();
        assertEquals(sourceCount, comparisons.size());
        assertEquals(sourceCount, targetConnection.sync().dbsize() + deleted);
        assertEquals(typeChanges.size(), comparisons.stream().filter(c -> c.getStatus() == Status.TYPE).count());
        assertEquals(valueChanges.size(), comparisons.stream().filter(c -> c.getStatus() == Status.VALUE).count());
        assertEquals(ttlChanges.size(), comparisons.stream().filter(c -> c.getStatus() == Status.TTL).count());
        assertEquals(deleted, comparisons.stream().filter(c -> c.getStatus() == Status.MISSING).count());
    }

}
