package org.springframework.batch.item.redis;

import com.redislabs.testcontainers.RedisContainer;
import com.redislabs.testcontainers.RedisServer;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import io.lettuce.core.api.sync.RedisHashCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.DataStructureValueReader;
import org.springframework.batch.item.redis.support.KeyComparisonItemWriter;
import org.springframework.batch.item.redis.support.KeyComparisonResultCounter;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.KeyValueItemReader;
import org.springframework.batch.item.redis.support.LiveKeyValueItemReader;
import org.springframework.batch.item.redis.support.ScanSizeEstimator;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.testcontainers.junit.jupiter.Container;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("unchecked")
@Slf4j
public class ReplicationTests extends TestBase {

    @Container
    private static final RedisContainer REDIS_REPLICA = new RedisContainer();

    @BeforeAll
    public static void setupReplicaContainer() {
        add(REDIS_REPLICA);
    }

    @ParameterizedTest
    @MethodSource("servers")
    public void testDataStructureReplication(RedisServer server) throws Throwable {
        dataGenerator(server).end(10000).build().call();
        KeyValueItemReader<DataStructure> reader = dataStructureReader(server);
        DataStructureItemWriter<DataStructure> writer = dataStructureWriter(REDIS_REPLICA);
        execute(server, "ds-replication", reader, writer);
        compare(server, "ds-replication");
    }

    @ParameterizedTest
    @MethodSource("servers")
    public void testReplication(RedisServer redisServer) throws Throwable {
        dataGenerator(redisServer).end(10000).build().call();
        KeyValueItemReader<KeyValue<byte[]>> reader = keyDumpReader(redisServer);
        OperationItemWriter<String, String, KeyValue<byte[]>> writer = keyDumpWriter(REDIS_REPLICA);
        execute(redisServer, "replication", reader, writer);
        compare(redisServer, "replication");
    }

    @ParameterizedTest
    @MethodSource("servers")
    public void testLiveReplication(RedisServer redisServer) throws Throwable {
        dataGenerator(redisServer).end(10000).build().call();
        KeyValueItemReader<KeyValue<byte[]>> reader = keyDumpReader(redisServer);
        reader.setName("reader");
        OperationItemWriter<String, String, KeyValue<byte[]>> writer = keyDumpWriter(REDIS_REPLICA);
        writer.setName("writer");
        TaskletStep replicationStep = step("replication", reader, writer).build();
        LiveKeyValueItemReader<KeyValue<byte[]>> liveReader = liveKeyDumpReader(redisServer);
        liveReader.setName("live-reader");
        OperationItemWriter<String, String, KeyValue<byte[]>> liveWriter = keyDumpWriter(REDIS_REPLICA);
        liveWriter.setName("live-writer");
        TaskletStep liveReplicationStep = flushing(step("live-replication", liveReader, liveWriter)).build();
        SimpleFlow replicationFlow = new FlowBuilder<SimpleFlow>("replication-flow").start(replicationStep).build();
        SimpleFlow liveReplicationFlow = new FlowBuilder<SimpleFlow>("live-replication-flow").start(liveReplicationStep).build();
        Job job = jobs.get(name(redisServer, "live-replication-job")).start(new FlowBuilder<SimpleFlow>("live-replication-flow").split(new SimpleAsyncTaskExecutor()).add(replicationFlow, liveReplicationFlow).build()).build().build();
        JobExecution execution = asyncJobLauncher.run(job, new JobParameters());
        awaitRunning(execution);
        dataGenerator(redisServer).end(123).build().call();
        awaitJobTermination(execution);
        compare(redisServer, "live-replication");
    }

    private void compare(RedisServer server, String name) throws Throwable {
        RedisServerCommands<String, String> sourceSync = sync(server);
        RedisServerCommands<String, String> targetSync = sync(REDIS_REPLICA);
        Assertions.assertEquals(sourceSync.dbsize(), targetSync.dbsize());
        KeyValueItemReader<DataStructure> left = dataStructureReader(server);
        DataStructureValueReader right = dataStructureValueReader(REDIS_REPLICA);
        KeyComparisonResultCounter counter = new KeyComparisonResultCounter();
        KeyComparisonItemWriter writer = KeyComparisonItemWriter.valueReader(right).resultHandler(counter).ttlTolerance(Duration.ofMillis(500)).build();
        execute(server, name + "-compare", left, writer);
        Assertions.assertEquals(sourceSync.dbsize(), counter.get(KeyComparisonItemWriter.Result.OK));
        Assertions.assertTrue(counter.isOK());
    }

    @ParameterizedTest
    @MethodSource("servers")
    public void testComparisonWriter(RedisServer server) throws Throwable {
        BaseRedisAsyncCommands<String, String> source = async(server);
        source.setAutoFlushCommands(false);
        BaseRedisAsyncCommands<String, String> target = async(REDIS_REPLICA);
        target.setAutoFlushCommands(false);
        List<RedisFuture<?>> sourceFutures = new ArrayList<>();
        List<RedisFuture<?>> targetFutures = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            sourceFutures.add(((RedisStringAsyncCommands<String, String>) source).set("key" + index, "value" + index));
            targetFutures.add(((RedisStringAsyncCommands<String, String>) target).set("key" + index, "value" + index));
        }
        source.flushCommands();
        LettuceFutures.awaitAll(10, TimeUnit.SECONDS, sourceFutures.toArray(new RedisFuture[0]));
        target.flushCommands();
        LettuceFutures.awaitAll(10, TimeUnit.SECONDS, targetFutures.toArray(new RedisFuture[0]));
        source.setAutoFlushCommands(true);
        target.setAutoFlushCommands(true);
        RedisHashCommands<String, String> sourceSync = sync(server);
        sourceSync.hset("zehash", "zefield", "zevalue");
        KeyValueItemReader<DataStructure> left = dataStructureReader(server);
        DataStructureValueReader right = dataStructureValueReader(REDIS_REPLICA);
        KeyComparisonResultCounter counter = new KeyComparisonResultCounter();
        KeyComparisonItemWriter writer = KeyComparisonItemWriter.valueReader(right).resultHandler(counter).ttlTolerance(Duration.ofMillis(500)).build();
        execute(server, "test-comparison-writer-compare", left, writer);
        Assertions.assertFalse(counter.isOK());
    }

    @ParameterizedTest
    @MethodSource("servers")
    public void testScanSizeEstimator(RedisServer server) throws Throwable {
        dataGenerator(server).end(12345).dataTypes(DataStructure.HASH).build().call();
        ScanSizeEstimator estimator = sizeEstimator(server);
        long matchSize = estimator.estimate(ScanSizeEstimator.EstimateOptions.builder().sampleSize(100).match("hash:*").build());
        RedisKeyCommands<String, String> sync = sync(server);
        long hashCount = sync.keys("hash:*").size();
        Assertions.assertEquals(hashCount, matchSize, (double) hashCount / 10);
        long typeSize = estimator.estimate(ScanSizeEstimator.EstimateOptions.builder().sampleSize(1000).type(DataStructure.HASH).build());
        Assertions.assertEquals(hashCount, typeSize, (double) hashCount / 10);
    }

    private ScanSizeEstimator sizeEstimator(RedisServer server) {
        if (server.isCluster()) {
            return ScanSizeEstimator.client(redisClusterClient(server)).build();
        }
        return ScanSizeEstimator.client(redisClient(server)).build();
    }

}
