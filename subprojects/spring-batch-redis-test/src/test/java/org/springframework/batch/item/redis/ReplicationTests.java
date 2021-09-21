package org.springframework.batch.item.redis;

import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import io.lettuce.core.api.sync.RedisHashCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import io.lettuce.core.api.sync.RedisSetCommands;
import lombok.extern.slf4j.Slf4j;
import org.javers.core.Javers;
import org.javers.core.JaversBuilder;
import org.javers.core.diff.ListCompareAlgorithm;
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
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("unchecked")
@Slf4j
public class ReplicationTests extends AbstractRedisTestBase {

    @Container
    private static final RedisContainer REDIS_REPLICA = new RedisContainer();

    @BeforeAll
    public static void setupReplicaContainer() {
        add(REDIS_REPLICA);
    }

    @ParameterizedTest(name = "{displayName} - {index}: {0}")
    @MethodSource("servers")
    public void testDataStructureReplication(RedisServer redis) throws Exception {
        dataGenerator(redis).end(10000).build().call();
        KeyValueItemReader<DataStructure> reader = dataStructureReader(redis);
        DataStructureItemWriter writer = dataStructureWriter(REDIS_REPLICA);
        execute(name(redis, "ds-replication"), reader, writer);
        compare(redis, "ds-replication");
    }

    @ParameterizedTest(name = "{displayName} - {index}: {0}")
    @MethodSource("servers")
    public void testLiveDSSetReplication(RedisServer redisServer) throws Exception {
        RedisSetCommands<String, String> sync = sync(redisServer);
        String key = "myset";
        sync.sadd(key, "1", "2", "3", "4", "5");
        KeyValueItemReader<DataStructure> reader = dataStructureReader(redisServer);
        reader.setName("ds-set-reader");
        DataStructureItemWriter writer = dataStructureWriter(REDIS_REPLICA);
        writer.setName("ds-set-writer");
        TaskletStep replicationStep = step("ds-replication", reader, writer).build();
        LiveKeyValueItemReader<DataStructure> liveReader = liveDataStructureReader(redisServer).idleTimeout(Duration.ofMillis(1000)).build();
        liveReader.setName("live-ds-reader");
        DataStructureItemWriter liveWriter = dataStructureWriter(REDIS_REPLICA);
        liveWriter.setName("live-ds-set-writer");
        TaskletStep liveReplicationStep = flushing(step("live-ds-set-replication", liveReader, liveWriter)).idleTimeout(Duration.ofMillis(1000)).build();
        SimpleFlow replicationFlow = new FlowBuilder<SimpleFlow>("ds-set-replication-flow").start(replicationStep).build();
        SimpleFlow liveReplicationFlow = new FlowBuilder<SimpleFlow>("live-ds-set-replication-flow").start(liveReplicationStep).build();
        Job job = jobs.get(name(redisServer, "live-ds-set-replication-job")).start(new FlowBuilder<SimpleFlow>("live-ds-replication-flow").split(new SimpleAsyncTaskExecutor()).add(replicationFlow, liveReplicationFlow).build()).build().build();
        JobExecution execution = asyncJobLauncher.run(job, new JobParameters());
        awaitRunning(execution);
        Thread.sleep(800);
        sync.srem(key, "5");
        awaitJobTermination(execution);
        Set<String> source = sync.smembers(key);
        RedisSetCommands<String, String> targetSync = sync(REDIS_REPLICA);
        Set<String> target = targetSync.smembers(key);
        Assertions.assertEquals(source, target);
    }

    @ParameterizedTest(name = "{displayName} - {index}: {0}")
    @MethodSource("servers")
    public void testReplication(RedisServer redisServer) throws Exception {
        dataGenerator(redisServer).end(10000).build().call();
        KeyValueItemReader<KeyValue<byte[]>> reader = keyDumpReader(redisServer);
        OperationItemWriter<String, String, KeyValue<byte[]>> writer = keyDumpWriter(REDIS_REPLICA);
        execute(name(redisServer, "replication"), reader, writer);
        compare(redisServer, "replication");
    }

    @ParameterizedTest(name = "{displayName} - {index}: {0}")
    @MethodSource("servers")
    public void testLiveReplication(RedisServer redisServer) throws Exception {
        dataGenerator(redisServer).end(10000).build().call();
        KeyValueItemReader<KeyValue<byte[]>> reader = keyDumpReader(redisServer);
        reader.setName("reader");
        OperationItemWriter<String, String, KeyValue<byte[]>> writer = keyDumpWriter(REDIS_REPLICA);
        writer.setName("writer");
        TaskletStep replicationStep = step("replication", reader, writer).build();
        LiveKeyValueItemReader<KeyValue<byte[]>> liveReader = liveKeyDumpReader(redisServer).build();
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

    @ParameterizedTest(name = "{displayName} - {index}: {0}")
    @MethodSource("servers")
    public void testLiveDSReplication(RedisServer redisServer) throws Exception {
        dataGenerator(redisServer).exclude(DataStructure.STREAM).end(10000).build().call();
        KeyValueItemReader<DataStructure> reader = dataStructureReader(redisServer);
        reader.setName("ds-reader");
        DataStructureItemWriter writer = dataStructureWriter(REDIS_REPLICA);
        writer.setName("ds-writer");
        TaskletStep replicationStep = step("ds-replication", reader, writer).build();
        LiveKeyValueItemReader<DataStructure> liveReader = liveDataStructureReader(redisServer).build();
        liveReader.setName("live-ds-reader");
        DataStructureItemWriter liveWriter = dataStructureWriter(REDIS_REPLICA);
        liveWriter.setName("live-ds-writer");
        TaskletStep liveReplicationStep = flushing(step("live-ds-replication", liveReader, liveWriter)).build();
        SimpleFlow replicationFlow = new FlowBuilder<SimpleFlow>("ds-replication-flow").start(replicationStep).build();
        SimpleFlow liveReplicationFlow = new FlowBuilder<SimpleFlow>("live-ds-replication-flow").start(liveReplicationStep).build();
        Job job = jobs.get(name(redisServer, "live-ds-replication-job")).start(new FlowBuilder<SimpleFlow>("live-ds-replication-flow").split(new SimpleAsyncTaskExecutor()).add(replicationFlow, liveReplicationFlow).build()).build().build();
        JobExecution execution = asyncJobLauncher.run(job, new JobParameters());
        awaitRunning(execution);
        dataGenerator(redisServer).exclude(DataStructure.STREAM).end(123).build().call();
        awaitJobTermination(execution);
        compare(redisServer, "live-ds-replication");
    }

    private void compare(RedisServer server, String name) throws Exception {
        RedisServerCommands<String, String> sourceSync = sync(server);
        RedisServerCommands<String, String> targetSync = sync(REDIS_REPLICA);
        Assertions.assertEquals(sourceSync.dbsize(), targetSync.dbsize());
        KeyValueItemReader<DataStructure> left = dataStructureReader(server);
        DataStructureValueReader right = dataStructureValueReader(REDIS_REPLICA);
        KeyComparisonResultCounter results = new KeyComparisonResultCounter();
        KeyComparisonItemWriter writer = KeyComparisonItemWriter.valueReader(right).resultHandler(results).resultHandler(new KeyComparisonMismatchPrinter()).ttlTolerance(Duration.ofMillis(500)).build();
        execute(name(server, name + "-compare"), left, writer);
        Assertions.assertEquals(sourceSync.dbsize(), results.get(KeyComparisonItemWriter.Status.OK));
        Assertions.assertTrue(results.isOK());
    }

    private static class KeyComparisonMismatchPrinter implements KeyComparisonItemWriter.KeyComparisonResultHandler {

        Javers javers = JaversBuilder.javers()
                .withListCompareAlgorithm(ListCompareAlgorithm.LEVENSHTEIN_DISTANCE)
                .build();

        @Override
        public void accept(DataStructure source, DataStructure target, KeyComparisonItemWriter.Status status) {
            switch (status) {
                case SOURCE:
                    log.warn("Missing key '{}'", source.getKey());
                    break;
                case TARGET:
                    log.warn("Extraneous key '{}'", target.getKey());
                    break;
                case TTL:
                    log.warn("TTL mismatch for key '{}': {} <> {}", source.getKey(), source.getAbsoluteTTL(), target.getAbsoluteTTL());
                    break;
                case TYPE:
                    log.warn("Type mismatch for key '{}': {} <> {}", source.getKey(), source.getType(), target.getType());
                    break;
                case VALUE:
                    switch (source.getType()) {
                        case DataStructure.SET:
                        case DataStructure.LIST:
                            diffCollections(source, target, String.class);
                            break;
                        case DataStructure.ZSET:
                            diffCollections(source, target, ScoredValue.class);
                            break;
                        case DataStructure.STREAM:
                            diffCollections(source, target, StreamMessage.class);
                            break;
                        default:
                            log.warn("Value mismatch for {} '{}': {}", source.getType(), source.getKey(), javers.compare(source, target).prettyPrint());
                            break;
                    }
                    break;
            }
        }

        private <T> void diffCollections(DataStructure source, DataStructure target, Class<T> itemClass) {
            Collection<T> sourceValue = (Collection<T>) source.getValue();
            Collection<T> targetValue = (Collection<T>) target.getValue();
            if (Math.abs(sourceValue.size() - targetValue.size()) > 5) {
                log.warn("Size mismatch for {} '{}': {} <> {}", source.getType(), source.getKey(), sourceValue.size(), targetValue.size());
            } else {
                log.warn("Value mismatch for {} '{}'", source.getType(), source.getKey());
                log.info("{}", javers.compareCollections(sourceValue, targetValue, itemClass).prettyPrint());
            }
        }

    }

    @ParameterizedTest(name = "{displayName} - {index}: {0}")
    @MethodSource("servers")
    public void testComparisonWriter(RedisServer server) throws Exception {
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
        KeyComparisonItemWriter writer = KeyComparisonItemWriter.valueReader(right).resultHandler(counter).resultHandler(new KeyComparisonMismatchPrinter()).ttlTolerance(Duration.ofMillis(500)).build();
        execute(name(server, "test-comparison-writer-compare"), left, writer);
        Assertions.assertTrue(counter.get(KeyComparisonItemWriter.Status.OK) > 0);
        Assertions.assertEquals(1, counter.get(KeyComparisonItemWriter.Status.SOURCE));
    }

    @ParameterizedTest(name = "{displayName} - {index}: {0}")
    @MethodSource("servers")
    public void testScanSizeEstimator(RedisServer server) throws Exception {
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
