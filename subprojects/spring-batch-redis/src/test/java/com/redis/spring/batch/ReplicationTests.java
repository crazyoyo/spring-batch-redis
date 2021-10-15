package com.redis.spring.batch;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.testcontainers.junit.jupiter.Container;

import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.DataStructureValueReader;
import com.redis.spring.batch.support.JobFactory;
import com.redis.spring.batch.support.KeyComparisonItemWriter;
import com.redis.spring.batch.support.KeyComparisonMismatchPrinter;
import com.redis.spring.batch.support.KeyComparisonResultCounter;
import com.redis.spring.batch.support.KeyValue;
import com.redis.spring.batch.support.LiveRedisItemReader;
import com.redis.spring.batch.support.ScanSizeEstimator;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import io.lettuce.core.api.sync.RedisHashCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import io.lettuce.core.api.sync.RedisSetCommands;
import lombok.extern.slf4j.Slf4j;

@SuppressWarnings("unchecked")
@Slf4j
public class ReplicationTests extends AbstractRedisTestBase {

	@Container
	private static final RedisContainer REDIS_REPLICA = new RedisContainer();

	@BeforeAll
	public static void setupReplicaContainer() {
		add(REDIS_REPLICA);
	}

	@ParameterizedTest
	@MethodSource("servers")
	public void testDataStructureReplication(RedisServer redis) throws Throwable {
		dataGenerator(redis).end(10000).build().call();
		RedisItemReader<String, DataStructure<String>> reader = dataStructureReader(redis);
		RedisItemWriter<String, String, DataStructure<String>> writer = dataStructureWriter(REDIS_REPLICA);
		jobFactory.run(name(redis, "ds-replication"), reader, writer);
		compare(redis, "ds-replication");
	}

	@ParameterizedTest
	@MethodSource("servers")
	public void testLiveDSSetReplication(RedisServer redisServer) throws Throwable {
		RedisSetCommands<String, String> sync = sync(redisServer);
		String key = "myset";
		sync.sadd(key, "1", "2", "3", "4", "5");
		RedisItemReader<String, DataStructure<String>> reader = dataStructureReader(redisServer);
		RedisItemWriter<String, String, DataStructure<String>> writer = dataStructureWriter(REDIS_REPLICA);
		TaskletStep replicationStep = jobFactory.step("ds-replication", reader, writer).build();
		LiveRedisItemReader<String, DataStructure<String>> liveReader = liveDataStructureReader(redisServer).build();
		RedisItemWriter<String, String, DataStructure<String>> liveWriter = dataStructureWriter(REDIS_REPLICA);
		TaskletStep liveReplicationStep = jobFactory
				.flushing(jobFactory.step("live-ds-set-replication", liveReader, liveWriter)).build();
		SimpleFlow replicationFlow = new FlowBuilder<SimpleFlow>("ds-set-replication-flow").start(replicationStep)
				.build();
		SimpleFlow liveReplicationFlow = new FlowBuilder<SimpleFlow>("live-ds-set-replication-flow")
				.start(liveReplicationStep).build();
		Job job = jobFactory.job(name(redisServer, "live-ds-set-replication-job"))
				.start(new FlowBuilder<SimpleFlow>("live-ds-replication-flow").split(new SimpleAsyncTaskExecutor())
						.add(replicationFlow, liveReplicationFlow).build())
				.build().build();
		JobFactory.JobExecutionWrapper execution = jobFactory.runAsync(job, new JobParameters()).awaitRunning();
		jobFactory.awaitOpen(liveReader);
		jobFactory.notificationSleep();
		sync.srem(key, "5");
		jobFactory.notificationSleep();
		execution.awaitTermination();
		Set<String> source = sync.smembers(key);
		RedisSetCommands<String, String> targetSync = sync(REDIS_REPLICA);
		Set<String> target = targetSync.smembers(key);
		Assertions.assertEquals(source, target);
	}

	@ParameterizedTest
	@MethodSource("servers")
	public void testReplication(RedisServer redisServer) throws Throwable {
		dataGenerator(redisServer).end(10000).build().call();
		RedisItemReader<String, KeyValue<String, byte[]>> reader = keyDumpReader(redisServer);
		RedisItemWriter<String, String, KeyValue<String, byte[]>> writer = keyDumpWriter(REDIS_REPLICA);
		jobFactory.run(name(redisServer, "replication"), reader, writer);
		compare(redisServer, "replication");
	}

	@ParameterizedTest
	@MethodSource("servers")
	public void testLiveReplication(RedisServer redisServer) throws Throwable {
		dataGenerator(redisServer).end(10000).build().call();
		RedisItemReader<String, KeyValue<String, byte[]>> reader = keyDumpReader(redisServer);
		RedisItemWriter<String, String, KeyValue<String, byte[]>> writer = keyDumpWriter(REDIS_REPLICA);
		TaskletStep replicationStep = jobFactory.step("replication", reader, writer).build();
		LiveRedisItemReader<String, KeyValue<String, byte[]>> liveReader = liveKeyDumpReader(redisServer);
		RedisItemWriter<String, String, KeyValue<String, byte[]>> liveWriter = keyDumpWriter(REDIS_REPLICA);
		TaskletStep liveReplicationStep = jobFactory
				.flushing(jobFactory.step("live-replication", liveReader, liveWriter)).build();
		SimpleFlow replicationFlow = new FlowBuilder<SimpleFlow>("replication-flow").start(replicationStep).build();
		SimpleFlow liveReplicationFlow = new FlowBuilder<SimpleFlow>("live-replication-flow").start(liveReplicationStep)
				.build();
		Job job = jobFactory.job(name(redisServer, "live-replication-job"))
				.start(new FlowBuilder<SimpleFlow>("live-replication-flow").split(new SimpleAsyncTaskExecutor())
						.add(replicationFlow, liveReplicationFlow).build())
				.build().build();
		JobFactory.JobExecutionWrapper execution = jobFactory.runAsync(job, new JobParameters()).awaitRunning();
		dataGenerator(redisServer).end(123).build().call();
		execution.awaitTermination();
		compare(redisServer, "live-replication");
	}

	@ParameterizedTest
	@MethodSource("servers")
	public void testLiveDSReplication(RedisServer redisServer) throws Throwable {
		dataGenerator(redisServer).end(10000).build().call();
		RedisItemReader<String, DataStructure<String>> reader = dataStructureReader(redisServer);
		RedisItemWriter<String, String, DataStructure<String>> writer = dataStructureWriter(REDIS_REPLICA);
		TaskletStep replicationStep = jobFactory.step("ds-replication", reader, writer).build();
		LiveRedisItemReader<String, DataStructure<String>> liveReader = liveDataStructureReader(redisServer).build();
		RedisItemWriter<String, String, DataStructure<String>> liveWriter = dataStructureWriter(REDIS_REPLICA);
		TaskletStep liveReplicationStep = jobFactory
				.flushing(jobFactory.step("live-ds-replication", liveReader, liveWriter)).build();
		SimpleFlow replicationFlow = new FlowBuilder<SimpleFlow>("ds-replication-flow").start(replicationStep).build();
		SimpleFlow liveReplicationFlow = new FlowBuilder<SimpleFlow>("live-ds-replication-flow")
				.start(liveReplicationStep).build();
		Job job = jobFactory.job(name(redisServer, "live-ds-replication-job"))
				.start(new FlowBuilder<SimpleFlow>("live-ds-replication-flow").split(new SimpleAsyncTaskExecutor())
						.add(replicationFlow, liveReplicationFlow).build())
				.build().build();
		JobFactory.JobExecutionWrapper execution = jobFactory.runAsync(job, new JobParameters()).awaitRunning();
		dataGenerator(redisServer).end(123).build().call();
		execution.awaitTermination();
		log.info("Comparing");
		compare(redisServer, "live-ds-replication");
	}

	private void compare(RedisServer server, String name) throws Throwable {
		RedisServerCommands<String, String> sourceSync = sync(server);
		RedisServerCommands<String, String> targetSync = sync(REDIS_REPLICA);
		Assertions.assertEquals(sourceSync.dbsize(), targetSync.dbsize());
		RedisItemReader<String, DataStructure<String>> left = dataStructureReader(server);
		DataStructureValueReader<String, String> right = dataStructureValueReader(REDIS_REPLICA);
		KeyComparisonResultCounter<String> results = new KeyComparisonResultCounter<>();
		KeyComparisonItemWriter<String> writer = KeyComparisonItemWriter.valueReader(right).resultHandler(results)
				.resultHandler(new KeyComparisonMismatchPrinter<>()).build();
		jobFactory.run(name(server, name + "-compare"), left, writer);
		Assertions.assertEquals(sourceSync.dbsize(), results.get(KeyComparisonItemWriter.Status.OK));
		Assertions.assertTrue(results.isOK());
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
		RedisItemReader<String, DataStructure<String>> left = dataStructureReader(server);
		DataStructureValueReader<String, String> right = dataStructureValueReader(REDIS_REPLICA);
		KeyComparisonResultCounter<String> counter = new KeyComparisonResultCounter<String>();
		KeyComparisonItemWriter<String> writer = KeyComparisonItemWriter.valueReader(right).resultHandler(counter)
				.resultHandler(new KeyComparisonMismatchPrinter<>()).build();
		jobFactory.run(name(server, "test-comparison-writer-compare"), left, writer);
		Assertions.assertTrue(counter.get(KeyComparisonItemWriter.Status.OK) > 0);
		Assertions.assertEquals(1, counter.get(KeyComparisonItemWriter.Status.MISSING));
	}

	@ParameterizedTest
	@MethodSource("servers")
	public void testScanSizeEstimator(RedisServer server) throws Exception {
		dataGenerator(server).end(12345).dataTypes(DataStructure.HASH).build().call();
		ScanSizeEstimator estimator = sizeEstimator(server);
		long matchSize = estimator
				.estimate(ScanSizeEstimator.EstimateOptions.builder().sampleSize(100).match("hash:*").build());
		RedisKeyCommands<String, String> sync = sync(server);
		long hashCount = sync.keys("hash:*").size();
		Assertions.assertEquals(hashCount, matchSize, (double) hashCount / 10);
		long typeSize = estimator.estimate(
				ScanSizeEstimator.EstimateOptions.builder().sampleSize(1000).type(DataStructure.HASH).build());
		Assertions.assertEquals(hashCount, typeSize, (double) hashCount / 10);
	}

	private ScanSizeEstimator sizeEstimator(RedisServer server) {
		return ScanSizeEstimator.client(client(server)).build();
	}

}
