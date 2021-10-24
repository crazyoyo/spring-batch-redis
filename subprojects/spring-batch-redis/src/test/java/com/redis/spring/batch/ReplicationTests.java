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
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.testcontainers.junit.jupiter.Container;

import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.DataStructureValueReader;
import com.redis.spring.batch.support.KeyComparisonItemWriter;
import com.redis.spring.batch.support.KeyComparisonMismatchPrinter;
import com.redis.spring.batch.support.KeyComparisonResultCounter;
import com.redis.spring.batch.support.KeyValue;
import com.redis.spring.batch.support.LiveRedisItemReader;
import com.redis.spring.batch.support.ScanSizeEstimator;
import com.redis.spring.batch.support.ScanSizeEstimator.EstimateOptions;
import com.redis.spring.batch.support.generator.Generator.DataType;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import io.lettuce.core.api.sync.RedisHashCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import io.lettuce.core.api.sync.RedisSetCommands;

@SuppressWarnings("unchecked")
public class ReplicationTests extends AbstractRedisTestBase {

	@Container
	private static final RedisContainer REDIS_REPLICA = new RedisContainer();

	@BeforeAll
	public static void setupReplicaContainer() {
		add(REDIS_REPLICA);
	}

	@ParameterizedTest
	@MethodSource("servers")
	public void testDataStructureReplication(RedisServer redis) throws Exception {
		String name = "ds-replication";
		dataGenerator(name, redis).to(100).build().call();
		RedisItemReader<String, DataStructure<String>> reader = dataStructureReader(redis);
		reader.setName(name(redis, name + "-reader"));
		run(redis, name, reader, dataStructureWriter(REDIS_REPLICA));
		compare(redis, name);
	}

	@ParameterizedTest
	@MethodSource("servers")
	public void testLiveDSSetReplication(RedisServer redisServer) throws Exception {
		String name = "live-ds-set-replication";
		RedisSetCommands<String, String> sync = sync(redisServer);
		String key = "myset";
		sync.sadd(key, "1", "2", "3", "4", "5");
		RedisItemReader<String, DataStructure<String>> reader = dataStructureReader(redisServer);
		RedisItemWriter<String, String, DataStructure<String>> writer = dataStructureWriter(REDIS_REPLICA);
		TaskletStep replicationStep = step("scan-" + name, reader, writer).build();
		LiveRedisItemReader<String, DataStructure<String>> liveReader = liveDataStructureReader(redisServer);
		RedisItemWriter<String, String, DataStructure<String>> liveWriter = dataStructureWriter(REDIS_REPLICA);
		TaskletStep liveReplicationStep = jobFactory.flushing(step("live-" + name, liveReader, liveWriter)).build();
		SimpleFlow replicationFlow = jobFactory.flow("scan-" + name).start(replicationStep).build();
		SimpleFlow liveReplicationFlow = jobFactory.flow("live-" + name).start(liveReplicationStep).build();
		Job job = jobFactory.job(name(redisServer, name)).start(jobFactory.flow(name)
				.split(new SimpleAsyncTaskExecutor()).add(replicationFlow, liveReplicationFlow).build()).build()
				.build();
		JobExecution execution = jobFactory.runAsync(job, new JobParameters());
		jobFactory.awaitRunning(execution);
		jobFactory.awaitOpen(liveReader);
		jobFactory.notificationSleep();
		sync.srem(key, "5");
		jobFactory.notificationSleep();
		jobFactory.awaitTermination(execution);
		Set<String> source = sync.smembers(key);
		RedisSetCommands<String, String> targetSync = sync(REDIS_REPLICA);
		Set<String> target = targetSync.smembers(key);
		Assertions.assertEquals(source, target);
	}

	@ParameterizedTest
	@MethodSource("servers")
	public void testReplication(RedisServer server) throws Exception {
		String name = "replication";
		dataGenerator(name, server).to(100).build().call();
		RedisItemReader<String, KeyValue<String, byte[]>> reader = keyDumpReader(server);
		reader.setName(name(server, name + "-reader"));
		RedisItemWriter<String, String, KeyValue<String, byte[]>> writer = keyDumpWriter(REDIS_REPLICA);
		run(server, name, reader, writer);
		compare(server, name);
	}

	@ParameterizedTest
	@MethodSource("servers")
	public void testLiveReplication(RedisServer server) throws Exception {
		RedisItemReader<String, KeyValue<String, byte[]>> reader = keyDumpReader(server);
		ItemWriter<KeyValue<String, byte[]>> writer = keyDumpWriter(REDIS_REPLICA);
		LiveRedisItemReader<String, KeyValue<String, byte[]>> liveReader = liveKeyDumpReader(server);
		ItemWriter<KeyValue<String, byte[]>> liveWriter = keyDumpWriter(REDIS_REPLICA);
		replicate("live-replication", server, reader, writer, liveReader, liveWriter);
	}

	@ParameterizedTest
	@MethodSource("servers")
	public void testLiveDSReplication(RedisServer server) throws Exception {
		RedisItemReader<String, DataStructure<String>> reader = dataStructureReader(server);
		RedisItemWriter<String, String, DataStructure<String>> writer = dataStructureWriter(REDIS_REPLICA);
		LiveRedisItemReader<String, DataStructure<String>> liveReader = liveDataStructureReader(server);
		RedisItemWriter<String, String, DataStructure<String>> liveWriter = dataStructureWriter(REDIS_REPLICA);
		replicate("live-ds-replication", server, reader, writer, liveReader, liveWriter);
	}

	private <T extends KeyValue<String, ?>> void replicate(String name, RedisServer server,
			RedisItemReader<String, T> reader, ItemWriter<T> writer, LiveRedisItemReader<String, T> liveReader,
			ItemWriter<T> liveWriter) throws Exception {
		reader.setName(name(server, name + "-reader"));
		liveReader.setName(name(server, name + "-live-reader"));
		dataGenerator(name, server).to(1000).build().call();
		TaskletStep replicationStep = step(name(server, "scan-" + name), reader, writer).build();
		TaskletStep liveReplicationStep = jobFactory
				.flushing(step(name(server, "live-" + name), liveReader, liveWriter)).build();
		SimpleFlow replicationFlow = jobFactory.flow(name(server, "scan-" + name)).start(replicationStep).build();
		SimpleFlow liveReplicationFlow = jobFactory.flow(name(server, "live-" + name)).start(liveReplicationStep)
				.build();
		Job job = jobFactory.job(name(server, name)).start(jobFactory.flow(name(server, name))
				.split(new SimpleAsyncTaskExecutor()).add(replicationFlow, liveReplicationFlow).build()).build()
				.build();
		JobExecution execution = jobFactory.runAsync(job, new JobParameters());
		jobFactory.awaitRunning(execution);
		jobFactory.awaitOpen(liveReader);
		Thread.sleep(100);
		dataGenerator("live-" + name, server).to(123).keyPrefix("live").build().call();
		jobFactory.awaitTermination(execution);
		compare(server, name);
	}

	private void compare(RedisServer server, String name) throws Exception {
		RedisServerCommands<String, String> sourceSync = sync(server);
		RedisServerCommands<String, String> targetSync = sync(REDIS_REPLICA);
		Assertions.assertEquals(sourceSync.dbsize(), targetSync.dbsize());
		RedisItemReader<String, DataStructure<String>> left = dataStructureReader(server);
		DataStructureValueReader<String, String> right = dataStructureValueReader(REDIS_REPLICA);
		KeyComparisonResultCounter<String> results = new KeyComparisonResultCounter<>();
		KeyComparisonItemWriter<String> writer = KeyComparisonItemWriter.valueReader(right).resultHandler(results)
				.resultHandler(new KeyComparisonMismatchPrinter<>()).build();
		run(server, name + "-compare", left, writer);
		Assertions.assertEquals(sourceSync.dbsize(), results.get(KeyComparisonItemWriter.Status.OK));
		Assertions.assertTrue(results.isOK());
	}

	@ParameterizedTest
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
		DataStructureValueReader<String, String> right = dataStructureValueReader(REDIS_REPLICA);
		KeyComparisonResultCounter<String> counter = new KeyComparisonResultCounter<String>();
		KeyComparisonItemWriter<String> writer = KeyComparisonItemWriter.valueReader(right).resultHandler(counter)
				.resultHandler(new KeyComparisonMismatchPrinter<>()).build();
		run(server, "test-comparison-writer-compare", dataStructureReader(server), writer);
		Assertions.assertTrue(counter.get(KeyComparisonItemWriter.Status.OK) > 0);
		Assertions.assertEquals(1, counter.get(KeyComparisonItemWriter.Status.MISSING));
	}

	@ParameterizedTest
	@MethodSource("servers")
	public void testScanSizeEstimator(RedisServer server) throws Exception {
		dataGenerator("scan-size-estimator", server).to(12345).dataTypes(DataType.HASH).build().call();
		ScanSizeEstimator estimator = sizeEstimator(server);
		String pattern = DataType.HASH + ":*";
		long expectedCount = sync(server).keys(pattern).size();
		long matchCount = estimator.estimate(EstimateOptions.builder().sampleSize(1000).match(pattern).build());
		Assertions.assertEquals(expectedCount, matchCount, expectedCount / 10);
		long typeSize = estimator.estimate(EstimateOptions.builder().sampleSize(1000).type(DataStructure.HASH).build());
		Assertions.assertEquals(expectedCount, typeSize, expectedCount / 10);
	}

	private ScanSizeEstimator sizeEstimator(RedisServer server) {
		return ScanSizeEstimator.client(client(server)).build();
	}

}
