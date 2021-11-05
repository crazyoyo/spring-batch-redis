package com.redis.spring.batch;

import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.testcontainers.junit.jupiter.Container;

import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.DataStructure.Type;
import com.redis.spring.batch.support.KeyComparator;
import com.redis.spring.batch.support.KeyComparator.KeyComparatorBuilder;
import com.redis.spring.batch.support.KeyComparator.RightComparatorBuilder;
import com.redis.spring.batch.support.KeyValue;
import com.redis.spring.batch.support.LiveRedisItemReader;
import com.redis.spring.batch.support.ScanSizeEstimator;
import com.redis.spring.batch.support.ScanSizeEstimator.ScanSizeEstimatorBuilder;
import com.redis.spring.batch.support.compare.KeyComparisonLogger;
import com.redis.spring.batch.support.compare.KeyComparisonResults;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.api.sync.RedisSetCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class ReplicationTests extends AbstractRedisTestBase {

	@Container
	private static final RedisContainer TARGET = new RedisContainer();
	private RedisClient targetClient;
	private StatefulRedisConnection<String, String> targetConnection;

	@BeforeEach
	void setupTarget() {
		targetClient = RedisClient.create(TARGET.getRedisURI());
		targetConnection = targetClient.connect();
	}

	@AfterEach
	void cleanupTarget() throws InterruptedException {
		targetConnection.sync().flushall();
		targetConnection.close();
		targetClient.shutdown();
		targetClient.getResources().shutdown();
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testDataStructureReplication(RedisServer redis) throws Exception {
		String name = "ds-replication";
		execute(dataGenerator(redis, name).end(100));
		RedisItemReader<String, DataStructure<String>> reader = dataStructureReader(redis, name);
		run(redis, name, reader, dataStructureWriter(targetClient));
		compare(redis, name);
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testLiveDSSetReplication(RedisServer redis) throws Exception {
		String name = "live-ds-set-replication";
		RedisSetCommands<String, String> sync = sync(redis);
		String key = "myset";
		sync.sadd(key, "1", "2", "3", "4", "5");
		JobExecution execution = runFlushing(redis, name, liveDataStructureReader(redis, name, 100),
				dataStructureWriter(targetClient));
		log.info("Removing from set");
		sync.srem(key, "5");
		awaitTermination(execution);
		Set<String> source = sync.smembers(key);
		Set<String> target = targetConnection.sync().smembers(key);
		Assertions.assertEquals(source, target);
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testReplication(RedisServer server) throws Exception {
		String name = "replication";
		execute(dataGenerator(server, name).end(100));
		RedisItemReader<String, KeyValue<String, byte[]>> reader = keyDumpReader(server, name);
		reader.setName(name(server, name + "-reader"));
		RedisItemWriter<String, String, KeyValue<String, byte[]>> writer = keyDumpWriter(targetClient);
		run(server, name, reader, writer);
		compare(server, name);
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testLiveReplication(RedisServer server) throws Exception {
		String name = "live-replication";
		liveReplication(name, server, keyDumpReader(server, name), keyDumpWriter(targetClient),
				liveKeyDumpReader(server, name, 100000), keyDumpWriter(targetClient));
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testLiveDSReplication(RedisServer server) throws Exception {
		String name = "live-ds-replication";
		liveReplication(name, server, dataStructureReader(server, name), dataStructureWriter(targetClient),
				liveDataStructureReader(server, name, 100000), dataStructureWriter(targetClient));
	}

	private <T extends KeyValue<String, ?>> void liveReplication(String name, RedisServer server,
			RedisItemReader<String, T> reader, ItemWriter<T> writer, LiveRedisItemReader<String, T> liveReader,
			ItemWriter<T> liveWriter) throws Exception {
		execute(dataGenerator(server, name).end(3000));
		TaskletStep replicationStep = step(server, name, reader, null, writer).build();
		SimpleFlow replicationFlow = flow(server, "scan-" + name).start(replicationStep).build();
		TaskletStep liveReplicationStep = flushingStep(server, "live-" + name, liveReader, null, liveWriter).build();
		SimpleFlow liveReplicationFlow = flow(server, "live-" + name).start(liveReplicationStep).build();
		Job job = job(server, name).start(flow(server, name).split(new SimpleAsyncTaskExecutor())
				.add(liveReplicationFlow, replicationFlow).build()).build().build();
		JobExecution execution = launchAsync(job);
		awaitOpen(liveReader);
		execute(dataGenerator(server, "live-" + name).chunkSize(1).dataType(Type.HASH).dataType(Type.LIST)
				.dataType(Type.SET).dataType(Type.STRING).dataType(Type.ZSET).between(3000, 4000));
		awaitTermination(execution);
		compare(server, name);
	}

	private void compare(RedisServer server, String name) throws Exception {
		Assertions.assertEquals(dbsize(server), targetConnection.sync().dbsize());
		KeyComparator comparator = comparator(server, name).build();
		comparator.addListener(new KeyComparisonLogger(log));
		KeyComparisonResults results = comparator.call();
		Assertions.assertTrue(results.isOK());
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testComparator(RedisServer server) throws Exception {
		String name = "comparator";
		execute(dataGenerator(server, name + "-source-init").end(120));
		execute(dataGenerator(targetClient, name(server, name) + "-target-init").end(100));
		KeyComparator comparator = comparator(server, name).build();
		KeyComparisonResults results = comparator.call();
		Assertions.assertEquals(results.getSource(), results.getOK() + results.getMissing() + results.getValue());
		Assertions.assertEquals(120, results.getMissing());
		Assertions.assertEquals(300, results.getValue());
	}

	private KeyComparatorBuilder comparator(RedisServer server, String name) {
		return configureJobRepository(comparator(server).right(targetClient).id(name(server, name)));
	}

	private RightComparatorBuilder comparator(RedisServer redis) {
		AbstractRedisClient client = clients.get(redis);
		if (client instanceof RedisClusterClient) {
			return KeyComparator.left((RedisClusterClient) client);
		}
		return KeyComparator.left((RedisClient) client);
	}

	@SuppressWarnings("unchecked")
	@ParameterizedTest
	@MethodSource("servers")
	void testScanSizeEstimator(RedisServer server) throws Exception {
		String pattern = "hash:*";
		execute(dataGenerator(server, "scan-size-estimator").end(12345).dataType(Type.HASH));
		long expectedCount = ((RedisKeyCommands<String, String>) sync(server)).keys(pattern).size();
		long matchCount = sizeEstimator(server).sampleSize(1000).match(pattern).build().call();
		Assertions.assertEquals(expectedCount, matchCount, expectedCount / 10);
		long typeSize = sizeEstimator(server).sampleSize(1000).type(DataStructure.HASH).build().call();
		Assertions.assertEquals(expectedCount, typeSize, expectedCount / 10);
	}

	private ScanSizeEstimatorBuilder sizeEstimator(RedisServer server) {
		if (server.isCluster()) {
			return ScanSizeEstimator.client((RedisClusterClient) clients.get(server));
		}
		return ScanSizeEstimator.client((RedisClient) clients.get(server));
	}

}
