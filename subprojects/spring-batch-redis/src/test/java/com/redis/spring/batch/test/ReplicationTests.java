package com.redis.spring.batch.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.testcontainers.junit.jupiter.Container;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;
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
import com.redis.testcontainers.junit.jupiter.RedisTestContext;
import com.redis.testcontainers.junit.jupiter.RedisTestContextsSource;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.sync.RedisSetCommands;
import io.lettuce.core.cluster.RedisClusterClient;

class ReplicationTests extends AbstractRedisTestBase {

	private static final Logger log = LoggerFactory.getLogger(ReplicationTests.class);

	@Container
	private static final RedisContainer TARGET = new RedisContainer();

	@Override
	protected Collection<RedisServer> servers() {
		Collection<RedisServer> servers = new ArrayList<>();
		servers.addAll(super.servers());
		servers.add(TARGET);
		return servers;
	}

	@Override
	protected Collection<RedisServer> testServers() {
		return super.servers();
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testDataStructureReplication(RedisTestContext redis) throws Exception {
		String name = "ds-replication";
		execute(dataGenerator(redis, name).end(100));
		RedisItemReader<String, DataStructure<String>> reader = dataStructureReader(redis, name);
		run(redis, name, reader, dataStructureWriter(getContext(TARGET)));
		compare(redis, name);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testLiveDSSetReplication(RedisTestContext redis) throws Exception {
		String name = "live-ds-set-replication";
		RedisSetCommands<String, String> sync = redis.sync();
		String key = "myset";
		sync.sadd(key, "1", "2", "3", "4", "5");
		JobExecution execution = runFlushing(redis, name, liveDataStructureReader(redis, name, 100),
				dataStructureWriter(getContext(TARGET)));
		log.info("Removing from set");
		sync.srem(key, "5");
		awaitTermination(execution);
		Set<String> source = sync.smembers(key);
		Set<String> target = getContext(TARGET).sync().smembers(key);
		Assertions.assertEquals(source, target);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testReplication(RedisTestContext server) throws Exception {
		String name = "replication";
		execute(dataGenerator(server, name).end(100));
		RedisItemReader<String, KeyValue<String, byte[]>> reader = keyDumpReader(server, name);
		reader.setName(name(server, name + "-reader"));
		RedisItemWriter<String, String, KeyValue<String, byte[]>> writer = keyDumpWriter(getContext(TARGET));
		run(server, name, reader, writer);
		compare(server, name);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testLiveReplication(RedisTestContext server) throws Exception {
		String name = "live-replication";
		liveReplication(name, server, keyDumpReader(server, name), keyDumpWriter(getContext(TARGET)),
				liveKeyDumpReader(server, name, 100000), keyDumpWriter(getContext(TARGET)));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testLiveDSReplication(RedisTestContext server) throws Exception {
		String name = "live-ds-replication";
		liveReplication(name, server, dataStructureReader(server, name), dataStructureWriter(getContext(TARGET)),
				liveDataStructureReader(server, name, 100000), dataStructureWriter(getContext(TARGET)));
	}

	private <T extends KeyValue<String, ?>> void liveReplication(String name, RedisTestContext server,
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

	private void compare(RedisTestContext server, String name) throws Exception {
		Assertions.assertEquals(server.sync().dbsize(), getContext(TARGET).sync().dbsize());
		KeyComparator comparator = comparator(server, name).build();
		comparator.addListener(new KeyComparisonLogger(log));
		KeyComparisonResults results = comparator.call();
		Assertions.assertTrue(results.isOK());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testComparator(RedisTestContext server) throws Exception {
		String name = "comparator";
		execute(dataGenerator(server, name + "-source-init").end(120));
		execute(dataGenerator(getContext(TARGET), name(server, name) + "-target-init").end(100));
		KeyComparator comparator = comparator(server, name).build();
		KeyComparisonResults results = comparator.call();
		Assertions.assertEquals(results.getSource(), results.getOK() + results.getMissing() + results.getValue());
		Assertions.assertEquals(120, results.getMissing());
		Assertions.assertEquals(300, results.getValue());
	}

	private KeyComparatorBuilder comparator(RedisTestContext server, String name) {
		RightComparatorBuilder rightBuilder = comparator(server);
		KeyComparatorBuilder builder;
		if (getContext(TARGET).isCluster()) {
			builder = rightBuilder.right(getContext(TARGET).getRedisClusterClient());
		} else {
			builder = rightBuilder.right(getContext(TARGET).getRedisClient());
		}
		return configureJobRepository(builder.id(name(server, name)));
	}

	private RightComparatorBuilder comparator(RedisTestContext context) {
		AbstractRedisClient client = context.getClient();
		if (client instanceof RedisClusterClient) {
			return KeyComparator.left((RedisClusterClient) client);
		}
		return KeyComparator.left((RedisClient) client);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testScanSizeEstimator(RedisTestContext server) throws Exception {
		String pattern = "hash:*";
		execute(dataGenerator(server, "scan-size-estimator").end(12345).dataType(Type.HASH));
		long expectedCount = server.sync().keys(pattern).size();
		long matchCount = sizeEstimator(server).sampleSize(1000).match(pattern).build().call();
		Assertions.assertEquals(expectedCount, matchCount, expectedCount / 10);
		long typeSize = sizeEstimator(server).sampleSize(1000).type(DataStructure.HASH).build().call();
		Assertions.assertEquals(expectedCount, typeSize, expectedCount / 10);
	}

	private ScanSizeEstimatorBuilder sizeEstimator(RedisTestContext server) {
		if (server.isCluster()) {
			return ScanSizeEstimator.client(server.getRedisClusterClient());
		}
		return ScanSizeEstimator.client(server.getRedisClient());
	}

}
