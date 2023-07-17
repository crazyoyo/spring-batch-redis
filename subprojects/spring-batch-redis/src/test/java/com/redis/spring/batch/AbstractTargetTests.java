package com.redis.spring.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.common.IntRange;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.reader.GeneratorItemReader;
import com.redis.spring.batch.reader.GeneratorItemReader.HashOptions;
import com.redis.spring.batch.reader.GeneratorItemReader.Type;
import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.spring.batch.reader.KeyComparisonItemReader;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.QueueOptions;
import com.redis.spring.batch.writer.KeyComparisonCountItemWriter;
import com.redis.spring.batch.writer.KeyComparisonCountItemWriter.Results;
import com.redis.spring.batch.writer.MergePolicy;
import com.redis.spring.batch.writer.StructOptions;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;

abstract class AbstractTargetTests extends AbstractTests {

	private static class RedisVersion {

		private static Pattern redisVersionPattern = Pattern.compile("redis_version:(\\d)\\.(\\d).*");

		public static RedisVersion UNKNOWN = new RedisVersion(0, 0);

		private final int major;
		private final int minor;

		public RedisVersion(int major, int minor) {
			this.major = major;
			this.minor = minor;
		}

		public int getMajor() {
			return major;
		}

		@SuppressWarnings("unused")
		public int getMinor() {
			return minor;
		}

		public static RedisVersion of(StatefulRedisModulesConnection<String, String> connection) {
			String info = connection.sync().info("SERVER");
			Matcher matcher = redisVersionPattern.matcher(info);
			if (matcher.find()) {
				int major = Integer.parseInt(matcher.group(1));
				int minor = Integer.parseInt(matcher.group(2));
				return new RedisVersion(major, minor);
			}
			return UNKNOWN;
		}

	}

	protected AbstractRedisClient targetClient;
	protected StatefulRedisModulesConnection<String, String> targetConnection;

	protected abstract RedisServer getTargetServer();

	@BeforeAll
	void setupTarget() {
		getTargetServer().start();
		targetClient = client(getTargetServer());
		targetConnection = RedisModulesUtils.connection(targetClient);
	}

	@AfterAll
	void teardownTarget() {
		targetConnection.close();
		targetClient.shutdown();
		targetClient.getResources().shutdown();
		getTargetServer().close();
	}

	@BeforeEach
	void flushAllTarget() {
		targetConnection.sync().flushall();
	}

	/**
	 * 
	 * @param left
	 * @param right
	 * @return
	 * @return list of differences
	 * @throws Exception
	 */
	protected List<? extends KeyComparison> compare(TestInfo testInfo) throws Exception {
		TestInfo finalTestInfo = testInfo(testInfo, "compare", "reader");
		List<KeyComparison> comparisons = readAll(finalTestInfo, comparisonReader());
		Assertions.assertFalse(comparisons.isEmpty());
		return comparisons;
	}

	protected boolean isOk(List<? extends KeyComparison> comparisons) {
		return comparisons.stream().allMatch(c -> c.getStatus() == Status.OK);
	}

	protected KeyComparisonItemReader comparisonReader() throws Exception {
		return new KeyComparisonItemReader.Builder(sourceClient, targetClient).jobRepository(jobRepository)
				.ttlTolerance(Duration.ofMillis(100)).build();
	}

	@Test
	void writeStructsOverwrite(TestInfo testInfo) throws Exception {
		GeneratorItemReader gen1 = new GeneratorItemReader();
		gen1.setMaxItemCount(100);
		gen1.setTypes(Arrays.asList(Type.HASH));
		gen1.setHashOptions(HashOptions.builder().fieldCount(IntRange.is(5)).build());
		generate(testInfo, sourceClient, gen1);
		GeneratorItemReader gen2 = new GeneratorItemReader();
		gen2.setMaxItemCount(100);
		gen2.setTypes(Arrays.asList(Type.HASH));
		gen2.setHashOptions(HashOptions.builder().fieldCount(IntRange.is(10)).build());
		generate(testInfo, targetClient, gen2);
		RedisItemReader<String, String> reader = reader(sourceClient).struct();
		RedisItemWriter<String, String> writer = writer(targetClient)
				.structOptions(StructOptions.builder().mergePolicy(MergePolicy.OVERWRITE).build()).struct();
		run(testInfo, reader, writer);
		assertEquals(sourceConnection.sync().hgetall("gen:1"), targetConnection.sync().hgetall("gen:1"));
	}

	@Test
	void writeStructsMerge(TestInfo testInfo) throws Exception {
		GeneratorItemReader gen1 = new GeneratorItemReader();
		gen1.setMaxItemCount(100);
		gen1.setTypes(Arrays.asList(Type.HASH));
		gen1.setHashOptions(HashOptions.builder().fieldCount(IntRange.is(5)).build());
		generate(testInfo, sourceClient, gen1);
		GeneratorItemReader gen2 = new GeneratorItemReader();
		gen2.setMaxItemCount(100);
		gen2.setTypes(Arrays.asList(Type.HASH));
		gen2.setHashOptions(HashOptions.builder().fieldCount(IntRange.is(10)).build());
		generate(testInfo, targetClient, gen2);
		RedisItemReader<String, String> reader = reader(sourceClient).struct();
		RedisItemWriter<String, String> writer = writer(targetClient)
				.structOptions(StructOptions.builder().mergePolicy(MergePolicy.MERGE).build()).struct();
		run(testInfo, reader, writer);
		Map<String, String> actual = targetConnection.sync().hgetall("gen:1");
		assertEquals(10, actual.size());
	}

	@Test
	void setComparator(TestInfo testInfo) throws Exception {
		sourceConnection.sync().sadd("set:1", "value1", "value2");
		targetConnection.sync().sadd("set:1", "value2", "value1");
		KeyComparisonItemReader reader = comparisonReader();
		List<KeyComparison> comparisons = readAll(testInfo, reader);
		Assertions.assertEquals(KeyComparison.Status.OK, comparisons.get(0).getStatus());
	}

	@Test
	void byteArrayCodec(TestInfo testInfo) throws Exception {
		Assumptions.assumeFalse(RedisVersion.of(sourceConnection).getMajor() == 7);
		try (StatefulRedisConnection<byte[], byte[]> connection = RedisModulesUtils.connection(sourceClient,
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
		RedisItemReader<byte[], byte[]> reader = reader(sourceClient, ByteArrayCodec.INSTANCE).dump();
		RedisItemWriter<byte[], byte[]> writer = writer(targetClient, ByteArrayCodec.INSTANCE).dump();
		run(testInfo, reader, writer);
		Assertions.assertEquals(sourceConnection.sync().dbsize(), targetConnection.sync().dbsize());
	}

	@Test
	void liveOnlyReplication(TestInfo testInfo) throws Exception {
		Assumptions.assumeFalse(RedisVersion.of(sourceConnection).getMajor() == 7);
		enableKeyspaceNotifications(sourceClient);
		LiveRedisItemReader<byte[], byte[]> reader = reader(sourceClient, ByteArrayCodec.INSTANCE).live().dump();
		reader.getKeyspaceNotificationOptions().setQueueOptions(QueueOptions.builder().capacity(100000).build());
		reader.getFlushingOptions().setIdleTimeout(DEFAULT_IDLE_TIMEOUT);
		RedisItemWriter<byte[], byte[]> writer = writer(targetClient, ByteArrayCodec.INSTANCE).dump();
		JobExecution execution = runAsync(job(testInfo, reader, writer));
		GeneratorItemReader gen = new GeneratorItemReader();
		gen.setMaxItemCount(100);
		gen.setTypes(Arrays.asList(Type.HASH, Type.LIST, Type.SET, Type.STRING, Type.ZSET));
		generate(testInfo, gen);
		awaitTermination(execution);
		Assertions.assertTrue(isOk(compare(testInfo)));
	}

	@Test
	void liveDumpAndRestoreReplication(TestInfo testInfo) throws Exception {
		Assumptions.assumeFalse(RedisVersion.of(sourceConnection).getMajor() == 7);
		enableKeyspaceNotifications(sourceClient);
		RedisItemReader<byte[], byte[]> reader = reader(sourceClient, ByteArrayCodec.INSTANCE).dump();
		RedisItemWriter<byte[], byte[]> writer = writer(targetClient, ByteArrayCodec.INSTANCE).dump();
		LiveRedisItemReader<byte[], byte[]> liveReader = reader(sourceClient, ByteArrayCodec.INSTANCE).live().dump();
		configure(liveReader);
		RedisItemWriter<byte[], byte[]> liveWriter = writer(targetClient, ByteArrayCodec.INSTANCE).dump();
		liveReplication(testInfo, reader, writer, liveReader, liveWriter);
	}

	@Test
	void liveSetReplication(TestInfo testInfo) throws Exception {
		enableKeyspaceNotifications(sourceClient);
		String key = "myset";
		sourceConnection.sync().sadd(key, "1", "2", "3", "4", "5");
		LiveRedisItemReader<String, String> reader = reader(sourceClient).live().struct();
		reader.getKeyspaceNotificationOptions().setQueueOptions(QueueOptions.builder().capacity(100).build());
		RedisItemWriter<String, String> writer = writer(targetClient).struct();
		JobExecution execution = runAsync(job(testInfo, reader, writer));
		sourceConnection.sync().srem(key, "5");
		awaitTermination(execution);
		assertEquals(sourceConnection.sync().smembers(key), targetConnection.sync().smembers(key));
	}

	@Test
	void replicateHLL(TestInfo testInfo) throws Exception {
		String key1 = "hll:1";
		sourceConnection.sync().pfadd(key1, "member:1", "member:2");
		String key2 = "hll:2";
		sourceConnection.sync().pfadd(key2, "member:1", "member:2", "member:3");
		RedisItemReader<byte[], byte[]> reader = reader(sourceClient, ByteArrayCodec.INSTANCE).struct();
		RedisItemWriter<byte[], byte[]> writer = writer(targetClient, ByteArrayCodec.INSTANCE).struct();
		run(testInfo, reader, writer);
		RedisModulesCommands<String, String> sourceSync = sourceConnection.sync();
		RedisModulesCommands<String, String> targetSync = targetConnection.sync();
		assertEquals(sourceSync.pfcount(key1), targetSync.pfcount(key1));
	}

	@Test
	void replicateDs(TestInfo testInfo) throws Exception {
		GeneratorItemReader gen = new GeneratorItemReader();
		gen.setMaxItemCount(10000);
		gen.setTypes(Arrays.asList(Type.HASH, Type.LIST, Type.SET, Type.STREAM, Type.STRING, Type.ZSET));
		generate(testInfo, gen);
		RedisItemReader<byte[], byte[]> reader = reader(sourceClient, ByteArrayCodec.INSTANCE).struct();
		RedisItemWriter<byte[], byte[]> writer = writer(targetClient, ByteArrayCodec.INSTANCE).struct();
		run(testInfo, reader, writer);
		Assertions.assertTrue(isOk(compare(testInfo)));
	}

	@Test
	void replicateDsEmptyCollections(TestInfo testInfo) throws Exception {
		GeneratorItemReader gen = new GeneratorItemReader();
		gen.setMaxItemCount(10000);
		gen.setTypes(Arrays.asList(Type.HASH, Type.LIST, Type.SET, Type.STREAM, Type.STRING, Type.ZSET));
		generate(testInfo, gen);
		
		RedisItemReader<byte[], byte[]> reader = reader(sourceClient, ByteArrayCodec.INSTANCE).struct();
		RedisItemWriter<byte[], byte[]> writer = writer(targetClient, ByteArrayCodec.INSTANCE).struct();
		run(testInfo, reader, writer);
		Assertions.assertTrue(isOk(compare(testInfo)));
	}

	protected <K, V> void liveReplication(TestInfo testInfo, RedisItemReader<K, V> reader, RedisItemWriter<K, V> writer,
			LiveRedisItemReader<K, V> liveReader, RedisItemWriter<K, V> liveWriter) throws Exception {
		GeneratorItemReader gen = new GeneratorItemReader();
		gen.setMaxItemCount(300);
		gen.setTypes(Arrays.asList(Type.HASH, Type.LIST, Type.SET, Type.STREAM, Type.STRING, Type.ZSET));
		generate(testInfo(testInfo, "generate"), gen);
		TaskletStep step = step(testInfo(testInfo, "step"), reader, writer).build();
		SimpleFlow flow = new FlowBuilder<SimpleFlow>(name(testInfo(testInfo, "snapshotFlow"))).start(step).build();
		TaskletStep liveStep = step(testInfo(testInfo, "liveStep"), liveReader, liveWriter).build();
		SimpleFlow liveFlow = new FlowBuilder<SimpleFlow>(name(testInfo(testInfo, "liveFlow"))).start(liveStep).build();
		Job job = job(testInfo).start(new FlowBuilder<SimpleFlow>(name(testInfo(testInfo, "flow")))
				.split(new SimpleAsyncTaskExecutor()).add(liveFlow, flow).build()).build().build();
		JobExecution execution = runAsync(job);
		GeneratorItemReader liveGen = new GeneratorItemReader();
		liveGen.setMaxItemCount(700);
		liveGen.setTypes(Arrays.asList(Type.HASH, Type.LIST, Type.SET, Type.STRING, Type.ZSET));
		liveGen.setExpiration(IntRange.is(100));
		liveGen.setKeyRange(IntRange.from(300));
		generate(testInfo(testInfo, "generateLive"), liveGen);
		try {
			awaitTermination(execution);
		} catch (ConditionTimeoutException e) {
			// ignore
		}
		awaitClosed(reader);
		awaitClosed(writer);
		awaitClosed(liveReader);
		awaitClosed(liveWriter);
		Assertions.assertTrue(isOk(compare(testInfo)));
	}

	@Test
	void comparator(TestInfo testInfo) throws Exception {
		Assumptions.assumeFalse(RedisVersion.of(sourceConnection).getMajor() == 7);
		GeneratorItemReader gen = new GeneratorItemReader();
		gen.setMaxItemCount(120);
		generate(testInfo, gen);
		run(testInfo(testInfo, "replicate"), reader(sourceClient, ByteArrayCodec.INSTANCE).dump(),
				writer(targetClient, ByteArrayCodec.INSTANCE).dump());
		long deleted = 0;
		for (int index = 0; index < 13; index++) {
			deleted += targetConnection.sync().del(targetConnection.sync().randomkey());
		}
		Set<String> ttlChanges = new HashSet<>();
		for (int index = 0; index < 23; index++) {
			String key = targetConnection.sync().randomkey();
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
		KeyComparisonItemReader reader = comparisonReader();
		KeyComparisonCountItemWriter writer = new KeyComparisonCountItemWriter();
		long sourceCount = sourceConnection.sync().dbsize();
		run(testInfo(testInfo, "compare"), reader, writer);
		Results results = writer.getResults();
		sourceCount = sourceConnection.sync().dbsize();
		assertEquals(sourceCount, results.getTotalCount());
		assertEquals(sourceCount, targetConnection.sync().dbsize() + deleted);
		assertEquals(typeChanges.size(), results.getCount(Status.TYPE));
		assertEquals(valueChanges.size(), results.getCount(Status.VALUE));
		assertEquals(ttlChanges.size(), results.getCount(Status.TTL));
		assertEquals(deleted, results.getCount(Status.MISSING));
	}

	@Test
	void readLive(TestInfo testInfo) throws Exception {
		enableKeyspaceNotifications(sourceClient);
		LiveRedisItemReader<byte[], byte[]> reader = reader(sourceClient, ByteArrayCodec.INSTANCE).live().dump();
		reader.getKeyspaceNotificationOptions().setQueueOptions(QueueOptions.builder().capacity(10000).build());
		SynchronizedListItemWriter<KeyValue<byte[]>> writer = new SynchronizedListItemWriter<>();
		JobExecution execution = runAsync(job(testInfo, reader, writer));
		GeneratorItemReader gen = new GeneratorItemReader();
		int count = 123;
		gen.setMaxItemCount(count);
		gen.setTypes(Arrays.asList(Type.HASH, Type.STRING));
		generate(testInfo, gen);
		awaitTermination(execution);
		awaitClosed(reader);
		awaitClosed(writer);
		Set<String> keys = writer.getItems().stream()
				.map(d -> StringCodec.UTF8.decodeKey(ByteArrayCodec.INSTANCE.encodeKey(d.getKey())))
				.collect(Collectors.toSet());
		Assertions.assertEquals(sourceConnection.sync().dbsize(), keys.size());
	}

	@Test
	void dedupeKeyspaceNotifications() throws Exception {
		enableKeyspaceNotifications(sourceClient);
		KeyspaceNotificationItemReader<String, String> reader = new KeyspaceNotificationItemReader<>(sourceClient,
				StringCodec.UTF8);
		reader.open(new ExecutionContext());
		RedisModulesCommands<String, String> commands = sourceConnection.sync();
		String key = "key1";
		commands.zadd(key, 1, "member1");
		commands.zadd(key, 2, "member2");
		commands.zadd(key, 3, "member3");
		awaitUntil(() -> reader.getQueue().size() == 1);
		Assertions.assertEquals(key, reader.read());
		reader.close();
	}

}
