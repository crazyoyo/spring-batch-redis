package com.redis.spring.batch;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.testcontainers.junit.jupiter.Container;

import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.spring.batch.builder.StreamItemReaderBuilder;
import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.DataStructureValueReader;
import com.redis.spring.batch.support.FlushingStepBuilder;
import com.redis.spring.batch.support.KeyComparator;
import com.redis.spring.batch.support.KeyComparator.KeyComparatorBuilder;
import com.redis.spring.batch.support.KeyComparator.RightComparatorBuilder;
import com.redis.spring.batch.support.KeyValue;
import com.redis.spring.batch.support.LiveKeyItemReader;
import com.redis.spring.batch.support.LiveRedisItemReader;
import com.redis.spring.batch.support.PollableItemReader;
import com.redis.spring.batch.support.ScanSizeEstimator;
import com.redis.spring.batch.support.ScanSizeEstimator.ScanSizeEstimatorBuilder;
import com.redis.spring.batch.support.StreamItemReader;
import com.redis.spring.batch.support.StreamItemReader.AckPolicy;
import com.redis.spring.batch.support.compare.KeyComparisonLogger;
import com.redis.spring.batch.support.compare.KeyComparisonResults;
import com.redis.spring.batch.support.convert.GeoValueConverter;
import com.redis.spring.batch.support.convert.ScoredValueConverter;
import com.redis.spring.batch.support.generator.Generator;
import com.redis.spring.batch.support.generator.Generator.GeneratorBuilder;
import com.redis.spring.batch.support.generator.Generator.Type;
import com.redis.spring.batch.support.operation.Geoadd;
import com.redis.spring.batch.support.operation.Hset;
import com.redis.spring.batch.support.operation.Xadd;
import com.redis.spring.batch.support.operation.Zadd;
import com.redis.testcontainers.RedisClusterContainer;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.junit.jupiter.RedisTestContext;
import com.redis.testcontainers.junit.jupiter.RedisTestContextsSource;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.GeoArgs;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisSetCommands;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.models.stream.PendingMessages;

class BatchTests extends AbstractTestBase {

	private static final Logger log = LoggerFactory.getLogger(BatchTests.class);

	@Container
	protected static final RedisContainer REDIS = new RedisContainer(
			RedisContainer.DEFAULT_IMAGE_NAME.withTag(RedisContainer.DEFAULT_TAG)).withKeyspaceNotifications();
	@Container
	protected static final RedisClusterContainer REDIS_CLUSTER = new RedisClusterContainer(
			RedisClusterContainer.DEFAULT_IMAGE_NAME.withTag(RedisClusterContainer.DEFAULT_TAG))
					.withKeyspaceNotifications();
	@Container
	private static final RedisContainer TARGET = new RedisContainer(
			RedisContainer.DEFAULT_IMAGE_NAME.withTag(RedisContainer.DEFAULT_TAG));

	@Override
	protected Collection<RedisServer> testServers() {
		return Arrays.asList(REDIS, REDIS_CLUSTER);
	}

	@Override
	protected Collection<RedisServer> servers() {
		return Arrays.asList(REDIS, REDIS_CLUSTER, TARGET);
	}

	private static final String STREAM = "stream:1";

	@ParameterizedTest
	@RedisTestContextsSource
	void testFlushingStep(RedisTestContext context) throws Exception {
		String name = "flushing-step";
		PollableItemReader<String> reader = keyspaceNotificationReader(context);
		ListItemWriter<String> writer = new ListItemWriter<>();
		JobExecution execution = runFlushing(context, name, reader, null, writer);
		execute(dataGenerator(context, name).end(3).type(Type.STRING).type(Type.HASH));
		awaitTermination(execution);
		Assertions.assertEquals(context.sync().dbsize(), writer.getWrittenItems().size());
	}

	private LiveKeyItemReader<String> keyspaceNotificationReader(RedisTestContext context) {
		return reader(context).dataStructure().live().keyReader();
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testKeyspaceNotificationReader(RedisTestContext context) throws Exception {
		String name = "keyspace-notification-reader";
		LiveKeyItemReader<String> keyReader = keyspaceNotificationReader(context);
		keyReader.open(new ExecutionContext());
		execute(dataGenerator(context, name).type(Type.HASH).type(Type.STRING).type(Type.LIST).type(Type.SET)
				.type(Type.ZSET).end(2));
		ListItemWriter<String> writer = new ListItemWriter<>();
		run(context, name, keyReader, writer);
		Assertions.assertEquals(context.sync().dbsize(), writer.getWrittenItems().size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testDataStructureReader(RedisTestContext context) throws Exception {
		String name = "ds-reader";
		dataGenerator(context, name).build().call();
		RedisItemReader<String, DataStructure<String>> reader = dataStructureReader(context, name);
		ListItemWriter<DataStructure<String>> writer = new ListItemWriter<>();
		run(context, name, reader, writer);
		Assertions.assertEquals(context.sync().dbsize(), writer.getWrittenItems().size());
	}

	private static class SynchronizedListItemWriter<T> implements ItemWriter<T> {

		private List<T> writtenItems = new ArrayList<>();

		@Override
		public synchronized void write(List<? extends T> items) throws Exception {
			writtenItems.addAll(items);
		}

		public List<? extends T> getWrittenItems() {
			return this.writtenItems;
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testMultiThreadedReader(RedisTestContext context) throws Exception {
		String name = "multi-threaded-reader";
		dataGenerator(context, name).build().call();
		RedisItemReader<String, DataStructure<String>> reader = dataStructureReader(context, name);
		SynchronizedItemStreamReader<DataStructure<String>> synchronizedReader = new SynchronizedItemStreamReader<>();
		synchronizedReader.setDelegate(reader);
		synchronizedReader.afterPropertiesSet();
		SynchronizedListItemWriter<DataStructure<String>> writer = new SynchronizedListItemWriter<>();
		int threads = 4;
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setMaxPoolSize(threads);
		taskExecutor.setCorePoolSize(threads);
		taskExecutor.afterPropertiesSet();
		launch(job(context, name, step(context, name, synchronizedReader, null, writer).taskExecutor(taskExecutor)
				.throttleLimit(threads).build()).build());
		Assertions.assertEquals(context.sync().dbsize(), writer.getWrittenItems().size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testLiveReader(RedisTestContext context) throws Exception {
		String name = "live-reader";
		LiveRedisItemReader<String, KeyValue<String, byte[]>> reader = liveKeyDumpReader(context, name, 10000);
		ListItemWriter<KeyValue<String, byte[]>> writer = new ListItemWriter<>();
		JobExecution execution = runFlushing(context, name, reader, null, writer);
		execute(dataGenerator(context, name).end(123).type(Type.HASH).type(Type.STRING));
		awaitTermination(execution);
		Assertions.assertEquals(context.sync().dbsize(), writer.getWrittenItems().size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testKeyValueItemReaderFaultTolerance(RedisTestContext context) throws Exception {
		String name = "reader-ft";
		execute(dataGenerator(context, name).type(Type.STRING));
		List<String> keys = IntStream.range(0, 100).boxed().map(i -> "string:" + i).collect(Collectors.toList());
		DelegatingPollableItemReader<String> keyReader = new DelegatingPollableItemReader<>(new ListItemReader<>(keys));
		DataStructureValueReader<String, String> valueReader = dataStructureValueReader(context);
		RedisItemReader<String, DataStructure<String>> reader = new RedisItemReader<>(jobRepository, transactionManager,
				keyReader, valueReader);
		reader.setChunkSize(1);
		reader.setQueueCapacity(1000);
		reader.setSkipPolicy(new AlwaysSkipItemSkipPolicy());
		reader.setName(name(context, name + "-reader"));
		ListItemWriter<DataStructure<String>> writer = new ListItemWriter<>();
		run(context, name, reader, writer);
		Assertions.assertEquals(50, writer.getWrittenItems().size());
	}

	private static final long COUNT = 100;

	private void assertMessageBody(List<? extends StreamMessage<String, String>> items) {
		for (StreamMessage<String, String> message : items) {
			Assertions.assertTrue(message.getBody().containsKey("field1"));
			Assertions.assertTrue(message.getBody().containsKey("field2"));
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testStreamReader(RedisTestContext redis) throws Exception {
		String name = "stream-reader";
		execute(streamDataGenerator(redis, name));
		StreamItemReader<String, String> reader = streamReader(redis).build();
		reader.open(new ExecutionContext());
		List<StreamMessage<String, String>> messages = reader.readMessages();
		Assertions.assertEquals(StreamItemReader.DEFAULT_COUNT, messages.size());
		assertMessageBody(messages);
	}

	private GeneratorBuilder streamDataGenerator(RedisTestContext redis, String name) {
		return dataGenerator(redis, name).type(Type.STREAM).end(1)
				.collectionCardinality(org.apache.commons.lang3.Range.is(COUNT));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testStreamReaderJob(RedisTestContext redis) throws Exception {
		String name = "stream-reader-job";
		execute(streamDataGenerator(redis, name));
		Assertions.assertEquals(COUNT, redis.sync().xlen(STREAM));
		StreamItemReader<String, String> reader = streamReader(redis).build();
		ListItemWriter<StreamMessage<String, String>> writer = new ListItemWriter<>();
		JobExecution execution = runFlushing(redis, name, reader, null, writer);
		awaitTermination(execution);
		Assertions.assertEquals(COUNT, writer.getWrittenItems().size());
		assertMessageBody(writer.getWrittenItems());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testMultipleStreamReaders(RedisTestContext redis) throws Exception {
		String name = "multiple-stream-readers";
		String consumerGroup = "consumerGroup";
		execute(streamDataGenerator(redis, name));
		StreamItemReader<String, String> reader1 = streamReader(redis).consumerGroup(consumerGroup)
				.consumer("consumer1").ackPolicy(AckPolicy.MANUAL).build();
		StreamItemReader<String, String> reader2 = streamReader(redis).consumerGroup(consumerGroup)
				.consumer("consumer2").ackPolicy(AckPolicy.MANUAL).build();
		ListItemWriter<StreamMessage<String, String>> writer1 = new ListItemWriter<>();
		JobExecution execution1 = runFlushing(redis, "stream-reader-1", reader1, null, writer1);
		ListItemWriter<StreamMessage<String, String>> writer2 = new ListItemWriter<>();
		JobExecution execution2 = runFlushing(redis, "stream-reader-2", reader2, null, writer2);
		awaitTermination(execution1);
		awaitTermination(execution2);
		Assertions.assertEquals(COUNT, writer1.getWrittenItems().size() + writer2.getWrittenItems().size());
		assertMessageBody(writer1.getWrittenItems());
		assertMessageBody(writer2.getWrittenItems());
		RedisStreamCommands<String, String> sync = redis.sync();
		PendingMessages pendingMessages = sync.xpending(STREAM, consumerGroup);
		Assertions.assertEquals(COUNT, pendingMessages.getCount());
		reader1.open(new ExecutionContext());
		reader1.ack(writer1.getWrittenItems());
		reader2.open(new ExecutionContext());
		reader2.ack(writer2.getWrittenItems());
		pendingMessages = sync.xpending(STREAM, consumerGroup);
		Assertions.assertEquals(0, pendingMessages.getCount());
	}

	private StreamItemReaderBuilder streamReader(RedisTestContext server) {
		return reader(server).stream(STREAM);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testStreamWriter(RedisTestContext redis) throws Exception {
		String stream = "stream:0";
		List<Map<String, String>> messages = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			Map<String, String> body = new HashMap<>();
			body.put("field1", "value1");
			body.put("field2", "value2");
			messages.add(body);
		}
		ListItemReader<Map<String, String>> reader = new ListItemReader<>(messages);
		RedisItemWriter<String, String, Map<String, String>> writer = operationWriter(redis,
				Xadd.<String, String, Map<String, String>>key(stream).body(t -> t).build()).build();
		run(redis, "stream-writer", reader, writer);
		RedisModulesCommands<String, String> sync = redis.sync();
		Assertions.assertEquals(messages.size(), sync.xlen(stream));
		List<StreamMessage<String, String>> xrange = sync.xrange(stream, Range.create("-", "+"));
		for (int index = 0; index < xrange.size(); index++) {
			StreamMessage<String, String> message = xrange.get(index);
			Assertions.assertEquals(messages.get(index), message.getBody());
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testRedisItemWriterWait(RedisTestContext context) throws Exception {
		List<Map<String, String>> maps = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			Map<String, String> body = new HashMap<>();
			body.put("id", String.valueOf(index));
			body.put("field1", "value1");
			body.put("field2", "value2");
			maps.add(body);
		}
		ListItemReader<Map<String, String>> reader = new ListItemReader<>(maps);
		RedisItemWriter<String, String, Map<String, String>> writer = operationWriter(context,
				Hset.<String, String, Map<String, String>>key(m -> "hash:" + m.remove("id")).map(m -> m).build())
						.waitForReplication(1, 300).build();
		JobExecution execution = run(context, "writer-wait", reader, writer);
		Assertions.assertEquals(ExitStatus.FAILED.getExitCode(), execution.getExitStatus().getExitCode());
		Assertions.assertEquals(1, execution.getAllFailureExceptions().size());
		Assertions.assertEquals(RedisCommandExecutionException.class,
				execution.getAllFailureExceptions().get(0).getClass());
		Assertions.assertEquals("Insufficient replication level - expected: 1, actual: 0",
				execution.getAllFailureExceptions().get(0).getMessage());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testHashWriter(RedisTestContext server) throws Exception {
		List<Map<String, String>> maps = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			Map<String, String> body = new HashMap<>();
			body.put("id", String.valueOf(index));
			body.put("field1", "value1");
			body.put("field2", "value2");
			maps.add(body);
		}
		ListItemReader<Map<String, String>> reader = new ListItemReader<>(maps);
		RedisItemWriter<String, String, Map<String, String>> writer = operationWriter(server,
				Hset.<String, String, Map<String, String>>key(m -> "hash:" + m.remove("id")).map(m -> m).build())
						.build();
		run(server, "hash-writer", reader, writer);
		Assertions.assertEquals(maps.size(), server.sync().keys("hash:*").size());
		for (int index = 0; index < maps.size(); index++) {
			Map<String, String> hash = server.sync().hgetall("hash:" + index);
			Assertions.assertEquals(maps.get(index), hash);
		}
	}

	private static class Geo {
		private String member;
		private double longitude;
		private double latitude;

		public Geo(String member, double longitude, double latitude) {
			this.member = member;
			this.longitude = longitude;
			this.latitude = latitude;
		}

		public String getMember() {
			return member;
		}

		public double getLongitude() {
			return longitude;
		}

		public double getLatitude() {
			return latitude;
		}

	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testGeoaddWriter(RedisTestContext redis) throws Exception {
		ListItemReader<Geo> reader = new ListItemReader<>(
				Arrays.asList(new Geo("Venice Breakwater", -118.476056, 33.985728),
						new Geo("Long Beach National", -73.667022, 40.582739)));
		GeoValueConverter<String, Geo> value = new GeoValueConverter<>(Geo::getMember, Geo::getLongitude,
				Geo::getLatitude);
		RedisItemWriter<String, String, Geo> writer = operationWriter(redis,
				Geoadd.<String, String, Geo>key("geoset").value(value).build()).build();
		run(redis, "geoadd-writer", reader, writer);
		Set<String> radius1 = redis.sync().georadius("geoset", -118, 34, 100, GeoArgs.Unit.mi);
		Assertions.assertEquals(1, radius1.size());
		Assertions.assertTrue(radius1.contains("Venice Breakwater"));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testHashDelWriter(RedisTestContext server) throws Exception {
		List<Entry<String, Map<String, String>>> hashes = new ArrayList<>();
		RedisModulesCommands<String, String> sync = server.sync();
		for (int index = 0; index < 100; index++) {
			String key = String.valueOf(index);
			Map<String, String> value = new HashMap<>();
			value.put("field1", "value1");
			sync.hset("hash:" + key, value);
			Map<String, String> body = new HashMap<>();
			body.put("field2", "value2");
			hashes.add(new AbstractMap.SimpleEntry<>(key, index < 50 ? null : body));
		}
		ListItemReader<Map.Entry<String, Map<String, String>>> reader = new ListItemReader<>(hashes);
		RedisItemWriter<String, String, Map.Entry<String, Map<String, String>>> writer = operationWriter(server,
				Hset.<String, String, Entry<String, Map<String, String>>>key(e -> "hash:" + e.getKey())
						.map(Map.Entry::getValue).build()).build();
		run(server, "hash-del-writer", reader, writer);
		Assertions.assertEquals(50, sync.keys("hash:*").size());
		Assertions.assertEquals(2, sync.hgetall("hash:50").size());
	}

	private static class ZValue {

		private String member;
		private double score;

		public ZValue(String member, double score) {
			super();
			this.member = member;
			this.score = score;
		}

		public String getMember() {
			return member;
		}

		public double getScore() {
			return score;
		}

	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testSortedSetWriter(RedisTestContext server) throws Exception {
		List<ZValue> values = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			values.add(new ZValue(String.valueOf(index), index % 10));
		}
		ListItemReader<ZValue> reader = new ListItemReader<>(values);
		ScoredValueConverter<String, ZValue> converter = new ScoredValueConverter<>(ZValue::getMember,
				ZValue::getScore);
		RedisItemWriter<String, String, ZValue> writer = operationWriter(server,
				Zadd.<String, String, ZValue>key("zset").value(converter).build()).build();
		run(server, "sorted-set-writer", reader, writer);
		RedisModulesCommands<String, String> sync = server.sync();
		Assertions.assertEquals(1, sync.dbsize());
		Assertions.assertEquals(values.size(), sync.zcard("zset"));
		List<String> range = sync.zrangebyscore("zset",
				Range.from(Range.Boundary.including(0), Range.Boundary.including(5)));
		Assertions.assertEquals(60, range.size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testDataStructureWriter(RedisTestContext context) throws Exception {
		List<DataStructure<String>> list = new ArrayList<>();
		long count = 100;
		for (int index = 0; index < count; index++) {
			Map<String, String> map = new HashMap<>();
			map.put("field1", "value1");
			map.put("field2", "value2");
			list.add(DataStructure.createHash("hash:" + index, map));
		}
		ListItemReader<DataStructure<String>> reader = new ListItemReader<>(list);
		RedisItemWriter<String, String, DataStructure<String>> writer = dataStructureWriter(context);
		run(context, "value-writer", reader, writer);
		RedisModulesCommands<String, String> sync = context.sync();
		List<String> keys = sync.keys("hash:*");
		Awaitility.await().until(() -> count == keys.size());
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

	@Test
	void testReplicationBinary() throws Exception {
		String name = "replication-binary";
		try (RedisTestContext redis = new RedisTestContext(REDIS)) {
			RedisClient client = redis.getRedisClient();
			StatefulRedisConnection<byte[], byte[]> connection = client.connect(new ByteArrayCodec());
			RedisAsyncCommands<byte[], byte[]> async = connection.async();
			async.setAutoFlushCommands(false);
			List<RedisFuture<?>> futures = new ArrayList<>();
			Random random = new Random();
			for (int index = 0; index < 100; index++) {
				String key = "binary:" + index;
				byte[] value = new byte[1000];
				random.nextBytes(value);
				futures.add(async.set(key.getBytes(), value));
			}
			async.flushCommands();
			LettuceFutures.awaitAll(connection.getTimeout(), futures.toArray(new RedisFuture[0]));
			RedisItemReader<String, KeyValue<String, byte[]>> reader = keyDumpReader(redis, name);
			reader.setName(name(redis, name + "-reader"));
			RedisItemWriter<String, String, KeyValue<String, byte[]>> writer = keyDumpWriter(getContext(TARGET));
			run(redis, name, reader, writer);
			compare(redis, name);
		}
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
			RedisItemReader<String, T> reader, RedisItemWriter<String, String, T> writer,
			LiveRedisItemReader<String, T> liveReader, RedisItemWriter<String, String, T> liveWriter) throws Exception {
		execute(dataGenerator(server, name).end(3000));
		TaskletStep replicationStep = step(server, name, reader, null, writer).build();
		SimpleFlow replicationFlow = flow(server, "scan-" + name).start(replicationStep).build();
		TaskletStep liveReplicationStep = flushingStep(server, "live-" + name, liveReader, null, liveWriter).build();
		SimpleFlow liveReplicationFlow = flow(server, "live-" + name).start(liveReplicationStep).build();
		Job job = job(server, name).start(flow(server, name).split(new SimpleAsyncTaskExecutor())
				.add(liveReplicationFlow, replicationFlow).build()).build().build();
		JobExecution execution = launchAsync(job);
		// TODO awaitOpen(liveReader);
		GeneratorBuilder liveGenerator = dataGenerator(server, "live-" + name).chunkSize(1)
				.types(Type.HASH, Type.LIST, Type.SET, Type.STRING, Type.ZSET).between(3000, 4000);
		execute(liveGenerator);
		awaitTermination(execution);
		awaitClosed(writer);
		awaitClosed(liveWriter);
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
		execute(dataGenerator(server, "scan-size-estimator").end(12345).type(Type.HASH));
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

	@ParameterizedTest
	@RedisTestContextsSource
	void testGeneratorDefaults(RedisTestContext context) throws Exception {
		execute(dataGenerator(context, "defaults"));
		RedisModulesCommands<String, String> sync = context.sync();
		long expectedCount = Generator.DEFAULT_SEQUENCE.getMaximum() - Generator.DEFAULT_SEQUENCE.getMinimum();
		long actualStringCount = sync.keys("string:*").size();
		Assertions.assertEquals(expectedCount, actualStringCount);
		Assertions.assertEquals(expectedCount * DataStructure.types().size(), sync.dbsize());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testGeneratorToOption(RedisTestContext context) throws Exception {
		int count = 123;
		execute(dataGenerator(context, "to-options").end(count));
		RedisModulesCommands<String, String> sync = context.sync();
		int actualStringCount = sync.keys("string:*").size();
		Assertions.assertEquals(count, actualStringCount);
		Awaitility.await().until(() -> count * DataStructure.types().size() == sync.dbsize());
		Assertions.assertEquals(Generator.DEFAULT_COLLECTION_CARDINALITY.getMinimum(), sync.scard("set:100"));
		Assertions.assertEquals(Generator.DEFAULT_COLLECTION_CARDINALITY.getMinimum(), sync.llen("list:101"));
		Assertions.assertEquals(Generator.DEFAULT_COLLECTION_CARDINALITY.getMinimum(), sync.zcard("zset:102"));
		Assertions.assertEquals(Generator.DEFAULT_COLLECTION_CARDINALITY.getMinimum(), sync.xlen("stream:103"));
	}

	@Test
	void testReaderSkipPolicy() throws Exception {
		String name = "skip-policy";
		List<Integer> items = IntStream.range(0, 100).boxed().collect(Collectors.toList());
		DelegatingPollableItemReader<Integer> reader = new DelegatingPollableItemReader<>(new ListItemReader<>(items));
		ListItemWriter<Integer> writer = new ListItemWriter<>();
		FlushingStepBuilder<Integer, Integer> stepBuilder = new FlushingStepBuilder<>(
				stepBuilderFactory.get(name).<Integer, Integer>chunk(1).reader(reader).writer(writer));
		stepBuilder.idleTimeout(Duration.ofMillis(100)).skip(TimeoutException.class)
				.skipPolicy(new AlwaysSkipItemSkipPolicy());
		launch(jobBuilderFactory.get(name).start(stepBuilder.build()).build());
		Assertions.assertEquals(items.size(), writer.getWrittenItems().size() * 2);
	}
}
