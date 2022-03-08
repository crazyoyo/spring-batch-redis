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
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
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

import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.spring.batch.RedisItemWriter.OperationBuilder;
import com.redis.spring.batch.RedisScanSizeEstimator.Builder;
import com.redis.spring.batch.compare.KeyComparator;
import com.redis.spring.batch.compare.KeyComparisonResults;
import com.redis.spring.batch.convert.GeoValueConverter;
import com.redis.spring.batch.convert.ScoredValueConverter;
import com.redis.spring.batch.generator.Generator;
import com.redis.spring.batch.generator.Generator.Type;
import com.redis.spring.batch.reader.DataStructureValueReader;
import com.redis.spring.batch.reader.LiveKeyItemReader;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.PollableItemReader;
import com.redis.spring.batch.reader.ScanRedisItemReaderBuilder;
import com.redis.spring.batch.reader.StreamItemReader;
import com.redis.spring.batch.reader.StreamItemReader.AckPolicy;
import com.redis.spring.batch.reader.StreamItemReaderBuilder;
import com.redis.spring.batch.step.FlushingSimpleStepBuilder;
import com.redis.spring.batch.support.JobRunner;
import com.redis.spring.batch.writer.operation.Geoadd;
import com.redis.spring.batch.writer.operation.Hset;
import com.redis.spring.batch.writer.operation.Xadd;
import com.redis.spring.batch.writer.operation.Zadd;
import com.redis.testcontainers.RedisClusterContainer;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

import io.lettuce.core.GeoArgs;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisSetCommands;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.codec.ByteArrayCodec;

class BatchTests extends AbstractTestBase {

	private static final Logger log = LoggerFactory.getLogger(BatchTests.class);

	protected static final RedisContainer REDIS = new RedisContainer(
			RedisContainer.DEFAULT_IMAGE_NAME.withTag(RedisContainer.DEFAULT_TAG)).withKeyspaceNotifications();
	protected static final RedisClusterContainer REDIS_CLUSTER = new RedisClusterContainer(
			RedisClusterContainer.DEFAULT_IMAGE_NAME.withTag(RedisClusterContainer.DEFAULT_TAG))
					.withKeyspaceNotifications();
	private static final RedisContainer TARGET = new RedisContainer(
			RedisContainer.DEFAULT_IMAGE_NAME.withTag(RedisContainer.DEFAULT_TAG));
	private static final String STREAM = "stream:1";

	@Override
	protected Collection<RedisServer> redisServers() {
		return Arrays.asList(REDIS, REDIS_CLUSTER, TARGET);
	}

	@Override
	protected Collection<RedisServer> testRedisServers() {
		return Arrays.asList(REDIS, REDIS_CLUSTER);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testFlushingStep(RedisTestContext context) throws Exception {
		String name = "flushing-step";
		PollableItemReader<String> reader = keyspaceNotificationReader(context);
		ListItemWriter<String> writer = new ListItemWriter<>();
		JobExecution execution = runFlushing(context, name, reader, null, writer);
		execute(dataGenerator(context, name).end(3).type(Type.STRING).type(Type.HASH));
		JobRunner.awaitTermination(execution);
		Awaitility.await().until(() -> context.sync().dbsize() == writer.getWrittenItems().size());
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
		Awaitility.await().until(() -> context.sync().dbsize() == writer.getWrittenItems().size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testDataStructureReader(RedisTestContext context) throws Exception {
		String name = "ds-reader";
		dataGenerator(context, name).build().call();
		RedisItemReader<String, DataStructure<String>> reader = dataStructureReader(context, name);
		ListItemWriter<DataStructure<String>> writer = new ListItemWriter<>();
		run(context, name, reader, writer);
		Awaitility.await().until(() -> context.sync().dbsize() == writer.getWrittenItems().size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testDataStructureIntrospectingReader(RedisTestContext context) throws Exception {
		String name = "ds-reader";
		dataGenerator(context, name).build().call();
		RedisItemReader<String, DataStructure<String>> reader = dataStructureReader(context, name);
		ListItemWriter<DataStructure<String>> writer = new ListItemWriter<>();
		run(context, name, reader, writer);
		Awaitility.await().until(() -> context.sync().dbsize() == writer.getWrittenItems().size());
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
		Awaitility.await().until(() -> context.sync().dbsize() == writer.getWrittenItems().size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testLiveReader(RedisTestContext context) throws Exception {
		String name = "live-reader";
		LiveRedisItemReader<String, KeyValue<String, byte[]>> reader = liveKeyDumpReader(context, name, 10000);
		ListItemWriter<KeyValue<String, byte[]>> writer = new ListItemWriter<>();
		JobExecution execution = runFlushing(context, name, reader, null, writer);
		execute(dataGenerator(context, name).end(123).type(Type.HASH).type(Type.STRING));
		JobRunner.awaitTermination(execution);
		Awaitility.await().until(() -> context.sync().dbsize() == writer.getWrittenItems().size());
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
		Awaitility.await().until(() -> 50 == writer.getWrittenItems().size());
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
		Awaitility.await().until(() -> StreamItemReader.DEFAULT_COUNT == messages.size());
		assertMessageBody(messages);
	}

	private Generator.Builder streamDataGenerator(RedisTestContext redis, String name) {
		return dataGenerator(redis, name).type(Type.STREAM).end(1)
				.collectionCardinality(com.redis.spring.batch.generator.Range.is(COUNT));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testStreamReaderJob(RedisTestContext redis) throws Exception {
		String name = "stream-reader-job";
		execute(streamDataGenerator(redis, name));
		Awaitility.await().until(() -> COUNT == redis.sync().xlen(STREAM));
		StreamItemReader<String, String> reader = streamReader(redis).build();
		ListItemWriter<StreamMessage<String, String>> writer = new ListItemWriter<>();
		JobExecution execution = runFlushing(redis, name, reader, null, writer);
		JobRunner.awaitTermination(execution);
		Awaitility.await().until(() -> COUNT == writer.getWrittenItems().size());
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
		JobRunner.awaitTermination(execution1);
		JobRunner.awaitTermination(execution2);
		Awaitility.await().until(() -> COUNT == writer1.getWrittenItems().size() + writer2.getWrittenItems().size());
		assertMessageBody(writer1.getWrittenItems());
		assertMessageBody(writer2.getWrittenItems());
		RedisStreamCommands<String, String> sync = redis.sync();
		Awaitility.await().until(() -> COUNT == sync.xpending(STREAM, consumerGroup).getCount());
		reader1.open(new ExecutionContext());
		reader1.ack(writer1.getWrittenItems());
		reader2.open(new ExecutionContext());
		reader2.ack(writer2.getWrittenItems());
		Awaitility.await().until(() -> 0 == sync.xpending(STREAM, consumerGroup).getCount());
	}

	private StreamItemReaderBuilder<String, String> streamReader(RedisTestContext server) {
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
		Awaitility.await().until(() -> messages.size() == sync.xlen(stream));
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
		Assertions.assertThrows(JobExecutionException.class, () -> run(context, "writer-wait", reader, writer),
				"Insufficient replication level - expected: 1, actual: 0");
//		Assertions.assertEquals(ExitStatus.FAILED.getExitCode(), execution.getExitStatus().getExitCode());
//		Assertions.assertEquals(1, execution.getAllFailureExceptions().size());
//		Assertions.assertEquals(RedisCommandExecutionException.class,
//				execution.getAllFailureExceptions().get(0).getClass());
//		Assertions.assertEquals("Insufficient replication level - expected: 1, actual: 0",
//				execution.getAllFailureExceptions().get(0).getMessage());
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
		Awaitility.await().until(() -> maps.size() == server.sync().keys("hash:*").size());
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
		Awaitility.await().until(() -> 50 == sync.keys("hash:*").size());
		Awaitility.await().until(() -> 2 == sync.hgetall("hash:50").size());
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
		Awaitility.await().until(() -> 1 == sync.dbsize());
		Awaitility.await().until(() -> values.size() == sync.zcard("zset"));
		Awaitility.await().until(() -> 60 == sync
				.zrangebyscore("zset", Range.from(Range.Boundary.including(0), Range.Boundary.including(5))).size());
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
			list.add(new DataStructure<>(DataStructure.TYPE_HASH + ":" + index, map, DataStructure.TYPE_HASH));
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
	void testDataStructureReplication(RedisTestContext server) throws Exception {
		String name = "ds-replication";
		RedisTestContext target = getContext(TARGET);
		execute(dataGenerator(server, name).end(100));
		RedisItemReader<String, DataStructure<String>> reader = dataStructureReader(server, name);
		run(server, name, reader, dataStructureWriter(target));
		compare(name, server, target);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testLiveDSSetReplication(RedisTestContext redis) throws Exception {
		String name = "live-ds-set-replication";
		RedisSetCommands<String, String> sync = redis.sync();
		String key = "myset";
		sync.sadd(key, "1", "2", "3", "4", "5");
		RedisTestContext target = getContext(TARGET);
		JobExecution execution = runFlushing(redis, name, liveDataStructureReader(redis, name, 100),
				dataStructureWriter(target));
		log.info("Removing from set");
		sync.srem(key, "5");
		JobRunner.awaitTermination(execution);
		Awaitility.await().until(() -> sync.smembers(key).equals(target.sync().smembers(key)));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testReplication(RedisTestContext server) throws Exception {
		String name = "replication";
		execute(dataGenerator(server, name).end(100));
		RedisItemReader<String, KeyValue<String, byte[]>> reader = keyDumpReader(server, name);
		reader.setName(name(server, name + "-reader"));
		RedisTestContext target = getContext(TARGET);
		RedisItemWriter<String, String, KeyValue<String, byte[]>> writer = keyDumpWriter(target);
		run(server, name, reader, writer);
		compare(name, server, target);
	}

	@Test
	void testReplicationBinary() throws Exception {
		String name = "replication-binary";
		try (RedisTestContext server = new RedisTestContext(REDIS)) {
			RedisClient client = server.getRedisClient();
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
			RedisItemReader<String, KeyValue<String, byte[]>> reader = keyDumpReader(server, name);
			reader.setName(name(server, name + "-reader"));
			RedisTestContext target = getContext(TARGET);
			RedisItemWriter<String, String, KeyValue<String, byte[]>> writer = keyDumpWriter(target);
			run(server, name, reader, writer);
			compare(name, server, target);
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testLiveReplication(RedisTestContext server) throws Exception {
		String name = "live-replication";
		RedisTestContext target = getContext(TARGET);
		liveReplication(name, server, keyDumpReader(server, name), keyDumpWriter(target),
				liveKeyDumpReader(server, name, 100000), keyDumpWriter(target));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testLiveDSReplication(RedisTestContext server) throws Exception {
		String name = "live-ds-replication";
		RedisTestContext target = getContext(TARGET);
		liveReplication(name, server, dataStructureReader(server, name), dataStructureWriter(target),
				liveDataStructureReader(server, name, 100000), dataStructureWriter(target));
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
		Awaitility.await().until(liveReader::isOpen);
		Awaitility.await().until(liveWriter::isOpen);
		Thread.sleep(100);
		Generator.Builder liveGenerator = dataGenerator(server, "live-" + name).chunkSize(1)
				.types(Type.HASH, Type.LIST, Type.SET, Type.STRING, Type.ZSET).between(3000, 4000);
		execute(liveGenerator);
		JobRunner.awaitTermination(execution);
		awaitClosed(writer);
		awaitClosed(liveWriter);
		compare(name, server, getContext(TARGET));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testComparator(RedisTestContext server) throws Exception {
		String name = "comparator";
		execute(dataGenerator(server, name + "-source-init").end(120));
		RedisTestContext target = getContext(TARGET);
		execute(dataGenerator(target, name(server, name) + "-target-init").end(100));
		KeyComparator comparator = comparator(name, server, target).build();
		KeyComparisonResults results = comparator.call();
		Assertions.assertEquals(results.getSource(), results.getOK() + results.getMissing() + results.getValue());
		Assertions.assertEquals(120, results.getMissing());
		Assertions.assertEquals(300, results.getValue());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testScanSizeEstimator(RedisTestContext server) throws Exception {
		String pattern = "hash:*";
		execute(dataGenerator(server, "scan-size-estimator").end(12345).type(Type.HASH));
		long expectedCount = server.sync().keys(pattern).size();
		long matchCount = sizeEstimator(server).sampleSize(1000).match(pattern).build().call();
		Assertions.assertEquals(expectedCount, matchCount, expectedCount / 10);
		long typeSize = sizeEstimator(server).sampleSize(1000).type(Type.HASH.name().toLowerCase()).build().call();
		Assertions.assertEquals(expectedCount, typeSize, expectedCount / 10);
	}

	private Builder sizeEstimator(RedisTestContext server) {
		if (server.isCluster()) {
			return RedisScanSizeEstimator.client(server.getRedisClusterClient());
		}
		return RedisScanSizeEstimator.client(server.getRedisClient());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testGeneratorDefaults(RedisTestContext context) throws Exception {
		execute(dataGenerator(context, "defaults"));
		RedisModulesCommands<String, String> sync = context.sync();
		long expectedCount = Generator.DEFAULT_SEQUENCE.getMaximum() - Generator.DEFAULT_SEQUENCE.getMinimum();
		Awaitility.await().until(() -> expectedCount == sync.keys("string:*").size());
		Awaitility.await().until(() -> expectedCount * Generator.Type.values().length == sync.dbsize());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testGeneratorToOption(RedisTestContext context) throws Exception {
		int count = 123;
		execute(dataGenerator(context, "to-options").end(count));
		RedisModulesCommands<String, String> sync = context.sync();
		Awaitility.await().until(() -> count == sync.keys("string:*").size());
		Awaitility.await().until(() -> count * Generator.Type.values().length == sync.dbsize());
		Awaitility.await().until(() -> Generator.DEFAULT_COLLECTION_CARDINALITY.getMinimum() == sync.scard("set:100"));
		Awaitility.await().until(() -> Generator.DEFAULT_COLLECTION_CARDINALITY.getMinimum() == sync.llen("list:101"));
		Awaitility.await().until(() -> Generator.DEFAULT_COLLECTION_CARDINALITY.getMinimum() == sync.zcard("zset:102"));
		Awaitility.await()
				.until(() -> Generator.DEFAULT_COLLECTION_CARDINALITY.getMinimum() == sync.xlen("stream:103"));
	}

	@Test
	void testReaderSkipPolicy() throws Exception {
		String name = "skip-policy";
		List<Integer> items = IntStream.range(0, 100).boxed().collect(Collectors.toList());
		DelegatingPollableItemReader<Integer> reader = new DelegatingPollableItemReader<>(new ListItemReader<>(items));
		ListItemWriter<Integer> writer = new ListItemWriter<>();
		FlushingSimpleStepBuilder<Integer, Integer> stepBuilder = new FlushingSimpleStepBuilder<>(
				stepBuilderFactory.get(name).<Integer, Integer>chunk(1).reader(reader).writer(writer));
		stepBuilder.idleTimeout(Duration.ofMillis(100)).skip(TimeoutException.class)
				.skipPolicy(new AlwaysSkipItemSkipPolicy());
		launch(jobBuilderFactory.get(name).start(stepBuilder.build()).build());
		Awaitility.await().until(() -> items.size() == writer.getWrittenItems().size() * 2);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testHyperLogLogReplication(RedisTestContext server) throws Exception {
		String name = "testHyperLogLogReplication";
		String key1 = "hll:1";
		server.sync().pfadd(key1, "member:1", "member:2");
		String key2 = "hll:2";
		server.sync().pfadd(key2, "member:1", "member:2", "member:3");
		RedisItemReader<byte[], DataStructure<byte[]>> reader = binaryDataStructureReader(server, name);
		reader.setName(name(server, name + "-reader"));
		RedisTestContext target = getContext(TARGET);
		RedisItemWriter<byte[], byte[], DataStructure<byte[]>> writer = binaryDataStructureWriter(target);
		run(server, name, reader, writer);
		RedisModulesCommands<String, String> sourceSync = server.sync();
		RedisModulesCommands<String, String> targetSync = target.sync();
		Awaitility.await().until(() -> sourceSync.pfcount(key1) == targetSync.pfcount(key1));
	}

	protected static RedisItemWriter<byte[], byte[], DataStructure<byte[]>> binaryDataStructureWriter(
			RedisTestContext context) {
		return binaryWriter(context).dataStructure().build();
	}

	protected static OperationBuilder<byte[], byte[]> binaryWriter(RedisTestContext context) {
		if (context.isCluster()) {
			return RedisItemWriter.client(context.getRedisClusterClient(), ByteArrayCodec.INSTANCE);
		}
		return RedisItemWriter.client(context.getRedisClient(), ByteArrayCodec.INSTANCE);
	}

	protected RedisItemReader<byte[], DataStructure<byte[]>> binaryDataStructureReader(RedisTestContext redis,
			String name) throws Exception {
		return setName(
				configureJobRepository(new ScanRedisItemReaderBuilder<byte[], byte[], DataStructure<byte[]>>(
						redis.getClient(), ByteArrayCodec.INSTANCE, DataStructureValueReader::new)).build(),
				redis, name + "-data-structure");
	}

}
