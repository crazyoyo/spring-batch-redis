package com.redis.spring.batch;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.spring.batch.DataStructure.Type;
import com.redis.spring.batch.RedisItemReader.StreamBuilder;
import com.redis.spring.batch.RedisItemWriter.OperationBuilder;
import com.redis.spring.batch.RedisItemWriter.WaitForReplication;
import com.redis.spring.batch.RedisScanSizeEstimator.Builder;
import com.redis.spring.batch.compare.KeyComparator;
import com.redis.spring.batch.compare.KeyComparisonLogger;
import com.redis.spring.batch.compare.KeyComparisonResults;
import com.redis.spring.batch.convert.GeoValueConverter;
import com.redis.spring.batch.convert.ScoredValueConverter;
import com.redis.spring.batch.reader.AbstractKeyspaceNotificationItemReader;
import com.redis.spring.batch.reader.AbstractValueReader.ValueReaderFactory;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.StreamItemReader;
import com.redis.spring.batch.reader.StreamItemReader.AckPolicy;
import com.redis.spring.batch.step.FlushingSimpleStepBuilder;
import com.redis.spring.batch.support.RandomDataStructureItemReader;
import com.redis.spring.batch.writer.operation.Geoadd;
import com.redis.spring.batch.writer.operation.Hset;
import com.redis.spring.batch.writer.operation.Xadd;
import com.redis.spring.batch.writer.operation.Zadd;
import com.redis.testcontainers.RedisClusterContainer;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.GeoArgs;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

class BatchTests extends AbstractTestBase {

	private static final Logger log = LoggerFactory.getLogger(BatchTests.class);

	protected static final RedisContainer REDIS = new RedisContainer(
			RedisContainer.DEFAULT_IMAGE_NAME.withTag(RedisContainer.DEFAULT_TAG)).withKeyspaceNotifications();
	protected static final RedisClusterContainer REDIS_CLUSTER = new RedisClusterContainer(
			RedisClusterContainer.DEFAULT_IMAGE_NAME.withTag(RedisClusterContainer.DEFAULT_TAG))
			.withKeyspaceNotifications();
	private static final RedisContainer TARGET = new RedisContainer(
			RedisContainer.DEFAULT_IMAGE_NAME.withTag(RedisContainer.DEFAULT_TAG));

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
	void testFlushingStep(RedisTestContext redis) throws Exception {
		AbstractKeyspaceNotificationItemReader<String> reader = keyspaceNotificationReader(redis);
		ListItemWriter<String> writer = new ListItemWriter<>();
		JobExecution execution = runFlushing(redis, reader, writer);
		log.info("Keyspace-notification reader open={}", reader.isOpen());
		run(name(redis) + "-gen", RandomDataStructureItemReader.builder().end(3).types(Type.STRING, Type.HASH).build(),
				dataStructureWriter(redis));
		jobRunner.awaitTermination(execution);
		Awaitility.await().until(() -> !reader.isOpen());
		Assertions.assertEquals(redis.sync().dbsize(), writer.getWrittenItems().size());
	}

	private AbstractKeyspaceNotificationItemReader<String> keyspaceNotificationReader(RedisTestContext server) {
		return reader(server).dataStructure().live().keyReader();
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testKeyspaceNotificationReader(RedisTestContext redis) throws Exception {
		AbstractKeyspaceNotificationItemReader<String> keyReader = keyspaceNotificationReader(redis);
		keyReader.open(new ExecutionContext());
		int count = 2;
		generate(redis, RandomDataStructureItemReader.builder()
				.types(Type.HASH, Type.STRING, Type.LIST, Type.SET, Type.ZSET).end(count).build());
		Set<String> keys = new HashSet<>(readFully(redis, keyReader));
		Assertions.assertEquals(new HashSet<>(redis.sync().keys("*")), keys);
	}

	private <T> List<T> readFully(RedisTestContext redis, ItemReader<T> reader) throws Exception {
		if (reader instanceof ItemStream) {
			if (reader instanceof ItemStreamSupport) {
				((ItemStreamSupport) reader).setName(name(redis) + "-reader");
			}
			((ItemStream) reader).open(new ExecutionContext());
		}
		List<T> list = new ArrayList<>();
		T value;
		while ((value = reader.read()) != null) {
			list.add(value);
		}
		if (reader instanceof ItemStream) {
			((ItemStream) reader).close();
		}
		return list;
	}

	private static class SynchronizedListItemWriter<T> extends AbstractItemStreamItemWriter<T> {

		private List<T> writtenItems = new ArrayList<>();
		private boolean open;

		@Override
		public synchronized void write(List<? extends T> items) throws Exception {
			writtenItems.addAll(items);
		}

		public List<? extends T> getWrittenItems() {
			return this.writtenItems;
		}

		@Override
		public void open(ExecutionContext executionContext) {
			super.open(executionContext);
			this.open = true;
		}

		@Override
		public void close() {
			super.close();
			this.open = false;
		}

		public boolean isOpen() {
			return open;
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testMultiThreadedReader(RedisTestContext redis) throws Exception {
		generate(redis);
		RedisItemReader<String, DataStructure<String>> reader = dataStructureReader(redis);
		reader.setName(name(redis) + "-reader");
		SynchronizedItemStreamReader<DataStructure<String>> synchronizedReader = new SynchronizedItemStreamReader<>();
		synchronizedReader.setDelegate(reader);
		synchronizedReader.afterPropertiesSet();
		SynchronizedListItemWriter<DataStructure<String>> writer = new SynchronizedListItemWriter<>();
		int threads = 4;
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setMaxPoolSize(threads);
		taskExecutor.setCorePoolSize(threads);
		taskExecutor.afterPropertiesSet();
		TaskletStep step = stepBuilderFactory.get(name(redis) + "-step")
				.<DataStructure<String>, DataStructure<String>>chunk(RedisItemReader.DEFAULT_CHUNK_SIZE)
				.reader(synchronizedReader).writer(writer).taskExecutor(taskExecutor).throttleLimit(threads).build();
		jobRunner.run(jobBuilderFactory.get(name(redis) + "-job").start(step).build());
		Awaitility.await().until(() -> !reader.isOpen());
		Awaitility.await().until(() -> !writer.isOpen());
		Assertions.assertEquals(redis.sync().dbsize(), writer.getWrittenItems().size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testLiveReader(RedisTestContext redis) throws Exception {
		LiveRedisItemReader<String, KeyValue<String, byte[]>> reader = liveKeyDumpReader(redis, 10000);
		ListItemWriter<KeyValue<String, byte[]>> writer = new ListItemWriter<>();
		JobExecution execution = runAsync(redis, reader, writer);
		generate(redis, RandomDataStructureItemReader.builder().end(123).types(Type.HASH, Type.STRING).build());
		jobRunner.awaitTermination(execution);
		Assertions.assertEquals(redis.sync().dbsize(), writer.getWrittenItems().size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testKeyValueItemReaderFaultTolerance(RedisTestContext redis) throws Exception {
		generate(redis, RandomDataStructureItemReader.builder().types(Type.STRING).build());
		List<String> keys = IntStream.range(0, 100).boxed().map(i -> "string:" + i).collect(Collectors.toList());
		ExceptionThrowingRedisItemReaderBuilder<String, String, DataStructure<String>> builder = new ExceptionThrowingRedisItemReaderBuilder<>(
				redis.getClient(), StringCodec.UTF8, ValueReaderFactory.dataStructure(), keys);
		builder.jobRunner(jobRunner);
		builder.valueQueueCapacity(1000);
		builder.skipPolicy(new AlwaysSkipItemSkipPolicy());
		ListItemWriter<DataStructure<String>> writer = new ListItemWriter<>();
		jobRunner.run(name(redis), 1, builder.build(), writer);
		Assertions.assertEquals(50, writer.getWrittenItems().size());
	}

	private static class ExceptionThrowingRedisItemReaderBuilder<K, V, T extends KeyValue<K, ?>>
			extends RedisItemReader.Builder<K, V, T> {

		private final List<K> keys;

		public ExceptionThrowingRedisItemReaderBuilder(AbstractRedisClient client, RedisCodec<K, V> codec,
				ValueReaderFactory<K, V, T> valueReaderFactory, List<K> keys) {
			super(client, codec, valueReaderFactory);
			this.keys = keys;
		}

		@Override
		protected ItemReader<K> keyReader() {
			return new ExceptionThrowingPollableItemReader<>(new ListItemReader<>(keys));
		}

	}

	private void assertMessageBody(List<? extends StreamMessage<String, String>> items) {
		for (StreamMessage<String, String> message : items) {
			Assertions.assertTrue(message.getBody().containsKey("field1"));
			Assertions.assertTrue(message.getBody().containsKey("field2"));
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testStreamReader(RedisTestContext redis) throws Exception {
		generateStreams(redis);
		List<String> keys = ScanIterator.scan(redis.sync(), KeyScanArgs.Builder.type(Type.STREAM.getString())).stream()
				.collect(Collectors.toList());
		for (String key : keys) {
			StreamItemReader<String, String> reader = streamReader(redis, key).build();
			reader.open(new ExecutionContext());
			List<StreamMessage<String, String>> messages = reader.readMessages();
			Assertions.assertEquals(StreamItemReader.DEFAULT_COUNT, messages.size());
			assertMessageBody(messages);
		}
	}

	private static final int COUNT = 100;

	private void generateStreams(RedisTestContext redis) throws JobExecutionException {
		generate(redis, RandomDataStructureItemReader.builder().types(Type.STREAM)
				.collectionCardinality(com.redis.spring.batch.support.Range.is(COUNT)).end(1).build());
	}

	private void generate(RedisTestContext redis, ItemReader<DataStructure<String>> reader)
			throws JobExecutionException {
		generate(name(redis), redis, reader);
	}

	private void generate(String id, RedisTestContext redis, ItemReader<DataStructure<String>> reader)
			throws JobExecutionException {
		run(id + "-generate", reader, dataStructureWriter(redis));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testStreamReaderJob(RedisTestContext redis) throws Exception {
		generateStreams(redis);
		List<String> keys = ScanIterator.scan(redis.sync(), KeyScanArgs.Builder.type(Type.STREAM.getString())).stream()
				.collect(Collectors.toList());
		for (String key : keys) {
			Assertions.assertEquals(COUNT, redis.sync().xlen(key));
			StreamItemReader<String, String> reader = streamReader(redis, key).build();
			ListItemWriter<StreamMessage<String, String>> writer = new ListItemWriter<>();
			JobExecution execution = runFlushing(name(redis) + "-" + key, reader, writer);
			jobRunner.awaitTermination(execution);
			Awaitility.await().until(() -> !reader.isOpen());
			Assertions.assertEquals(COUNT, writer.getWrittenItems().size());
			assertMessageBody(writer.getWrittenItems());
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testMultipleStreamReaders(RedisTestContext redis) throws Exception {
		String consumerGroup = "consumerGroup";
		generateStreams(redis);
		final List<String> keys = ScanIterator.scan(redis.sync(), KeyScanArgs.Builder.type(Type.STREAM.getString()))
				.stream().collect(Collectors.toList());
		for (String key : keys) {
			StreamItemReader<String, String> reader1 = streamReader(redis, key).consumerGroup(consumerGroup)
					.consumer("consumer1").ackPolicy(AckPolicy.MANUAL).build();
			StreamItemReader<String, String> reader2 = streamReader(redis, key).consumerGroup(consumerGroup)
					.consumer("consumer2").ackPolicy(AckPolicy.MANUAL).build();
			ListItemWriter<StreamMessage<String, String>> writer1 = new ListItemWriter<>();
			JobExecution execution1 = runFlushing(name(redis) + "-reader1", reader1, writer1);
			ListItemWriter<StreamMessage<String, String>> writer2 = new ListItemWriter<>();
			JobExecution execution2 = runFlushing(name(redis) + "-reader2", reader2, writer2);
			jobRunner.awaitTermination(execution1);
			jobRunner.awaitTermination(execution2);
			Awaitility.await().until(() -> !reader1.isOpen());
			Awaitility.await().until(() -> !reader2.isOpen());
			Assertions.assertEquals(COUNT, writer1.getWrittenItems().size() + writer2.getWrittenItems().size());
			assertMessageBody(writer1.getWrittenItems());
			assertMessageBody(writer2.getWrittenItems());
			RedisModulesCommands<String, String> sync = redis.sync();
			Assertions.assertEquals(COUNT, sync.xpending(key, consumerGroup).getCount());
			reader1.open(new ExecutionContext());
			reader1.ack(writer1.getWrittenItems());
			reader2.open(new ExecutionContext());
			reader2.ack(writer2.getWrittenItems());
			Assertions.assertEquals(0, sync.xpending(key, consumerGroup).getCount());
		}
	}

	private StreamBuilder<String, String> streamReader(RedisTestContext redis, String key) {
		return reader(redis).stream(key);
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
		run(redis, reader, writer);
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
	void testRedisItemWriterWait(RedisTestContext redis) throws Exception {
		List<Map<String, String>> maps = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			Map<String, String> body = new HashMap<>();
			body.put("id", String.valueOf(index));
			body.put("field1", "value1");
			body.put("field2", "value2");
			maps.add(body);
		}
		ListItemReader<Map<String, String>> reader = new ListItemReader<>(maps);
		RedisItemWriter<String, String, Map<String, String>> writer = operationWriter(redis,
				Hset.<String, String, Map<String, String>>key(m -> "hash:" + m.remove("id")).map(m -> m).build())
				.waitForReplication(WaitForReplication.builder().replicas(1).timeout(Duration.ofMillis(300)).build())
				.build();
		Assertions.assertThrows(JobExecutionException.class, () -> run(redis, reader, writer),
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
	void testHashWriter(RedisTestContext redis) throws Exception {
		List<Map<String, String>> maps = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			Map<String, String> body = new HashMap<>();
			body.put("id", String.valueOf(index));
			body.put("field1", "value1");
			body.put("field2", "value2");
			maps.add(body);
		}
		ListItemReader<Map<String, String>> reader = new ListItemReader<>(maps);
		RedisItemWriter<String, String, Map<String, String>> writer = operationWriter(redis,
				Hset.<String, String, Map<String, String>>key(m -> "hash:" + m.remove("id")).map(m -> m).build())
				.build();
		run(redis, reader, writer);
		Assertions.assertEquals(maps.size(), redis.sync().keys("hash:*").size());
		for (int index = 0; index < maps.size(); index++) {
			Map<String, String> hash = redis.sync().hgetall("hash:" + index);
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
		run(redis, reader, writer);
		Set<String> radius1 = redis.sync().georadius("geoset", -118, 34, 100, GeoArgs.Unit.mi);
		Assertions.assertEquals(1, radius1.size());
		Assertions.assertTrue(radius1.contains("Venice Breakwater"));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testHashDelWriter(RedisTestContext redis) throws Exception {
		List<Entry<String, Map<String, String>>> hashes = new ArrayList<>();
		RedisModulesCommands<String, String> sync = redis.sync();
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
		RedisItemWriter<String, String, Map.Entry<String, Map<String, String>>> writer = operationWriter(redis,
				Hset.<String, String, Entry<String, Map<String, String>>>key(e -> "hash:" + e.getKey())
						.map(Map.Entry::getValue).build())
				.build();
		run(redis, reader, writer);
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
	void testSortedSetWriter(RedisTestContext redis) throws Exception {
		List<ZValue> values = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			values.add(new ZValue(String.valueOf(index), index % 10));
		}
		ListItemReader<ZValue> reader = new ListItemReader<>(values);
		ScoredValueConverter<String, ZValue> converter = new ScoredValueConverter<>(ZValue::getMember,
				ZValue::getScore);
		RedisItemWriter<String, String, ZValue> writer = operationWriter(redis,
				Zadd.<String, String, ZValue>key("zset").value(converter).build()).build();
		run(redis, reader, writer);
		RedisModulesCommands<String, String> sync = redis.sync();
		Assertions.assertEquals(1, sync.dbsize());
		Assertions.assertEquals(values.size(), sync.zcard("zset"));
		Assertions.assertEquals(60, sync
				.zrangebyscore("zset", Range.from(Range.Boundary.including(0), Range.Boundary.including(5))).size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testDataStructureWriter(RedisTestContext redis) throws Exception {
		List<DataStructure<String>> list = new ArrayList<>();
		long count = 100;
		for (int index = 0; index < count; index++) {
			Map<String, String> map = new HashMap<>();
			map.put("field1", "value1");
			map.put("field2", "value2");
			DataStructure<String> ds = new DataStructure<>();
			ds.setKey(Type.HASH + ":" + index);
			ds.setType(Type.HASH);
			ds.setValue(map);
			list.add(ds);
		}
		ListItemReader<DataStructure<String>> reader = new ListItemReader<>(list);
		RedisItemWriter<String, String, DataStructure<String>> writer = dataStructureWriter(redis);
		run(redis, reader, writer);
		Awaitility.await().until(() -> !writer.isOpen());
		RedisModulesCommands<String, String> sync = redis.sync();
		List<String> keys = sync.keys("hash:*");
		Assertions.assertEquals(count, keys.size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testDataStructureReplication(RedisTestContext redis) throws Exception {
		RedisTestContext target = getContext(TARGET);
		generate(redis, RandomDataStructureItemReader.builder().end(100).build());
		RedisItemReader<String, DataStructure<String>> reader = dataStructureReader(redis);
		run(redis, reader, replicationDataStructureWriter());
		compare(redis, target);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testReplication(RedisTestContext redis) throws Exception {
		generate(redis, RandomDataStructureItemReader.builder().end(100).build());
		replicate(redis);
		compare(redis, getContext(TARGET));
	}

	private JobExecution replicate(RedisTestContext redis) throws Exception {
		RedisItemReader<String, KeyValue<String, byte[]>> reader = keyDumpReader(redis);
		RedisTestContext target = getContext(TARGET);
		RedisItemWriter<String, String, KeyValue<String, byte[]>> writer = keyDumpWriter(target);
		return run(redis, reader, writer);
	}

	@Test
	void testReplicationBinary() throws Exception {
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
			RedisItemReader<String, KeyValue<String, byte[]>> reader = keyDumpReader(redis);
			RedisTestContext target = getContext(TARGET);
			RedisItemWriter<String, String, KeyValue<String, byte[]>> writer = keyDumpWriter(target);
			run(redis, reader, writer);
			compare(redis, target);
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testLiveReplication(RedisTestContext redis) throws Exception {
		RedisTestContext target = getContext(TARGET);
		liveReplication(redis, keyDumpReader(redis), keyDumpWriter(target), liveKeyDumpReader(redis, 100000),
				keyDumpWriter(target));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testLiveDSSetReplication(RedisTestContext redis) throws Exception {
		String key = "myset";
		redis.sync().sadd(key, "1", "2", "3", "4", "5");
		LiveRedisItemReader<String, DataStructure<String>> reader = liveDataStructureReader(redis, 100);
		RedisItemWriter<String, String, DataStructure<String>> writer = replicationDataStructureWriter();
		JobExecution execution = runAsync(redis, reader, writer);
		awaitOpen(reader);
		awaitOpen(writer);
		log.info("Removing from set");
		redis.sync().srem(key, "5");
		jobRunner.awaitTermination(execution);
		awaitClosed(writer);
		Assertions.assertEquals(redis.sync().smembers(key), getContext(TARGET).sync().smembers(key));
	}

	private void awaitOpen(LiveRedisItemReader<?, ?> reader) {
		Awaitility.await().until(reader::isOpen);
	}

	private void awaitOpen(RedisItemWriter<?, ?, ?> writer) {
		Awaitility.await().until(writer::isOpen);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testLiveDSReplication(RedisTestContext redis) throws Exception {
		liveReplication(redis, dataStructureReader(redis), replicationDataStructureWriter(),
				liveDataStructureReader(redis, 100000), replicationDataStructureWriter());
	}

	private RedisItemWriter<String, String, DataStructure<String>> replicationDataStructureWriter() {
		return writer(getContext(TARGET)).string().dataStructure().xaddArgs(m -> new XAddArgs().id(m.getId())).build();
	}

	private <T extends KeyValue<String, ?>> void liveReplication(RedisTestContext redis,
			RedisItemReader<String, T> reader, RedisItemWriter<String, String, T> writer,
			LiveRedisItemReader<String, T> liveReader, RedisItemWriter<String, String, T> liveWriter) throws Exception {
		String name = name(redis);
		generate(redis, RandomDataStructureItemReader.builder().end(3000).build());
		TaskletStep step = jobRunner.step(name + "-snapshot-step", RedisItemReader.DEFAULT_CHUNK_SIZE, reader, writer)
				.build();
		SimpleFlow flow = jobRunner.flow(name + "-snapshot-flow").start(step).build();
		liveReader.setIdleTimeout(DEFAULT_IDLE_TIMEOUT);
		TaskletStep liveStep = new FlushingSimpleStepBuilder<>(
				jobRunner.step(name + "-live-step", RedisItemReader.DEFAULT_CHUNK_SIZE, liveReader, liveWriter))
				.idleTimeout(DEFAULT_IDLE_TIMEOUT).build();
		SimpleFlow liveFlow = jobRunner.flow(name + "-live-flow").start(liveStep).build();
		Job job = jobBuilderFactory.get(name + "-job")
				.start(jobRunner.flow(name + "-flow").split(new SimpleAsyncTaskExecutor()).add(liveFlow, flow).build())
				.build().build();
		JobExecution execution = jobRunner.runAsync(job);
		awaitOpen(liveReader);
		awaitOpen(liveWriter);
		generate(name(redis) + "-generate-2", redis, RandomDataStructureItemReader.builder()
				.types(Type.HASH, Type.LIST, Type.SET, Type.STRING, Type.ZSET).between(3000, 4000).build());
		jobRunner.awaitTermination(execution);
		awaitClosed(writer);
		awaitClosed(liveWriter);
		compare(redis, getContext(TARGET));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testComparator(RedisTestContext server) throws Exception {
		int sourceEnd = 120;
		generate(server, RandomDataStructureItemReader.builder().end(sourceEnd).build());
		replicate(server);
		RedisTestContext target = getContext(TARGET);
		long deleteCount = 13;
		for (int index = 0; index < deleteCount; index++) {
			target.sync().del(target.sync().randomkey());
		}
		Set<String> ttlChanges = new HashSet<>();
		for (int index = 0; index < 23; index++) {
			String key = target.sync().randomkey();
			long ttl = target.sync().ttl(key) + 12345;
			if (target.sync().expire(key, ttl)) {
				ttlChanges.add(key);
			}
		}
		Set<String> typeChanges = new HashSet<>();
		Set<String> valueChanges = new HashSet<>();
		for (int index = 0; index < 17; index++) {
			String key = target.sync().randomkey();
			String type = target.sync().type(key);
			if (type.equals(Type.STRING.getString())) {
				if (!typeChanges.contains(key)) {
					valueChanges.add(key);
				}
				ttlChanges.remove(key);
			} else {
				typeChanges.add(key);
				valueChanges.remove(key);
				ttlChanges.remove(key);
			}
			target.sync().set(key, "blah");
		}
		KeyComparator comparator = comparator(server, getContext(TARGET)).build();
		comparator.addListener(new KeyComparisonLogger());
		KeyComparisonResults results = comparator.call();
		Assertions.assertEquals(results.getSource() - deleteCount, results.getTarget());
		Assertions.assertEquals(deleteCount, results.getMissing());
		Assertions.assertEquals(typeChanges.size(), results.getType());
		Assertions.assertEquals(valueChanges.size(), results.getValue());
		Assertions.assertEquals(ttlChanges.size(), results.getTTL());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testScanSizeEstimator(RedisTestContext server) throws Exception {
		String pattern = RandomDataStructureItemReader.DEFAULT_KEYSPACE + "*";
		int count = 12345;
		generate(server, RandomDataStructureItemReader.builder().end(count).build());
		long expectedCount = server.sync().dbsize();
		Assertions.assertEquals(expectedCount, sizeEstimator(server).sampleSize(1000).match(pattern).build().call(),
				expectedCount / 10);
		Assertions.assertEquals(expectedCount / RandomDataStructureItemReader.defaultTypes().size(),
				sizeEstimator(server).sampleSize(1000).type(Type.HASH).build().call(), expectedCount / 10);
	}

	private Builder sizeEstimator(RedisTestContext server) {
		return RedisScanSizeEstimator.client(server.getClient());
	}

	@Test
	void testReaderSkipPolicy() throws Exception {
		String name = "skip-policy";
		List<Integer> items = IntStream.range(0, 100).boxed().collect(Collectors.toList());
		ExceptionThrowingPollableItemReader<Integer> reader = new ExceptionThrowingPollableItemReader<>(
				new ListItemReader<>(items));
		ListItemWriter<Integer> writer = new ListItemWriter<>();
		FlushingSimpleStepBuilder<Integer, Integer> stepBuilder = new FlushingSimpleStepBuilder<>(
				stepBuilderFactory.get(name).<Integer, Integer>chunk(1).reader(reader).writer(writer));
		stepBuilder.idleTimeout(Duration.ofMillis(100)).skip(TimeoutException.class)
				.skipPolicy(new AlwaysSkipItemSkipPolicy());
		jobRunner.run(jobBuilderFactory.get(name).start(stepBuilder.build()).build());
		Assertions.assertEquals(items.size(), writer.getWrittenItems().size() * 2);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testHyperLogLogReplication(RedisTestContext redis) throws Exception {
		String key1 = "hll:1";
		redis.sync().pfadd(key1, "member:1", "member:2");
		String key2 = "hll:2";
		redis.sync().pfadd(key2, "member:1", "member:2", "member:3");
		RedisItemReader<byte[], DataStructure<byte[]>> reader = binaryDataStructureReader(redis);
		RedisTestContext target = getContext(TARGET);
		RedisItemWriter<byte[], byte[], DataStructure<byte[]>> writer = binaryDataStructureWriter(target);
		run(redis, reader, writer);
		RedisModulesCommands<String, String> sourceSync = redis.sync();
		RedisModulesCommands<String, String> targetSync = target.sync();
		Assertions.assertEquals(sourceSync.pfcount(key1), targetSync.pfcount(key1));
	}

	protected static RedisItemWriter<byte[], byte[], DataStructure<byte[]>> binaryDataStructureWriter(
			RedisTestContext server) {
		return binaryWriter(server).dataStructure().build();
	}

	protected static OperationBuilder<byte[], byte[]> binaryWriter(RedisTestContext server) {
		return RedisItemWriter.client(server.getClient()).bytes();
	}

	protected RedisItemReader<byte[], DataStructure<byte[]>> binaryDataStructureReader(RedisTestContext redis)
			throws Exception {
		return RedisItemReader.client(redis.getClient()).bytes().dataStructure().jobRunner(jobRunner).build();
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testDataStructureReader(RedisTestContext redis) throws Exception {
		generate(redis);
		Assertions.assertEquals(redis.sync().dbsize(), readFully(redis, dataStructureReader(redis)).size());
	}

}
