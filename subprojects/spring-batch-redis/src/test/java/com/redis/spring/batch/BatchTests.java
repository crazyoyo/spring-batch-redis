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
import java.util.UUID;
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
import org.springframework.batch.item.ItemWriter;
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
	void testFlushingStep(RedisTestContext server) throws Exception {
		AbstractKeyspaceNotificationItemReader<String> reader = keyspaceNotificationReader(server);
		ListItemWriter<String> writer = new ListItemWriter<>();
		JobExecution execution = runFlushing(reader, writer);
		log.info("Keyspace-notification reader open={}", reader.isOpen());
		run(RandomDataStructureItemReader.builder().end(3).types(Type.STRING, Type.HASH).build(),
				dataStructureWriter(server));
		jobRunner.awaitTermination(execution);
		Assertions.assertEquals(server.sync().dbsize(), writer.getWrittenItems().size());
	}

	private AbstractKeyspaceNotificationItemReader<String> keyspaceNotificationReader(RedisTestContext server) {
		return reader(server).dataStructure().live().keyReader();
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testKeyspaceNotificationReader(RedisTestContext server) throws Exception {
		AbstractKeyspaceNotificationItemReader<String> keyReader = keyspaceNotificationReader(server);
		keyReader.open(new ExecutionContext());
		int count = 2;
		generate(server, RandomDataStructureItemReader.builder()
				.types(Type.HASH, Type.STRING, Type.LIST, Type.SET, Type.ZSET).end(count).build());
		Set<String> keys = new HashSet<>(readFully(keyReader));
		Assertions.assertEquals(new HashSet<>(server.sync().keys("*")), keys);
	}

	private <T> List<T> readFully(ItemReader<T> reader) throws Exception {
		if (reader instanceof ItemStream) {
			if (reader instanceof ItemStreamSupport) {
				((ItemStreamSupport) reader).setName(UUID.randomUUID().toString());
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

	@ParameterizedTest
	@RedisTestContextsSource
	void testDataStructureReader(RedisTestContext server) throws Exception {
		generate(server);
		Assertions.assertEquals(server.sync().dbsize(), readFully(dataStructureReader(server)).size());
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
	void testMultiThreadedReader(RedisTestContext server) throws Exception {
		String name = "multithreadedReader";
		generate(server);
		RedisItemReader<String, DataStructure<String>> reader = dataStructureReader(server);
		SynchronizedItemStreamReader<DataStructure<String>> synchronizedReader = new SynchronizedItemStreamReader<>();
		synchronizedReader.setDelegate(reader);
		synchronizedReader.afterPropertiesSet();
		SynchronizedListItemWriter<DataStructure<String>> writer = new SynchronizedListItemWriter<>();
		int threads = 4;
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setMaxPoolSize(threads);
		taskExecutor.setCorePoolSize(threads);
		taskExecutor.afterPropertiesSet();
		TaskletStep step = stepBuilderFactory.get(name + "-step")
				.<DataStructure<String>, DataStructure<String>>chunk(RedisItemReader.DEFAULT_CHUNK_SIZE)
				.reader(synchronizedReader).writer(writer).taskExecutor(taskExecutor).throttleLimit(threads).build();
		Job job = jobBuilderFactory.get(name(server, name)).start(step).build();
		jobRunner.run(job);
		Assertions.assertEquals(server.sync().dbsize(), writer.getWrittenItems().size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testLiveReader(RedisTestContext server) throws Exception {
		LiveRedisItemReader<String, KeyValue<String, byte[]>> reader = liveKeyDumpReader(server, 10000);
		ListItemWriter<KeyValue<String, byte[]>> writer = new ListItemWriter<>();
		JobExecution execution = runAsync(name(server, "livereader"), reader, writer);
		generate(server, RandomDataStructureItemReader.builder().end(123).types(Type.HASH, Type.STRING).build());
		jobRunner.awaitTermination(execution);
		Assertions.assertEquals(server.sync().dbsize(), writer.getWrittenItems().size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testKeyValueItemReaderFaultTolerance(RedisTestContext server) throws Exception {
		generate(server, RandomDataStructureItemReader.builder().types(Type.STRING).build());
		List<String> keys = IntStream.range(0, 100).boxed().map(i -> "string:" + i).collect(Collectors.toList());
		ExceptionThrowingRedisItemReaderBuilder<String, String, DataStructure<String>> builder = new ExceptionThrowingRedisItemReaderBuilder<>(
				server.getClient(), StringCodec.UTF8, ValueReaderFactory.dataStructure(), keys);
		builder.jobRunner(jobRunner);
		builder.valueQueueCapacity(1000);
		builder.skipPolicy(new AlwaysSkipItemSkipPolicy());
		ListItemWriter<DataStructure<String>> writer = new ListItemWriter<>();
		jobRunner.run(UUID.randomUUID().toString(), 1, builder.build(), writer);
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
		run(reader, dataStructureWriter(redis));
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
			jobRunner.awaitTermination(runFlushing(reader, writer));
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
			JobExecution execution1 = runFlushing(reader1, writer1);
			ListItemWriter<StreamMessage<String, String>> writer2 = new ListItemWriter<>();
			JobExecution execution2 = runFlushing(reader2, writer2);
			jobRunner.awaitTermination(execution1);
			jobRunner.awaitTermination(execution2);
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

	private StreamBuilder<String, String> streamReader(RedisTestContext server, String key) {
		return reader(server).stream(key);
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
		run(reader, writer);
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
	void testRedisItemWriterWait(RedisTestContext server) throws Exception {
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
				.waitForReplication(WaitForReplication.builder().replicas(1).timeout(Duration.ofMillis(300)).build())
				.build();
		Assertions.assertThrows(JobExecutionException.class, () -> run(reader, writer),
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
		run(reader, writer);
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
		run(reader, writer);
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
						.map(Map.Entry::getValue).build())
				.build();
		run(reader, writer);
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
		run(reader, writer);
		RedisModulesCommands<String, String> sync = server.sync();
		Assertions.assertEquals(1, sync.dbsize());
		Assertions.assertEquals(values.size(), sync.zcard("zset"));
		Assertions.assertEquals(60, sync
				.zrangebyscore("zset", Range.from(Range.Boundary.including(0), Range.Boundary.including(5))).size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testDataStructureWriter(RedisTestContext server) throws Exception {
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
		RedisItemWriter<String, String, DataStructure<String>> writer = dataStructureWriter(server);
		run(reader, writer);
		RedisModulesCommands<String, String> sync = server.sync();
		List<String> keys = sync.keys("hash:*");
		Assertions.assertEquals(count, keys.size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testDataStructureReplication(RedisTestContext server) throws Exception {
		RedisTestContext target = getContext(TARGET);
		generate(server, RandomDataStructureItemReader.builder().end(100).build());
		RedisItemReader<String, DataStructure<String>> reader = dataStructureReader(server);
		run(reader, replicationDataStructureWriter());
		compare(server, target);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testReplication(RedisTestContext server) throws Exception {
		generate(server, RandomDataStructureItemReader.builder().end(100).build());
		replicate(server);
		compare(server, getContext(TARGET));
	}

	private JobExecution replicate(RedisTestContext server) throws Exception {
		RedisItemReader<String, KeyValue<String, byte[]>> reader = keyDumpReader(server);
		RedisTestContext target = getContext(TARGET);
		RedisItemWriter<String, String, KeyValue<String, byte[]>> writer = keyDumpWriter(target);
		return run(reader, writer);
	}

	@Test
	void testReplicationBinary() throws Exception {
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
			RedisItemReader<String, KeyValue<String, byte[]>> reader = keyDumpReader(server);
			RedisTestContext target = getContext(TARGET);
			RedisItemWriter<String, String, KeyValue<String, byte[]>> writer = keyDumpWriter(target);
			run(reader, writer);
			compare(server, target);
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testLiveReplication(RedisTestContext server) throws Exception {
		RedisTestContext target = getContext(TARGET);
		liveReplication(server, keyDumpReader(server), keyDumpWriter(target), liveKeyDumpReader(server, 100000),
				keyDumpWriter(target));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testLiveDSSetReplication(RedisTestContext redis) throws Exception {
		String key = "myset";
		redis.sync().sadd(key, "1", "2", "3", "4", "5");
		LiveRedisItemReader<String, DataStructure<String>> reader = liveDataStructureReader(redis, 100);
		RedisItemWriter<String, String, DataStructure<String>> writer = replicationDataStructureWriter();
		JobExecution execution = runAsync(reader, writer);
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
	void testLiveDSReplication(RedisTestContext server) throws Exception {
		liveReplication(server, dataStructureReader(server), replicationDataStructureWriter(),
				liveDataStructureReader(server, 100000), replicationDataStructureWriter());
	}

	private RedisItemWriter<String, String, DataStructure<String>> replicationDataStructureWriter() {
		return writer(getContext(TARGET)).string().dataStructure().xaddArgs(m -> new XAddArgs().id(m.getId())).build();
	}

	private <T extends KeyValue<String, ?>> void liveReplication(RedisTestContext server,
			RedisItemReader<String, T> reader, RedisItemWriter<String, String, T> writer,
			LiveRedisItemReader<String, T> liveReader, RedisItemWriter<String, String, T> liveWriter) throws Exception {
		String name = UUID.randomUUID().toString();
		generate(server, RandomDataStructureItemReader.builder().end(3000).build());
		TaskletStep step = jobRunner
				.step(UUID.randomUUID().toString(), RedisItemReader.DEFAULT_CHUNK_SIZE, reader, writer).build();
		SimpleFlow flow = jobRunner.flow("flow-" + name).start(step).build();
		liveReader.setIdleTimeout(DEFAULT_IDLE_TIMEOUT);
		TaskletStep liveStep = new FlushingSimpleStepBuilder<>(jobRunner.step(UUID.randomUUID().toString(),
				RedisItemReader.DEFAULT_CHUNK_SIZE, liveReader, liveWriter)).idleTimeout(DEFAULT_IDLE_TIMEOUT).build();
		SimpleFlow liveFlow = jobRunner.flow("live-" + name).start(liveStep).build();
		Job job = jobBuilderFactory.get(name)
				.start(jobRunner.flow(name).split(new SimpleAsyncTaskExecutor()).add(liveFlow, flow).build()).build()
				.build();
		JobExecution execution = jobRunner.runAsync(job);
		awaitOpen(liveReader);
		awaitOpen(liveWriter);
		generate(server, RandomDataStructureItemReader.builder()
				.types(Type.HASH, Type.LIST, Type.SET, Type.STRING, Type.ZSET).between(3000, 4000).build());
		jobRunner.awaitTermination(execution);
		awaitClosed(writer);
		awaitClosed(liveWriter);
		compare(server, getContext(TARGET));
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

	@ParameterizedTest
	@RedisTestContextsSource
	void testGeneratorDefaults(RedisTestContext server) throws Exception {
		generate(server);
		long expectedCount = RandomDataStructureItemReader.DEFAULT_SEQUENCE.getMaximum()
				- RandomDataStructureItemReader.DEFAULT_SEQUENCE.getMinimum();
		Assertions.assertEquals(expectedCount, server.sync().dbsize());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testGeneratorToOption(RedisTestContext server) throws Exception {
		int count = 123;
		generate(server, RandomDataStructureItemReader.builder().end(count).build());
		Assertions.assertEquals(count, server.sync().dbsize());
		ScanIterator<String> setIterator = ScanIterator.scan(server.sync(),
				KeyScanArgs.Builder.type(Type.SET.getString()));
		while (setIterator.hasNext()) {
			Assertions.assertEquals(RandomDataStructureItemReader.DEFAULT_COLLECTION_CARDINALITY.getMinimum(),
					Math.toIntExact(server.sync().scard(setIterator.next())));
		}
		ScanIterator<String> listIterator = ScanIterator.scan(server.sync(),
				KeyScanArgs.Builder.type(Type.LIST.getString()));
		while (listIterator.hasNext()) {
			Assertions.assertEquals(RandomDataStructureItemReader.DEFAULT_COLLECTION_CARDINALITY.getMinimum(),
					Math.toIntExact(server.sync().llen(listIterator.next())));
		}
		ScanIterator<String> zsetIterator = ScanIterator.scan(server.sync(),
				KeyScanArgs.Builder.type(Type.ZSET.getString()));
		while (zsetIterator.hasNext()) {
			Assertions.assertEquals(RandomDataStructureItemReader.DEFAULT_COLLECTION_CARDINALITY.getMinimum(),
					Math.toIntExact(server.sync().zcard(zsetIterator.next())));
		}
		ScanIterator<String> streamIterator = ScanIterator.scan(server.sync(),
				KeyScanArgs.Builder.type(Type.STREAM.getString()));
		while (streamIterator.hasNext()) {
			Assertions.assertEquals(RandomDataStructureItemReader.DEFAULT_COLLECTION_CARDINALITY.getMinimum(),
					Math.toIntExact(server.sync().xlen(streamIterator.next())));
		}
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
	void testHyperLogLogReplication(RedisTestContext server) throws Exception {
		String key1 = "hll:1";
		server.sync().pfadd(key1, "member:1", "member:2");
		String key2 = "hll:2";
		server.sync().pfadd(key2, "member:1", "member:2", "member:3");
		RedisItemReader<byte[], DataStructure<byte[]>> reader = binaryDataStructureReader(server);
		RedisTestContext target = getContext(TARGET);
		RedisItemWriter<byte[], byte[], DataStructure<byte[]>> writer = binaryDataStructureWriter(target);
		run(reader, writer);
		RedisModulesCommands<String, String> sourceSync = server.sync();
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

}
