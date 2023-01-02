package com.redis.spring.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;
import org.springframework.util.unit.DataSize;
import org.springframework.util.unit.DataUnit;

import com.redis.enterprise.Database;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.DataStructure.Type;
import com.redis.spring.batch.common.IntRange;
import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.StepOptions;
import com.redis.spring.batch.convert.GeoValueConverter;
import com.redis.spring.batch.convert.IdentityConverter;
import com.redis.spring.batch.convert.ScoredValueConverter;
import com.redis.spring.batch.reader.DataGeneratorItemReader;
import com.redis.spring.batch.reader.DataGeneratorOptions;
import com.redis.spring.batch.reader.DataGeneratorOptions.SetOptions;
import com.redis.spring.batch.reader.DataGeneratorOptions.StreamOptions;
import com.redis.spring.batch.reader.HotKeyFilter;
import com.redis.spring.batch.reader.HotKeyFilter.KeyContext;
import com.redis.spring.batch.reader.HotKeyFilterOptions;
import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.PollableItemReader;
import com.redis.spring.batch.reader.ScanSizeEstimator;
import com.redis.spring.batch.reader.ScanSizeEstimator.Builder;
import com.redis.spring.batch.reader.ScanSizeEstimatorOptions;
import com.redis.spring.batch.reader.SlotRangeFilter;
import com.redis.spring.batch.reader.StreamItemReader;
import com.redis.spring.batch.reader.StreamReaderBuilder;
import com.redis.spring.batch.reader.StreamReaderOptions;
import com.redis.spring.batch.reader.StreamReaderOptions.AckPolicy;
import com.redis.spring.batch.step.FlushingSimpleStepBuilder;
import com.redis.spring.batch.writer.KeyComparisonCountItemWriter;
import com.redis.spring.batch.writer.KeyComparisonCountItemWriter.Results;
import com.redis.spring.batch.writer.WaitForReplication;
import com.redis.spring.batch.writer.WriterOptions;
import com.redis.spring.batch.writer.operation.Geoadd;
import com.redis.spring.batch.writer.operation.Hset;
import com.redis.spring.batch.writer.operation.Xadd;
import com.redis.spring.batch.writer.operation.Zadd;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisEnterpriseContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;
import com.redis.testcontainers.junit.RedisTestInstance;

import io.lettuce.core.Consumer;
import io.lettuce.core.GeoArgs;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisSortedSetCommands;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.models.stream.PendingMessages;

class RedisIntegrationTests extends AbstractTestBase {

	private static final Logger log = LoggerFactory.getLogger(RedisIntegrationTests.class);

	protected static final RedisContainer REDIS = new RedisContainer(
			RedisContainer.DEFAULT_IMAGE_NAME.withTag(RedisContainer.DEFAULT_TAG));
	protected static final RedisEnterpriseContainer REDIS_ENTERPRISE = new RedisEnterpriseContainer(
			RedisEnterpriseContainer.DEFAULT_IMAGE_NAME.withTag("latest"))
			.withDatabase(Database.name("BatchTests").memory(DataSize.ofMegabytes(50)).ossCluster(true).build());
	private static final RedisContainer TARGET = new RedisContainer(
			RedisContainer.DEFAULT_IMAGE_NAME.withTag(RedisContainer.DEFAULT_TAG));

	@TestInstance(Lifecycle.PER_CLASS)
	class NestedTestInstance implements RedisTestInstance {

		@Override
		public List<RedisTestContext> getContexts() {
			return RedisIntegrationTests.this.getContexts();
		}
	}

	@Override
	protected Collection<RedisServer> redisServers() {
		return Arrays.asList(REDIS, REDIS_ENTERPRISE, TARGET);
	}

	@Override
	protected Collection<RedisServer> testRedisServers() {
		return Arrays.asList(REDIS, REDIS_ENTERPRISE);
	}

	@Nested
	class Writer extends NestedTestInstance {

		@ParameterizedTest
		@RedisTestContextsSource
		void writeWait(RedisTestContext redis) throws Exception {
			List<Map<String, String>> maps = new ArrayList<>();
			for (int index = 0; index < 100; index++) {
				Map<String, String> body = new HashMap<>();
				body.put("id", String.valueOf(index));
				body.put("field1", "value1");
				body.put("field2", "value2");
				maps.add(body);
			}
			ListItemReader<Map<String, String>> reader = new ListItemReader<>(maps);
			RedisItemWriter<String, String, Map<String, String>> writer = RedisItemWriter
					.operation(pool(redis),
							Hset.<String, Map<String, String>>key(m -> "hash:" + m.remove("id"))
									.map(IdentityConverter.instance()).build())
					.options(WriterOptions.builder()
							.waitForReplication(WaitForReplication.of(1, Duration.ofMillis(300))).build())
					.build();
			String name = name(redis);
			setName(name, reader, writer);
			Job job = jobRunner.job(name)
					.start(jobRunner.step(name, reader, null, writer, StepOptions.builder().build()).build()).build();
			JobExecution execution = jobRunner.getJobLauncher().run(job, new JobParameters());
			List<Throwable> exceptions = execution.getAllFailureExceptions();
			assertEquals("Insufficient replication level - expected: 1, actual: 0", exceptions.get(0).getMessage());
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void writeHash(RedisTestContext redis) throws Exception {
			List<Map<String, String>> maps = new ArrayList<>();
			for (int index = 0; index < 100; index++) {
				Map<String, String> body = new HashMap<>();
				body.put("id", String.valueOf(index));
				body.put("field1", "value1");
				body.put("field2", "value2");
				maps.add(body);
			}
			ListItemReader<Map<String, String>> reader = new ListItemReader<>(maps);
			RedisItemWriter<String, String, Map<String, String>> writer = RedisItemWriter
					.operation(pool(redis), Hset.<String, Map<String, String>>key(m -> "hash:" + m.remove("id"))
							.map(IdentityConverter.instance()).build())
					.build();
			run(redis, reader, writer);
			assertEquals(maps.size(), redis.sync().keys("hash:*").size());
			for (int index = 0; index < maps.size(); index++) {
				Map<String, String> hash = redis.sync().hgetall("hash:" + index);
				assertEquals(maps.get(index), hash);
			}
		}

		private class Geo {
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
		void writeGeo(RedisTestContext redis) throws Exception {
			ListItemReader<Geo> reader = new ListItemReader<>(
					Arrays.asList(new Geo("Venice Breakwater", -118.476056, 33.985728),
							new Geo("Long Beach National", -73.667022, 40.582739)));
			GeoValueConverter<String, Geo> value = new GeoValueConverter<>(Geo::getMember, Geo::getLongitude,
					Geo::getLatitude);
			RedisItemWriter<String, String, Geo> writer = RedisItemWriter
					.operation(pool(redis), Geoadd.<String, Geo>key("geoset").value(value).build()).build();
			run(redis, reader, writer);
			Set<String> radius1 = redis.sync().georadius("geoset", -118, 34, 100, GeoArgs.Unit.mi);
			assertEquals(1, radius1.size());
			assertTrue(radius1.contains("Venice Breakwater"));
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void writeHashDel(RedisTestContext redis) throws Exception {
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
			RedisItemWriter<String, String, Map.Entry<String, Map<String, String>>> writer = RedisItemWriter.operation(
					pool(redis), Hset.<String, Entry<String, Map<String, String>>>key(e -> "hash:" + e.getKey())
							.map(Map.Entry::getValue).build())
					.build();
			run(redis, reader, writer);
			assertEquals(50, sync.keys("hash:*").size());
			assertEquals(2, sync.hgetall("hash:50").size());
		}

		private class ZValue {

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
		void writeSortedSet(RedisTestContext redis) throws Exception {
			List<ZValue> values = new ArrayList<>();
			for (int index = 0; index < 100; index++) {
				values.add(new ZValue(String.valueOf(index), index % 10));
			}
			ListItemReader<ZValue> reader = new ListItemReader<>(values);
			ScoredValueConverter<String, ZValue> converter = new ScoredValueConverter<>(ZValue::getMember,
					ZValue::getScore);
			RedisItemWriter<String, String, ZValue> writer = RedisItemWriter
					.operation(pool(redis), Zadd.<String, ZValue>key("zset").value(converter).build()).build();
			run(redis, reader, writer);
			RedisModulesCommands<String, String> sync = redis.sync();
			assertEquals(1, sync.dbsize());
			assertEquals(values.size(), sync.zcard("zset"));
			assertEquals(60,
					sync.zrangebyscore("zset", Range.from(Range.Boundary.including(0), Range.Boundary.including(5)))
							.size());
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void writeDataStructures(RedisTestContext redis) throws Exception {
			List<DataStructure<String>> list = new ArrayList<>();
			long count = 100;
			for (int index = 0; index < count; index++) {
				Map<String, String> map = new HashMap<>();
				map.put("field1", "value1");
				map.put("field2", "value2");
				DataStructure<String> ds = new DataStructure<>();
				ds.setKey("hash:" + index);
				ds.setType(Type.HASH);
				ds.setValue(map);
				list.add(ds);
			}
			ListItemReader<DataStructure<String>> reader = new ListItemReader<>(list);
			RedisItemWriter<String, String, DataStructure<String>> writer = RedisItemWriter.dataStructure(pool(redis))
					.build();
			run(redis, reader, writer);
			RedisModulesCommands<String, String> sync = redis.sync();
			List<String> keys = sync.keys("hash:*");
			assertEquals(count, keys.size());
		}
	}

	@Nested
	class Reader extends NestedTestInstance {

		@ParameterizedTest
		@RedisTestContextsSource
		void filterKeySlot(RedisTestContext redis) throws Exception {
			enableKeyspaceNotifications(redis);
			LiveRedisItemReader<String, DataStructure<String>> reader = RedisItemReader
					.liveDataStructure(pool(redis), jobRunner, redis.getPubSubConnection(), StringCodec.UTF8)
					.stepOptions(DEFAULT_FLUSHING_STEP_OPTIONS).keyFilter(SlotRangeFilter.of(0, 8000)).build();
			ListItemWriter<DataStructure<String>> writer = new ListItemWriter<>();
			JobExecution execution = runAsync(redis, reader, writer);
			int count = 100;
			generate(redis, DataGeneratorOptions.builder().count(count).build());
			awaitTermination(execution);
			Assertions.assertFalse(writer.getWrittenItems().stream().map(DataStructure::getKey).map(SlotHash::getSlot)
					.anyMatch(s -> s < 0 || s > 8000));
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void filterHotKeys(RedisTestContext redis) throws Exception {
			SetOptions setOptions = SetOptions.builder().cardinality(IntRange.is(1000)).build();
			generate(redis, DataGeneratorOptions.builder().types(Type.SET).setOptions(setOptions).count(1).build());
			HotKeyFilterOptions options = HotKeyFilterOptions.builder().maxMemoryUsage(DataSize.of(100, DataUnit.BYTES))
					.maxThroughput(1).pruneInterval(Duration.ofMillis(100)).stepOptions(DEFAULT_FLUSHING_STEP_OPTIONS)
					.build();
			HotKeyFilter<String, String> filter = new HotKeyFilter<>(pool(redis), jobRunner, options);
			filter.setName(name(redis) + "-filterHotKeys");
			filter.open(new ExecutionContext());
			Awaitility.await().until(filter::isOpen);
			String key = "gen:1";
			Assertions.assertTrue(filter.test(key));
			Awaitility.await().pollInterval(Duration.ofMillis(1)).until(() -> {
				return !filter.test(key);
			});
			Assertions.assertTrue(filter.getMetadata().containsKey(key));
			Awaitility.await().until(() -> filter.getMetadata().isEmpty());
			filter.close();
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void readKeyspaceNotifications(RedisTestContext redis) throws Exception {
			enableKeyspaceNotifications(redis);
			PollableItemReader<String> keyReader = RedisItemReader
					.liveDataStructure(pool(redis), jobRunner, redis.getPubSubConnection(), StringCodec.UTF8)
					.keyReader();
			ListItemWriter<String> writer = new ListItemWriter<>();
			JobExecution execution = runAsync(redis, keyReader, writer);
			int count = 2;
			generate(redis, DataGeneratorOptions.builder().types(Type.HASH, Type.STRING, Type.LIST, Type.SET, Type.ZSET)
					.count(count).build());
			awaitTermination(execution);
			assertEquals(new HashSet<>(redis.sync().keys("*")), new HashSet<>(writer.getWrittenItems()));
		}

		private class SynchronizedListItemWriter<T> extends AbstractItemStreamItemWriter<T> {

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
		void readThreads(RedisTestContext redis) throws Exception {
			generate(redis);
			RedisItemReader<String, DataStructure<String>> reader = dataStructureReader(redis).build();
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
					.<DataStructure<String>, DataStructure<String>>chunk(StepOptions.DEFAULT_CHUNK_SIZE)
					.reader(synchronizedReader).writer(writer).taskExecutor(taskExecutor).throttleLimit(threads)
					.build();
			Job job = jobBuilderFactory.get(name(redis) + "-job").start(step).build();
			jobRunner.getJobLauncher().run(job, new JobParameters());
			Awaitility.await().until(() -> !writer.isOpen());
			assertEquals(redis.sync().dbsize(), writer.getWrittenItems().size());
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void readLive(RedisTestContext redis) throws Exception {
			enableKeyspaceNotifications(redis);
			LiveRedisItemReader<String, KeyDump<String>> reader = configureLiveReader(liveKeyDumpReader(redis), 10000)
					.build();
			ListItemWriter<KeyDump<String>> writer = new ListItemWriter<>();
			JobExecution execution = runAsync(redis, reader, writer);
			generate(redis, DataGeneratorOptions.builder().count(123).types(Type.HASH, Type.STRING).build());
			awaitTermination(execution);
			Awaitility.await().until(() -> redis.sync().dbsize() == writer.getWrittenItems().size());
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void readHotKeyContext(RedisTestContext redis) throws Exception {
			generate(redis, DataGeneratorOptions.builder().build());
			GenericObjectPool<StatefulConnection<String, String>> pool = pool(redis);
			HotKeyFilter<String, String> filter = new HotKeyFilter<>(pool, jobRunner, HotKeyFilterOptions.builder()
					.maxMemoryUsage(DataSize.ofBytes(1)).stepOptions(DEFAULT_FLUSHING_STEP_OPTIONS).build());
			filter.setName(name(redis) + "-filter");
			filter.open(new ExecutionContext());
			Awaitility.await().until(() -> filter.isOpen());
			List<String> keys = redis.sync().keys("*");
			keys.forEach(filter::test);
			Awaitility.await().until(() -> filter.contexts().stream().allMatch(e -> e.getValue().getType() != null));
			for (Entry<String, KeyContext> entry : filter.contexts()) {
				Assertions.assertEquals(redis.sync().type(entry.getKey()), entry.getValue().getType());
				Assertions.assertEquals(redis.sync().memoryUsage(entry.getKey()), entry.getValue().getMemoryUsage());
			}
			Awaitility.await().until(() -> filter.contexts().stream().anyMatch(e -> !filter.test(e.getValue())));
			for (Entry<String, KeyContext> entry : filter.contexts()) {
				if (!filter.test(entry.getValue())) {
					Assertions
							.assertTrue(HotKeyFilterOptions.defaultBlockedTypes().contains(entry.getValue().getType()));
				}
			}
			filter.close();
		}

	}

	@Nested
	class Stream extends NestedTestInstance {

		private static final int COUNT = 57;

		private static final String DEFAULT_CONSUMER_GROUP = "consumerGroup";

		private void generateStreams(RedisTestContext redis) throws JobExecutionException {
			generate(redis, DataGeneratorOptions.builder().types(Type.STREAM)
					.streamOptions(StreamOptions.builder().messageCount(COUNT).build()).count(10).build());
		}

		private StreamReaderBuilder<String, String> streamReader(RedisTestContext redis, String key,
				Consumer<String> consumer) {
			return RedisItemReader.stream(pool(redis), key, consumer);
		}

		private void assertMessageBody(List<? extends StreamMessage<String, String>> items) {
			for (StreamMessage<String, String> message : items) {
				assertTrue(message.getBody().containsKey("field1"));
				assertTrue(message.getBody().containsKey("field2"));
			}
		}

		private void assertEquals(String expectedId, Map<String, String> expectedBody, String expectedStream,
				StreamMessage<String, String> message) {
			Assertions.assertEquals(expectedId, message.getId());
			Assertions.assertEquals(expectedBody, message.getBody());
			Assertions.assertEquals(expectedStream, message.getStream());
		}

		private Map<String, String> map(String... args) {
			Assert.notNull(args, "Args cannot be null");
			Assert.isTrue(args.length % 2 == 0, "Args length is not a multiple of 2");
			Map<String, String> body = new LinkedHashMap<>();
			for (int index = 0; index < args.length / 2; index++) {
				body.put(args[index * 2], args[index * 2 + 1]);
			}
			return body;
		}

		private void awaitClosed(StreamItemReader<?, ?> reader) {
			Awaitility.await().until(() -> !reader.isOpen());
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void readStreamAutoAck(RedisTestContext redis) throws InterruptedException {
			String stream = "stream1";
			String consumerGroup = "batchtests-readStreamAutoAck";
			Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
			final StreamItemReader<String, String> reader = RedisItemReader.stream(pool(redis), stream, consumer)
					.options(StreamReaderOptions.builder().ackPolicy(AckPolicy.AUTO).build()).build();
			reader.open(new ExecutionContext());
			String field1 = "field1";
			String value1 = "value1";
			String field2 = "field2";
			String value2 = "value2";
			Map<String, String> body = map(field1, value1, field2, value2);
			String id1 = redis.sync().xadd(stream, body);
			String id2 = redis.sync().xadd(stream, body);
			String id3 = redis.sync().xadd(stream, body);
			List<StreamMessage<String, String>> messages = new ArrayList<>();
			Awaitility.await().until(() -> messages.addAll(reader.readMessages()));
			Assertions.assertEquals(3, messages.size());
			assertEquals(id1, body, stream, messages.get(0));
			assertEquals(id2, body, stream, messages.get(1));
			assertEquals(id3, body, stream, messages.get(2));
			reader.close();
			Assertions.assertEquals(0, redis.sync().xpending(stream, consumerGroup).getCount(), "pending messages");
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void readStreamManualAck(RedisTestContext redis) throws Exception {
			String stream = "stream1";
			String consumerGroup = "batchtests-readStreamManualAck";
			Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
			final StreamItemReader<String, String> reader = RedisItemReader.stream(pool(redis), stream, consumer)
					.options(StreamReaderOptions.builder().ackPolicy(AckPolicy.MANUAL).build()).build();
			reader.open(new ExecutionContext());
			String field1 = "field1";
			String value1 = "value1";
			String field2 = "field2";
			String value2 = "value2";
			Map<String, String> body = map(field1, value1, field2, value2);
			String id1 = redis.sync().xadd(stream, body);
			String id2 = redis.sync().xadd(stream, body);
			String id3 = redis.sync().xadd(stream, body);
			List<StreamMessage<String, String>> messages = new ArrayList<>();
			Awaitility.await().until(() -> messages.addAll(reader.readMessages()));
			Assertions.assertEquals(3, messages.size());

			assertEquals(id1, body, stream, messages.get(0));
			assertEquals(id2, body, stream, messages.get(1));
			assertEquals(id3, body, stream, messages.get(2));
			PendingMessages pendingMsgsBeforeCommit = redis.sync().xpending(stream, consumerGroup);
			Assertions.assertEquals(3, pendingMsgsBeforeCommit.getCount(), "pending messages before commit");
			redis.sync().xack(stream, consumerGroup, messages.get(0).getId(), messages.get(1).getId());
			PendingMessages pendingMsgsAfterCommit = redis.sync().xpending(stream, consumerGroup);
			Assertions.assertEquals(1, pendingMsgsAfterCommit.getCount(), "pending messages after commit");
			reader.close();
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void readStreamManualAckRecover(RedisTestContext redis) throws InterruptedException {
			String stream = "stream1";
			Consumer<String> consumer = Consumer.from("batchtests-readStreamManualAckRecover", "consumer1");
			final StreamItemReader<String, String> reader = RedisItemReader.stream(pool(redis), stream, consumer)
					.options(StreamReaderOptions.builder().ackPolicy(AckPolicy.MANUAL).build()).build();
			reader.open(new ExecutionContext());
			String field1 = "field1";
			String value1 = "value1";
			String field2 = "field2";
			String value2 = "value2";
			Map<String, String> body = map(field1, value1, field2, value2);
			redis.sync().xadd(stream, body);
			redis.sync().xadd(stream, body);
			redis.sync().xadd(stream, body);
			List<StreamMessage<String, String>> messages = new ArrayList<>();
			Awaitility.await().until(() -> messages.addAll(reader.readMessages()));
			Assertions.assertEquals(3, messages.size());

			List<StreamMessage<String, String>> recoveredMessages = new ArrayList<>();
			redis.sync().xadd(stream, body);
			redis.sync().xadd(stream, body);
			redis.sync().xadd(stream, body);

			reader.close();

			final StreamItemReader<String, String> reader2 = RedisItemReader.stream(pool(redis), stream, consumer)
					.options(StreamReaderOptions.builder().ackPolicy(AckPolicy.MANUAL).build()).build();
			reader2.open(new ExecutionContext());

			Awaitility.await().until(() -> recoveredMessages.addAll(reader2.readMessages()));
			Awaitility.await().until(() -> !recoveredMessages.addAll(reader2.readMessages()));

			Assertions.assertEquals(6, recoveredMessages.size());
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void readStreamManualAckRecoverUncommitted(RedisTestContext redis) throws InterruptedException {
			String stream = "stream1";
			String consumerGroup = "batchtests-readStreamManualAckRecoverUncommitted";
			Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
			final StreamItemReader<String, String> reader = RedisItemReader.stream(pool(redis), stream, consumer)
					.options(StreamReaderOptions.builder().ackPolicy(AckPolicy.MANUAL).build()).build();
			reader.open(new ExecutionContext());
			String field1 = "field1";
			String value1 = "value1";
			String field2 = "field2";
			String value2 = "value2";
			Map<String, String> body = map(field1, value1, field2, value2);
			redis.sync().xadd(stream, body);
			redis.sync().xadd(stream, body);
			String id3 = redis.sync().xadd(stream, body);
			List<StreamMessage<String, String>> messages = new ArrayList<>();
			Awaitility.await().until(() -> messages.addAll(reader.readMessages()));
			Assertions.assertEquals(3, messages.size());
			redis.sync().xack(stream, consumerGroup, messages.get(0).getId(), messages.get(1).getId());

			List<StreamMessage<String, String>> recoveredMessages = new ArrayList<>();
			String id4 = redis.sync().xadd(stream, body);
			String id5 = redis.sync().xadd(stream, body);
			String id6 = redis.sync().xadd(stream, body);
			reader.close();

			final StreamItemReader<String, String> reader2 = RedisItemReader.stream(pool(redis), stream, consumer)
					.options(StreamReaderOptions.builder().ackPolicy(AckPolicy.MANUAL).offset(messages.get(1).getId())
							.build())
					.build();
			reader2.open(new ExecutionContext());

			// Wait until task.poll() doesn't return any more records
			Awaitility.await().until(() -> recoveredMessages.addAll(reader2.readMessages()));
			Awaitility.await().until(() -> !recoveredMessages.addAll(reader2.readMessages()));
			List<String> recoveredIds = recoveredMessages.stream().map(StreamMessage::getId)
					.collect(Collectors.toList());
			Assertions.assertEquals(Arrays.<String>asList(id3, id4, id5, id6), recoveredIds, "recoveredIds");
			reader2.close();
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void readStreamManualAckRecoverFromOffset(RedisTestContext redis) throws Exception {
			String stream = "stream1";
			String consumerGroup = "batchtests-readStreamManualAckRecoverFromOffset";
			Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
			final StreamItemReader<String, String> reader = RedisItemReader.stream(pool(redis), stream, consumer)
					.options(StreamReaderOptions.builder().ackPolicy(AckPolicy.MANUAL).build()).build();
			reader.open(new ExecutionContext());
			String field1 = "field1";
			String value1 = "value1";
			String field2 = "field2";
			String value2 = "value2";
			Map<String, String> body = map(field1, value1, field2, value2);
			redis.sync().xadd(stream, body);
			redis.sync().xadd(stream, body);
			String id3 = redis.sync().xadd(stream, body);
			List<StreamMessage<String, String>> sourceRecords = new ArrayList<>();
			Awaitility.await().until(() -> sourceRecords.addAll(reader.readMessages()));
			Assertions.assertEquals(3, sourceRecords.size());

			List<StreamMessage<String, String>> recoveredRecords = new ArrayList<>();
			String id4 = redis.sync().xadd(stream, body);
			String id5 = redis.sync().xadd(stream, body);
			String id6 = redis.sync().xadd(stream, body);

			reader.close();

			final StreamItemReader<String, String> reader2 = RedisItemReader.stream(pool(redis), stream, consumer)
					.options(StreamReaderOptions.builder().ackPolicy(AckPolicy.MANUAL).offset(id3).build()).build();
			reader2.open(new ExecutionContext());

			// Wait until task.poll() doesn't return any more records
			Awaitility.await().until(() -> recoveredRecords.addAll(reader2.readMessages()));
			Awaitility.await().until(() -> !recoveredRecords.addAll(reader2.readMessages()));
			List<String> recoveredIds = recoveredRecords.stream().map(StreamMessage::getId)
					.collect(Collectors.toList());
			Assertions.assertEquals(Arrays.<String>asList(id4, id5, id6), recoveredIds, "recoveredIds");
			reader2.close();
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void readStreamRecoverManualAckToAutoAck(RedisTestContext redis) throws InterruptedException {
			String stream = "stream1";
			String consumerGroup = "readStreamRecoverManualAckToAutoAck";
			Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
			final StreamItemReader<String, String> reader = RedisItemReader.stream(pool(redis), stream, consumer)
					.options(StreamReaderOptions.builder().ackPolicy(AckPolicy.MANUAL).build()).build();
			reader.open(new ExecutionContext());
			String field1 = "field1";
			String value1 = "value1";
			String field2 = "field2";
			String value2 = "value2";
			Map<String, String> body = map(field1, value1, field2, value2);
			redis.sync().xadd(stream, body);
			redis.sync().xadd(stream, body);
			redis.sync().xadd(stream, body);
			List<StreamMessage<String, String>> sourceRecords = new ArrayList<>();
			Awaitility.await().until(() -> sourceRecords.addAll(reader.readMessages()));
			Assertions.assertEquals(3, sourceRecords.size());

			List<StreamMessage<String, String>> recoveredRecords = new ArrayList<>();
			String id4 = redis.sync().xadd(stream, body);
			String id5 = redis.sync().xadd(stream, body);
			String id6 = redis.sync().xadd(stream, body);
			reader.close();

			final StreamItemReader<String, String> reader2 = RedisItemReader.stream(pool(redis), stream, consumer)
					.options(StreamReaderOptions.builder().ackPolicy(AckPolicy.AUTO).build()).build();
			reader2.open(new ExecutionContext());

			// Wait until task.poll() doesn't return any more records
			Awaitility.await().until(() -> recoveredRecords.addAll(reader2.readMessages()));
			Awaitility.await().until(() -> !recoveredRecords.addAll(reader2.readMessages()));
			List<String> recoveredIds = recoveredRecords.stream().map(StreamMessage::getId)
					.collect(Collectors.toList());
			Assertions.assertEquals(Arrays.asList(id4, id5, id6), recoveredIds, "recoveredIds");

			PendingMessages pending = redis.sync().xpending(stream, consumerGroup);
			Assertions.assertEquals(0, pending.getCount(), "pending message count");
			reader2.close();
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void readMessages(RedisTestContext redis) throws Exception {
			generateStreams(redis);
			List<String> keys = ScanIterator.scan(redis.sync(), KeyScanArgs.Builder.type(Type.STREAM.getString()))
					.stream().collect(Collectors.toList());
			for (String key : keys) {
				long count = redis.sync().xlen(key);
				StreamItemReader<String, String> reader = streamReader(redis, key,
						Consumer.from("batchtests-readmessages", "consumer1")).build();
				reader.open(new ExecutionContext());
				List<StreamMessage<String, String>> messages = new ArrayList<>();
				Awaitility.await().until(() -> {
					messages.addAll(reader.readMessages());
					return messages.size() == count;
				});
				assertMessageBody(messages);
				Awaitility.await().until(() -> reader.ack(reader.readMessages()) == 0);
				reader.close();
			}
		}

		@Test
		void readStreamJob() throws Exception {
			RedisTestContext redis = getContext(REDIS);
			generateStreams(redis);
			List<String> keys = ScanIterator.scan(redis.sync(), KeyScanArgs.Builder.type(Type.STREAM.getString()))
					.stream().collect(Collectors.toList());
			for (String key : keys) {
				StreamItemReader<String, String> reader = streamReader(redis, key,
						Consumer.from("batchtests-readstreamjob", "consumer1")).build();
				ListItemWriter<StreamMessage<String, String>> writer = new ListItemWriter<>();
				run(name(redis) + "-" + key, reader, writer);
				awaitClosed(reader);
				Awaitility.await().until(() -> COUNT == writer.getWrittenItems().size());
				assertMessageBody(writer.getWrittenItems());
			}
		}

		@Test
		void readMultipleStreams() throws Exception {
			RedisTestContext redis = getContext(REDIS);
			generateStreams(redis);
			final List<String> keys = ScanIterator.scan(redis.sync(), KeyScanArgs.Builder.type(Type.STREAM.getString()))
					.stream().collect(Collectors.toList());
			for (String key : keys) {
				StreamItemReader<String, String> reader1 = streamReader(redis, key,
						Consumer.from(DEFAULT_CONSUMER_GROUP, "consumer1"))
						.options(StreamReaderOptions.builder().ackPolicy(AckPolicy.MANUAL).build()).build();
				StreamItemReader<String, String> reader2 = streamReader(redis, key,
						Consumer.from(DEFAULT_CONSUMER_GROUP, "consumer2"))
						.options(StreamReaderOptions.builder().ackPolicy(AckPolicy.MANUAL).build()).build();
				ListItemWriter<StreamMessage<String, String>> writer1 = new ListItemWriter<>();
				JobExecution execution1 = runAsync(name(redis) + "-" + key + "-reader1", reader1, writer1);
				ListItemWriter<StreamMessage<String, String>> writer2 = new ListItemWriter<>();
				JobExecution execution2 = runAsync(name(redis) + "-" + key + "-reader2", reader2, writer2);
				awaitTermination(execution1);
				awaitTermination(execution2);
				awaitClosed(reader1);
				awaitClosed(reader2);
				Awaitility.await()
						.until(() -> COUNT == writer1.getWrittenItems().size() + writer2.getWrittenItems().size());
				assertMessageBody(writer1.getWrittenItems());
				assertMessageBody(writer2.getWrittenItems());
				RedisModulesCommands<String, String> sync = redis.sync();
				Assertions.assertEquals(COUNT, sync.xpending(key, DEFAULT_CONSUMER_GROUP).getCount());
				reader1.open(new ExecutionContext());
				reader1.ack(writer1.getWrittenItems().stream().map(StreamMessage::getId).toArray(String[]::new));
				reader1.close();
				reader2.open(new ExecutionContext());
				reader2.ack(writer2.getWrittenItems().stream().map(StreamMessage::getId).toArray(String[]::new));
				reader2.close();
				Assertions.assertEquals(0, sync.xpending(key, DEFAULT_CONSUMER_GROUP).getCount());
			}
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void writeStream(RedisTestContext redis) throws Exception {
			String stream = "stream:0";
			List<Map<String, String>> messages = new ArrayList<>();
			for (int index = 0; index < 100; index++) {
				Map<String, String> body = new HashMap<>();
				body.put("field1", "value1");
				body.put("field2", "value2");
				messages.add(body);
			}
			ListItemReader<Map<String, String>> reader = new ListItemReader<>(messages);
			RedisItemWriter<String, String, Map<String, String>> writer = RedisItemWriter.operation(pool(redis),
					Xadd.<String, Map<String, String>>key(stream).<String>body(IdentityConverter.instance()).build())
					.build();
			run(redis, reader, writer);
			RedisModulesCommands<String, String> sync = redis.sync();
			Assertions.assertEquals(messages.size(), sync.xlen(stream));
			List<StreamMessage<String, String>> xrange = sync.xrange(stream, Range.create("-", "+"));
			for (int index = 0; index < xrange.size(); index++) {
				StreamMessage<String, String> message = xrange.get(index);
				Assertions.assertEquals(messages.get(index), message.getBody());
			}
		}

	}

	@Nested
	class Replication extends NestedTestInstance {

		@ParameterizedTest
		@RedisTestContextsSource
		void dataStructures(RedisTestContext redis) throws Exception {
			RedisTestContext target = getContext(TARGET);
			generate(redis, DataGeneratorOptions.builder().count(100).build());
			RedisItemReader<String, DataStructure<String>> reader = dataStructureReader(redis).build();
			run(redis, reader, dataStructureWriter(target).build());
			awaitClosed(reader);
			compare(redis, target);
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void dumpAndRestore(RedisTestContext redis) throws Exception {
			RedisTestContext target = getContext(TARGET);
			generate(redis, DataGeneratorOptions.builder().count(100).build());
			RedisItemReader<String, KeyDump<String>> reader = keyDumpReader(redis).build();
			run("dumpAndRestore-" + name(redis), reader, keyDumpWriter(target).build());
			awaitClosed(reader);
			compare(redis, target);
		}

		@Test
		void byteArrayCodec() throws Exception {
			RedisTestContext target = getContext(TARGET);
			try (RedisTestContext redis = new RedisTestContext(REDIS)) {
				RedisClient client = redis.getRedisClient();
				StatefulRedisConnection<byte[], byte[]> connection = client.connect(new ByteArrayCodec());
				connection.setAutoFlushCommands(false);
				try {
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
				} finally {
					connection.setAutoFlushCommands(true);
				}
				run(redis, keyDumpReader(redis).build(), keyDumpWriter(target).build());
				compare(redis, target);
			}
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void live(RedisTestContext redis) throws Exception {
			RedisTestContext target = getContext(TARGET);
			enableKeyspaceNotifications(redis);
			liveReplication(redis, target, keyDumpReader(redis).build(), keyDumpWriter(target).build(),
					configureLiveReader(liveKeyDumpReader(redis), 100000).build(), keyDumpWriter(target).build());
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void liveSet(RedisTestContext redis) throws Exception {
			RedisTestContext target = getContext(TARGET);
			enableKeyspaceNotifications(redis);
			String key = "myset";
			redis.sync().sadd(key, "1", "2", "3", "4", "5");
			LiveRedisItemReader<String, DataStructure<String>> reader = configureLiveReader(
					liveDataStructureReader(redis), 100).build();
			JobExecution execution = runAsync(redis, reader, dataStructureWriter(target).build());
			awaitOpen(reader);
			log.info("Removing from set");
			redis.sync().srem(key, "5");
			awaitTermination(execution);
			assertEquals(redis.sync().smembers(key), target.sync().smembers(key));
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void hyperLogLog(RedisTestContext redis) throws Exception {
			RedisTestContext target = getContext(TARGET);
			String key1 = "hll:1";
			redis.sync().pfadd(key1, "member:1", "member:2");
			String key2 = "hll:2";
			redis.sync().pfadd(key2, "member:1", "member:2", "member:3");
			RedisItemReader<byte[], DataStructure<byte[]>> reader = RedisItemReader
					.dataStructure(bytesPool(redis), jobRunner).build();
			RedisItemWriter<byte[], byte[], DataStructure<byte[]>> writer = RedisItemWriter
					.dataStructure(bytesPool(target)).build();
			run(redis, reader, writer);
			RedisModulesCommands<String, String> sourceSync = redis.sync();
			RedisModulesCommands<String, String> targetSync = target.sync();
			assertEquals(sourceSync.pfcount(key1), targetSync.pfcount(key1));
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void replicateWithHotKeyFilter(RedisTestContext redis) throws Exception {
			RedisTestContext target = getContext(TARGET);
			List<String> coldkeys = IntStream.range(1, 100).mapToObj(i -> "cold:" + i).collect(Collectors.toList());
			List<String> hotkeys = IntStream.range(1, 100).mapToObj(i -> "hot:" + i).collect(Collectors.toList());
			StepOptions stepOptions = StepOptions.builder().flushingInterval(Duration.ofMillis(1))
					.idleTimeout(Duration.ofMillis(500)).build();
			enableKeyspaceNotifications(redis);
			HotKeyFilterOptions options = HotKeyFilterOptions.builder().maxMemoryUsage(DataSize.of(1, DataUnit.BYTES))
					.maxThroughput(100).build();
			HotKeyFilter<String, String> filter = new HotKeyFilter<>(pool(redis), jobRunner, options);
			filter.setName(name(redis) + "-filter");
			LiveRedisItemReader<String, DataStructure<String>> reader = liveDataStructureReader(redis).keyFilter(filter)
					.stepOptions(stepOptions).build();
			ItemWriter<DataStructure<String>> writer = dataStructureWriter(target).build();
			String name = name(redis);
			JobExecution execution = runAsync(name, reader, writer, stepOptions);
			awaitOpen(reader);
			Awaitility.await().until(() -> filter.isOpen());
			ZaddRunnable hotUpdater = new ZaddRunnable(redis, hotkeys);
			ZaddRunnable coldUpdater = new ZaddRunnable(redis, coldkeys);
			ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
			ScheduledFuture<?> coldFuture = executor.scheduleAtFixedRate(coldUpdater, 0, 10, TimeUnit.MILLISECONDS);
			ScheduledFuture<?> hotFuture = executor.scheduleAtFixedRate(hotUpdater, 0, 1, TimeUnit.MILLISECONDS);
			Awaitility.await().pollInterval(Duration.ofMillis(100))
					.until(() -> hotkeys.stream().allMatch(k -> redis.sync().zcard(k) > target.sync().zcard(k)));
			coldFuture.cancel(true);
			hotFuture.cancel(true);
			Awaitility.await().until(coldFuture::isDone);
			Awaitility.await().until(hotFuture::isDone);
			awaitTermination(execution);
			awaitClosed(reader);
			Awaitility.await().until(() -> !filter.isOpen());
			coldkeys.forEach(k -> Assertions.assertEquals(redis.sync().zcard(k), target.sync().zcard(k)));
			hotkeys.forEach(k -> Assertions.assertTrue(redis.sync().zcard(k) > target.sync().zcard(k)));
			Map<String, KeyContext> metadata = filter.getMetadata();
			hotkeys.forEach(k -> Assertions.assertTrue(metadata.containsKey(k)));
		}

		private class ZaddRunnable implements Runnable {

			private final AtomicInteger index = new AtomicInteger();
			private final RedisSortedSetCommands<String, String> commands;
			private final List<String> keys;

			public ZaddRunnable(RedisTestContext redis, List<String> keys) {
				this.commands = redis.sync();
				this.keys = keys;
			}

			@Override
			public void run() {
				int memberId = index.incrementAndGet();
				for (String key : keys) {
					commands.zadd(key, System.nanoTime(), String.valueOf(memberId));
				}
			}
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void liveDataStructures(RedisTestContext redis) throws Exception {
			RedisTestContext target = getContext(TARGET);
			enableKeyspaceNotifications(redis);
			liveReplication(redis, target, dataStructureReader(redis).build(), dataStructureWriter(target).build(),
					configureLiveReader(liveDataStructureReader(redis), 100000).build(),
					dataStructureWriter(target).build());
		}

		private <T extends KeyValue<String>> void liveReplication(RedisTestContext redis, RedisTestContext target,
				RedisItemReader<String, T> reader, RedisItemWriter<String, String, T> writer,
				LiveRedisItemReader<String, T> liveReader, RedisItemWriter<String, String, T> liveWriter)
				throws Exception {
			String name = name(redis);
			reader.setName(name + "-reader");
			writer.setName(name + "-writer");
			liveReader.setName(name + "-liveReader");
			liveWriter.setName(name + "-liveWriter");
			generate(redis, DataGeneratorOptions.builder()
					.types(Type.HASH, Type.LIST, Type.SET, Type.STREAM, Type.STRING, Type.ZSET).count(300).build());
			TaskletStep step = jobRunner.step(name + "-snapshot", reader, null, writer, DEFAULT_STEP_OPTIONS).build();
			SimpleFlow flow = new FlowBuilder<SimpleFlow>(name + "-snapshot-flow").start(step).build();
			TaskletStep liveStep = new FlushingSimpleStepBuilder<>(
					jobRunner.step(name + "-live-step", liveReader, null, liveWriter, DEFAULT_FLUSHING_STEP_OPTIONS))
					.idleTimeout(DEFAULT_IDLE_TIMEOUT).build();
			SimpleFlow liveFlow = new FlowBuilder<SimpleFlow>(name + "-live-flow").start(liveStep).build();
			Job job = jobBuilderFactory.get(name + "-job").start(new FlowBuilder<SimpleFlow>(name + "-flow")
					.split(new SimpleAsyncTaskExecutor()).add(liveFlow, flow).build()).build().build();
			JobExecution execution = jobRunner.getAsyncJobLauncher().run(job, new JobParameters());
			awaitOpen(liveReader);
			generate(name(redis) + "-generate-2", redis,
					DataGeneratorOptions.builder().types(Type.HASH, Type.LIST, Type.SET, Type.STRING, Type.ZSET)
							.expiration(IntRange.is(100)).range(IntRange.between(300, 1000)).build());
			awaitTermination(execution);
			awaitClosed(reader);
			awaitClosed(liveReader);
			compare(redis, target);
		}

	}

	private void enableKeyspaceNotifications(RedisTestContext redis) {
		redis.sync().configSet("notify-keyspace-events", "AK");
	}

	private void generate(RedisTestContext redis, DataGeneratorOptions options) throws JobExecutionException {
		generate(name(redis), redis, options);
	}

	private void generate(String name, RedisTestContext redis, DataGeneratorOptions options)
			throws JobExecutionException {
		DataGeneratorItemReader reader = new DataGeneratorItemReader(options);
		reader.setMaxItemCount(options.getKeyRange().getMax() - options.getKeyRange().getMin() + 1);
		run(name + "-generate", reader, RedisItemWriter.dataStructure(pool(redis)).build());
	}

	@Nested
	class Other extends NestedTestInstance {

		@ParameterizedTest
		@RedisTestContextsSource
		void scanSizeEstimator(RedisTestContext redis) throws Exception {
			String pattern = DataGeneratorOptions.DEFAULT_KEYSPACE + "*";
			int count = 12345;
			generate(redis, DataGeneratorOptions.builder().count(count).build());
			long expectedCount = redis.sync().dbsize();
			Builder estimator = ScanSizeEstimator.builder(pool(redis));
			assertEquals(expectedCount,
					estimator.options(ScanSizeEstimatorOptions.builder().match(pattern).build()).build().execute(),
					expectedCount / 10);
			assertEquals(expectedCount / DataGeneratorOptions.defaultTypes().size(), estimator
					.options(ScanSizeEstimatorOptions.builder().type(Type.HASH.getString()).build()).build().execute(),
					expectedCount / 10);
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void compare(RedisTestContext redis) throws Exception {
			RedisTestContext target = getContext(TARGET);
			generate(redis, DataGeneratorOptions.builder().count(120).build());
			run("replicate-" + name(redis), keyDumpReader(redis).build(), keyDumpWriter(target).build());
			long deleted = 0;
			for (int index = 0; index < 13; index++) {
				deleted += target.sync().del(target.sync().randomkey());
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
				Type type = Type.of(target.sync().type(key));
				if (type == Type.STRING) {
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
			RedisItemReader<String, KeyComparison<String>> reader = comparisonReader(redis, getContext(TARGET));
			KeyComparisonCountItemWriter<String> writer = new KeyComparisonCountItemWriter<>();
			run(name(redis), reader, writer);
			Results results = writer.getResults();
			long sourceCount = redis.sync().dbsize();
			assertEquals(sourceCount, results.getTotalCount());
			assertEquals(sourceCount, target.sync().dbsize() + deleted);
			assertEquals(typeChanges.size(), results.getCount(Status.TYPE));
			assertEquals(valueChanges.size(), results.getCount(Status.VALUE));
			assertEquals(ttlChanges.size(), results.getCount(Status.TTL));
			assertEquals(deleted, results.getCount(Status.MISSING));
		}

	}

}
