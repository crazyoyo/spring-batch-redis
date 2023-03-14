package com.redis.spring.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;

import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.DataStructure.Type;
import com.redis.spring.batch.common.IntRange;
import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.StepOptions;
import com.redis.spring.batch.convert.GeoValueConverter;
import com.redis.spring.batch.convert.IdentityConverter;
import com.redis.spring.batch.convert.ScoredValueConverter;
import com.redis.spring.batch.reader.GeneratorItemReader;
import com.redis.spring.batch.reader.GeneratorReaderOptions;
import com.redis.spring.batch.reader.GeneratorReaderOptions.StreamOptions;
import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.spring.batch.reader.KeyEventType;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.ReaderOptions;
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

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.Consumer;
import io.lettuce.core.GeoArgs;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.Range;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.models.stream.PendingMessages;

abstract class AbstractIntegrationTests extends AbstractTestBase {

	@Nested
	class Writer {

		@Test
		void writeWait() throws Exception {
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
					.operation(sourcePool,
							Hset.<String, Map<String, String>>key(m -> "hash:" + m.remove("id"))
									.map(IdentityConverter.instance()).build())
					.options(WriterOptions.builder()
							.waitForReplication(WaitForReplication.of(1, Duration.ofMillis(300))).build())
					.build();
			String name = name();
			setName(name, reader, writer);
			Job job = jobRunner.job(name)
					.start(jobRunner.step(name, reader, null, writer, StepOptions.builder().build()).build()).build();
			JobExecution execution = jobRunner.getJobLauncher().run(job, new JobParameters());
			List<Throwable> exceptions = execution.getAllFailureExceptions();
			assertEquals("Insufficient replication level - expected: 1, actual: 0", exceptions.get(0).getMessage());
		}

		@Test
		void writeHash() throws Exception {
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
					.operation(sourcePool, Hset.<String, Map<String, String>>key(m -> "hash:" + m.remove("id"))
							.map(IdentityConverter.instance()).build())
					.build();
			run(reader, writer);
			assertEquals(maps.size(), sourceConnection.sync().keys("hash:*").size());
			for (int index = 0; index < maps.size(); index++) {
				Map<String, String> hash = sourceConnection.sync().hgetall("hash:" + index);
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

		@Test
		void writeGeo() throws Exception {
			ListItemReader<Geo> reader = new ListItemReader<>(
					Arrays.asList(new Geo("Venice Breakwater", -118.476056, 33.985728),
							new Geo("Long Beach National", -73.667022, 40.582739)));
			GeoValueConverter<String, Geo> value = new GeoValueConverter<>(Geo::getMember, Geo::getLongitude,
					Geo::getLatitude);
			RedisItemWriter<String, String, Geo> writer = RedisItemWriter
					.operation(sourcePool, Geoadd.<String, Geo>key("geoset").value(value).build()).build();
			run(reader, writer);
			Set<String> radius1 = sourceConnection.sync().georadius("geoset", -118, 34, 100, GeoArgs.Unit.mi);
			assertEquals(1, radius1.size());
			assertTrue(radius1.contains("Venice Breakwater"));
		}

		@Test
		void writeHashDel() throws Exception {
			List<Entry<String, Map<String, String>>> hashes = new ArrayList<>();
			RedisModulesCommands<String, String> sync = sourceConnection.sync();
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
					sourcePool, Hset.<String, Entry<String, Map<String, String>>>key(e -> "hash:" + e.getKey())
							.map(Map.Entry::getValue).build())
					.build();
			run(reader, writer);
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

		@Test
		void writeSortedSet() throws Exception {
			List<ZValue> values = new ArrayList<>();
			for (int index = 0; index < 100; index++) {
				values.add(new ZValue(String.valueOf(index), index % 10));
			}
			ListItemReader<ZValue> reader = new ListItemReader<>(values);
			ScoredValueConverter<String, ZValue> converter = new ScoredValueConverter<>(ZValue::getMember,
					ZValue::getScore);
			RedisItemWriter<String, String, ZValue> writer = RedisItemWriter
					.operation(sourcePool, Zadd.<String, ZValue>key("zset").value(converter).build()).build();
			run(reader, writer);
			RedisModulesCommands<String, String> sync = sourceConnection.sync();
			assertEquals(1, sync.dbsize());
			assertEquals(values.size(), sync.zcard("zset"));
			assertEquals(60,
					sync.zrangebyscore("zset", Range.from(Range.Boundary.including(0), Range.Boundary.including(5)))
							.size());
		}

		@Test
		void writeDataStructures() throws Exception {
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
			RedisItemWriter<String, String, DataStructure<String>> writer = RedisItemWriter.dataStructure(sourcePool)
					.build();
			run(reader, writer);
			RedisModulesCommands<String, String> sync = sourceConnection.sync();
			List<String> keys = sync.keys("hash:*");
			assertEquals(count, keys.size());
		}
	}

	@Nested
	class Reader {

		@Test
		void filterKeySlot() throws Exception {
			enableKeyspaceNotifications(sourceClient);
			LiveRedisItemReader<String, DataStructure<String>> reader = RedisItemReader
					.liveDataStructure(sourcePool, jobRunner, sourceClient, StringCodec.UTF8)
					.readerOptions(ReaderOptions.builder().stepOptions(DEFAULT_FLUSHING_STEP_OPTIONS).build())
					.keyFilter(SlotRangeFilter.of(0, 8000)).build();
			ListItemWriter<DataStructure<String>> writer = new ListItemWriter<>();
			JobExecution execution = runAsync(reader, writer);
			int count = 100;
			generate(GeneratorReaderOptions.builder().count(count).build());
			awaitTermination(execution);
			Assertions.assertFalse(writer.getWrittenItems().stream().map(DataStructure::getKey).map(SlotHash::getSlot)
					.anyMatch(s -> s < 0 || s > 8000));
		}

		@Test
		void readKeyspaceNotifications() throws Exception {
			enableKeyspaceNotifications(sourceClient);
			try (KeyspaceNotificationItemReader reader = new KeyspaceNotificationItemReader(sourceClient)) {
				reader.getOptions().getQueueOptions().setCapacity(100000);
				reader.open(new ExecutionContext());
				generate(GeneratorReaderOptions.builder().types(Type.HASH, Type.LIST, Type.SET, Type.STREAM,
						Type.STRING, Type.ZSET, Type.TIMESERIES, Type.JSON).build());
				Awaitility.await().until(() -> reader.getQueue().size() == 100);
				Assertions.assertEquals(KeyEventType.SET, reader.getQueue().remove().getEventType());
				assertEventTypes(reader, KeyEventType.SET, KeyEventType.HSET, KeyEventType.JSON_SET, KeyEventType.RPUSH,
						KeyEventType.SADD, KeyEventType.ZADD, KeyEventType.XADD, KeyEventType.TS_ADD);
			}
		}

		private void assertEventTypes(KeyspaceNotificationItemReader reader, KeyEventType... expectedEventTypes) {
			Set<KeyEventType> actualEventTypes = new LinkedHashSet<>();
			while (!reader.getQueue().isEmpty()) {
				actualEventTypes.add(reader.getQueue().remove().getEventType());
			}
			Assertions.assertEquals(new LinkedHashSet<>(Arrays.asList(expectedEventTypes)), actualEventTypes);
		}

		@Test
		void dedupeKeyspaceNotifications() throws Exception {
			enableKeyspaceNotifications(sourceClient);
			try (KeyspaceNotificationItemReader reader = new KeyspaceNotificationItemReader(sourceClient)) {
				reader.getOptions().getQueueOptions().setCapacity(100000);
				reader.open(new ExecutionContext());
				RedisModulesCommands<String, String> commands = sourceConnection.sync();
				String key = "key1";
				commands.zadd(key, 1, "member1");
				commands.zadd(key, 2, "member2");
				commands.zadd(key, 3, "member3");
				Awaitility.await().until(() -> reader.getQueue().size() == 1);
				Assertions.assertEquals(key, reader.read());
			}
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

		@Test
		void readThreads() throws Exception {
			generate();
			RedisItemReader<String, DataStructure<String>> reader = dataStructureReader().build();
			String name = name();
			reader.setName(name + "-reader");
			SynchronizedItemStreamReader<DataStructure<String>> synchronizedReader = new SynchronizedItemStreamReader<>();
			synchronizedReader.setDelegate(reader);
			synchronizedReader.afterPropertiesSet();
			SynchronizedListItemWriter<DataStructure<String>> writer = new SynchronizedListItemWriter<>();
			int threads = 4;
			ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
			taskExecutor.setMaxPoolSize(threads);
			taskExecutor.setCorePoolSize(threads);
			taskExecutor.afterPropertiesSet();
			TaskletStep step = stepBuilderFactory.get(name)
					.<DataStructure<String>, DataStructure<String>>chunk(StepOptions.DEFAULT_CHUNK_SIZE)
					.reader(synchronizedReader).writer(writer).taskExecutor(taskExecutor).throttleLimit(threads)
					.build();
			Job job = jobBuilderFactory.get(name).start(step).build();
			jobRunner.getJobLauncher().run(job, new JobParameters());
			Awaitility.await().until(() -> !writer.isOpen());
			assertEquals(sourceConnection.sync().dbsize(), writer.getWrittenItems().size());
		}

		@Test
		void readLive() throws Exception {
			enableKeyspaceNotifications(sourceClient);
			LiveRedisItemReader<String, KeyDump<String>> reader = liveReader(liveKeyDumpReader(), 10000);
			ListItemWriter<KeyDump<String>> writer = new ListItemWriter<>();
			JobExecution execution = runAsync(reader, writer);
			awaitOpen(reader);
			generate(GeneratorReaderOptions.builder().count(123).types(Type.HASH, Type.STRING).build());
			awaitTermination(execution);
			awaitClosed(reader);
			Awaitility.await().until(() -> sourceConnection.sync().dbsize() == writer.getWrittenItems().size());
		}

	}

	@Nested
	class Stream {

		private static final int COUNT = 57;

		private static final String DEFAULT_CONSUMER_GROUP = "consumerGroup";

		private void generateStreams() throws JobExecutionException {
			generate(GeneratorReaderOptions.builder().types(Type.STREAM)
					.streamOptions(StreamOptions.builder().messageCount(COUNT).build()).count(10).build());
		}

		private StreamReaderBuilder<String, String> streamReader(String key, Consumer<String> consumer) {
			return RedisItemReader.stream(sourcePool, key, consumer);
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

		@Test
		void readStreamAutoAck() throws InterruptedException {
			String stream = "stream1";
			String consumerGroup = "batchtests-readStreamAutoAck";
			Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
			final StreamItemReader<String, String> reader = RedisItemReader.stream(sourcePool, stream, consumer)
					.options(StreamReaderOptions.builder().ackPolicy(AckPolicy.AUTO).build()).build();
			reader.open(new ExecutionContext());
			String field1 = "field1";
			String value1 = "value1";
			String field2 = "field2";
			String value2 = "value2";
			Map<String, String> body = map(field1, value1, field2, value2);
			String id1 = sourceConnection.sync().xadd(stream, body);
			String id2 = sourceConnection.sync().xadd(stream, body);
			String id3 = sourceConnection.sync().xadd(stream, body);
			List<StreamMessage<String, String>> messages = new ArrayList<>();
			Awaitility.await().until(() -> messages.addAll(reader.readMessages()));
			Assertions.assertEquals(3, messages.size());
			assertEquals(id1, body, stream, messages.get(0));
			assertEquals(id2, body, stream, messages.get(1));
			assertEquals(id3, body, stream, messages.get(2));
			reader.close();
			Assertions.assertEquals(0, sourceConnection.sync().xpending(stream, consumerGroup).getCount(),
					"pending messages");
		}

		@Test
		void readStreamManualAck() throws Exception {
			String stream = "stream1";
			String consumerGroup = "batchtests-readStreamManualAck";
			Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
			final StreamItemReader<String, String> reader = RedisItemReader.stream(sourcePool, stream, consumer)
					.options(StreamReaderOptions.builder().ackPolicy(AckPolicy.MANUAL).build()).build();
			reader.open(new ExecutionContext());
			String field1 = "field1";
			String value1 = "value1";
			String field2 = "field2";
			String value2 = "value2";
			Map<String, String> body = map(field1, value1, field2, value2);
			String id1 = sourceConnection.sync().xadd(stream, body);
			String id2 = sourceConnection.sync().xadd(stream, body);
			String id3 = sourceConnection.sync().xadd(stream, body);
			List<StreamMessage<String, String>> messages = new ArrayList<>();
			Awaitility.await().until(() -> messages.addAll(reader.readMessages()));
			Assertions.assertEquals(3, messages.size());

			assertEquals(id1, body, stream, messages.get(0));
			assertEquals(id2, body, stream, messages.get(1));
			assertEquals(id3, body, stream, messages.get(2));
			PendingMessages pendingMsgsBeforeCommit = sourceConnection.sync().xpending(stream, consumerGroup);
			Assertions.assertEquals(3, pendingMsgsBeforeCommit.getCount(), "pending messages before commit");
			sourceConnection.sync().xack(stream, consumerGroup, messages.get(0).getId(), messages.get(1).getId());
			PendingMessages pendingMsgsAfterCommit = sourceConnection.sync().xpending(stream, consumerGroup);
			Assertions.assertEquals(1, pendingMsgsAfterCommit.getCount(), "pending messages after commit");
			reader.close();
		}

		@Test
		void readStreamManualAckRecover() throws InterruptedException {
			String stream = "stream1";
			Consumer<String> consumer = Consumer.from("batchtests-readStreamManualAckRecover", "consumer1");
			final StreamItemReader<String, String> reader = RedisItemReader.stream(sourcePool, stream, consumer)
					.options(StreamReaderOptions.builder().ackPolicy(AckPolicy.MANUAL).build()).build();
			reader.open(new ExecutionContext());
			String field1 = "field1";
			String value1 = "value1";
			String field2 = "field2";
			String value2 = "value2";
			Map<String, String> body = map(field1, value1, field2, value2);
			sourceConnection.sync().xadd(stream, body);
			sourceConnection.sync().xadd(stream, body);
			sourceConnection.sync().xadd(stream, body);
			List<StreamMessage<String, String>> messages = new ArrayList<>();
			Awaitility.await().until(() -> messages.addAll(reader.readMessages()));
			Assertions.assertEquals(3, messages.size());

			List<StreamMessage<String, String>> recoveredMessages = new ArrayList<>();
			sourceConnection.sync().xadd(stream, body);
			sourceConnection.sync().xadd(stream, body);
			sourceConnection.sync().xadd(stream, body);

			reader.close();

			final StreamItemReader<String, String> reader2 = RedisItemReader.stream(sourcePool, stream, consumer)
					.options(StreamReaderOptions.builder().ackPolicy(AckPolicy.MANUAL).build()).build();
			reader2.open(new ExecutionContext());

			Awaitility.await().until(() -> recoveredMessages.addAll(reader2.readMessages()));
			Awaitility.await().until(() -> !recoveredMessages.addAll(reader2.readMessages()));

			Assertions.assertEquals(6, recoveredMessages.size());
		}

		@Test
		void readStreamManualAckRecoverUncommitted() throws InterruptedException {
			String stream = "stream1";
			String consumerGroup = "batchtests-readStreamManualAckRecoverUncommitted";
			Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
			final StreamItemReader<String, String> reader = RedisItemReader.stream(sourcePool, stream, consumer)
					.options(StreamReaderOptions.builder().ackPolicy(AckPolicy.MANUAL).build()).build();
			reader.open(new ExecutionContext());
			String field1 = "field1";
			String value1 = "value1";
			String field2 = "field2";
			String value2 = "value2";
			Map<String, String> body = map(field1, value1, field2, value2);
			sourceConnection.sync().xadd(stream, body);
			sourceConnection.sync().xadd(stream, body);
			String id3 = sourceConnection.sync().xadd(stream, body);
			List<StreamMessage<String, String>> messages = new ArrayList<>();
			Awaitility.await().until(() -> messages.addAll(reader.readMessages()));
			Assertions.assertEquals(3, messages.size());
			sourceConnection.sync().xack(stream, consumerGroup, messages.get(0).getId(), messages.get(1).getId());

			List<StreamMessage<String, String>> recoveredMessages = new ArrayList<>();
			String id4 = sourceConnection.sync().xadd(stream, body);
			String id5 = sourceConnection.sync().xadd(stream, body);
			String id6 = sourceConnection.sync().xadd(stream, body);
			reader.close();

			final StreamItemReader<String, String> reader2 = RedisItemReader.stream(sourcePool, stream, consumer)
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

		@Test
		void readStreamManualAckRecoverFromOffset() throws Exception {
			String stream = "stream1";
			String consumerGroup = "batchtests-readStreamManualAckRecoverFromOffset";
			Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
			final StreamItemReader<String, String> reader = RedisItemReader.stream(sourcePool, stream, consumer)
					.options(StreamReaderOptions.builder().ackPolicy(AckPolicy.MANUAL).build()).build();
			reader.open(new ExecutionContext());
			String field1 = "field1";
			String value1 = "value1";
			String field2 = "field2";
			String value2 = "value2";
			Map<String, String> body = map(field1, value1, field2, value2);
			sourceConnection.sync().xadd(stream, body);
			sourceConnection.sync().xadd(stream, body);
			String id3 = sourceConnection.sync().xadd(stream, body);
			List<StreamMessage<String, String>> sourceRecords = new ArrayList<>();
			Awaitility.await().until(() -> sourceRecords.addAll(reader.readMessages()));
			Assertions.assertEquals(3, sourceRecords.size());

			List<StreamMessage<String, String>> recoveredRecords = new ArrayList<>();
			String id4 = sourceConnection.sync().xadd(stream, body);
			String id5 = sourceConnection.sync().xadd(stream, body);
			String id6 = sourceConnection.sync().xadd(stream, body);

			reader.close();

			final StreamItemReader<String, String> reader2 = RedisItemReader.stream(sourcePool, stream, consumer)
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

		@Test
		void readStreamRecoverManualAckToAutoAck() throws InterruptedException {
			String stream = "stream1";
			String consumerGroup = "readStreamRecoverManualAckToAutoAck";
			Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
			final StreamItemReader<String, String> reader = RedisItemReader.stream(sourcePool, stream, consumer)
					.options(StreamReaderOptions.builder().ackPolicy(AckPolicy.MANUAL).build()).build();
			reader.open(new ExecutionContext());
			String field1 = "field1";
			String value1 = "value1";
			String field2 = "field2";
			String value2 = "value2";
			Map<String, String> body = map(field1, value1, field2, value2);
			sourceConnection.sync().xadd(stream, body);
			sourceConnection.sync().xadd(stream, body);
			sourceConnection.sync().xadd(stream, body);
			List<StreamMessage<String, String>> sourceRecords = new ArrayList<>();
			Awaitility.await().until(() -> sourceRecords.addAll(reader.readMessages()));
			Assertions.assertEquals(3, sourceRecords.size());

			List<StreamMessage<String, String>> recoveredRecords = new ArrayList<>();
			String id4 = sourceConnection.sync().xadd(stream, body);
			String id5 = sourceConnection.sync().xadd(stream, body);
			String id6 = sourceConnection.sync().xadd(stream, body);
			reader.close();

			final StreamItemReader<String, String> reader2 = RedisItemReader.stream(sourcePool, stream, consumer)
					.options(StreamReaderOptions.builder().ackPolicy(AckPolicy.AUTO).build()).build();
			reader2.open(new ExecutionContext());

			// Wait until task.poll() doesn't return any more records
			Awaitility.await().until(() -> recoveredRecords.addAll(reader2.readMessages()));
			Awaitility.await().until(() -> !recoveredRecords.addAll(reader2.readMessages()));
			List<String> recoveredIds = recoveredRecords.stream().map(StreamMessage::getId)
					.collect(Collectors.toList());
			Assertions.assertEquals(Arrays.asList(id4, id5, id6), recoveredIds, "recoveredIds");

			PendingMessages pending = sourceConnection.sync().xpending(stream, consumerGroup);
			Assertions.assertEquals(0, pending.getCount(), "pending message count");
			reader2.close();
		}

		@Test
		void readMessages() throws Exception {
			generateStreams();
			List<String> keys = ScanIterator
					.scan(sourceConnection.sync(), KeyScanArgs.Builder.type(Type.STREAM.getString())).stream()
					.collect(Collectors.toList());
			for (String key : keys) {
				long count = sourceConnection.sync().xlen(key);
				StreamItemReader<String, String> reader = streamReader(key,
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
			generateStreams();
			List<String> keys = ScanIterator
					.scan(sourceConnection.sync(), KeyScanArgs.Builder.type(Type.STREAM.getString())).stream()
					.collect(Collectors.toList());
			for (String key : keys) {
				StreamItemReader<String, String> reader = streamReader(key,
						Consumer.from("batchtests-readstreamjob", "consumer1")).build();
				ListItemWriter<StreamMessage<String, String>> writer = new ListItemWriter<>();
				run(reader, writer);
				awaitClosed(reader);
				Awaitility.await().until(() -> COUNT == writer.getWrittenItems().size());
				assertMessageBody(writer.getWrittenItems());
			}
		}

		@Test
		void readMultipleStreams() throws Exception {
			generateStreams();
			final List<String> keys = ScanIterator
					.scan(sourceConnection.sync(), KeyScanArgs.Builder.type(Type.STREAM.getString())).stream()
					.collect(Collectors.toList());
			for (String key : keys) {
				StreamItemReader<String, String> reader1 = streamReader(key,
						Consumer.from(DEFAULT_CONSUMER_GROUP, "consumer1"))
						.options(StreamReaderOptions.builder().ackPolicy(AckPolicy.MANUAL).build()).build();
				StreamItemReader<String, String> reader2 = streamReader(key,
						Consumer.from(DEFAULT_CONSUMER_GROUP, "consumer2"))
						.options(StreamReaderOptions.builder().ackPolicy(AckPolicy.MANUAL).build()).build();
				ListItemWriter<StreamMessage<String, String>> writer1 = new ListItemWriter<>();
				JobExecution execution1 = runAsync(reader1, writer1);
				ListItemWriter<StreamMessage<String, String>> writer2 = new ListItemWriter<>();
				JobExecution execution2 = runAsync(reader2, writer2);
				awaitTermination(execution1);
				awaitTermination(execution2);
				awaitClosed(reader1);
				awaitClosed(reader2);
				Awaitility.await()
						.until(() -> COUNT == writer1.getWrittenItems().size() + writer2.getWrittenItems().size());
				assertMessageBody(writer1.getWrittenItems());
				assertMessageBody(writer2.getWrittenItems());
				RedisModulesCommands<String, String> sync = sourceConnection.sync();
				Assertions.assertEquals(COUNT, sync.xpending(key, DEFAULT_CONSUMER_GROUP).getCount());
				reader1.open(new ExecutionContext());
				reader1.ack(writer1.getWrittenItems());
				reader1.close();
				reader2.open(new ExecutionContext());
				reader2.ack(writer2.getWrittenItems());
				reader2.close();
				Assertions.assertEquals(0, sync.xpending(key, DEFAULT_CONSUMER_GROUP).getCount());
			}
		}

		@Test
		void writeStream() throws Exception {
			String stream = "stream:0";
			List<Map<String, String>> messages = new ArrayList<>();
			for (int index = 0; index < 100; index++) {
				Map<String, String> body = new HashMap<>();
				body.put("field1", "value1");
				body.put("field2", "value2");
				messages.add(body);
			}
			ListItemReader<Map<String, String>> reader = new ListItemReader<>(messages);
			RedisItemWriter<String, String, Map<String, String>> writer = RedisItemWriter.operation(sourcePool,
					Xadd.<String, Map<String, String>>key(stream).<String>body(IdentityConverter.instance()).build())
					.build();
			run(reader, writer);
			RedisModulesCommands<String, String> sync = sourceConnection.sync();
			Assertions.assertEquals(messages.size(), sync.xlen(stream));
			List<StreamMessage<String, String>> xrange = sync.xrange(stream, Range.create("-", "+"));
			for (int index = 0; index < xrange.size(); index++) {
				StreamMessage<String, String> message = xrange.get(index);
				Assertions.assertEquals(messages.get(index), message.getBody());
			}
		}

	}

	@Nested
	class Replication {

		@Test
		void dataStructures() throws Exception {
			generate(GeneratorReaderOptions.builder().count(100).build());
			RedisItemReader<String, DataStructure<String>> reader = dataStructureReader().build();
			run(reader, dataStructureWriter().build());
			awaitClosed(reader);
			Assertions.assertTrue(compare());
		}

		@Test
		void dumpAndRestore() throws Exception {
			generate(GeneratorReaderOptions.builder().count(100).build());
			RedisItemReader<String, KeyDump<String>> reader = keyDumpReader().build();
			run(reader, keyDumpWriter().build());
			awaitClosed(reader);
			Assertions.assertTrue(compare());
		}

		@Test
		void byteArrayCodec() throws Exception {
			StatefulRedisConnection<byte[], byte[]> connection = RedisModulesUtils.connection(sourceClient,
					ByteArrayCodec.INSTANCE);
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
			run(keyDumpReader().build(), keyDumpWriter().build());
			Assertions.assertTrue(compare());
		}

		@Test
		void liveOnly() throws JobExecutionException {
			enableKeyspaceNotifications(sourceClient);
			String name = name();
			LiveRedisItemReader<String, KeyDump<String>> reader = liveReader(liveKeyDumpReader(), 100000);
			RedisItemWriter<String, String, KeyDump<String>> writer = keyDumpWriter().build();
			reader.setName(name + "-reader");
			writer.setName(name + "-writer");
			JobExecution execution = runAsync(reader, writer);
			awaitOpen(reader);
			generate(GeneratorReaderOptions.builder().types(Type.HASH, Type.LIST, Type.SET, Type.STRING, Type.ZSET)
					.build());
			awaitTermination(execution);
			awaitClosed(reader);
			awaitClosed(writer);
			Assertions.assertTrue(compare());
		}

		@Test
		void live() throws Exception {
			enableKeyspaceNotifications(sourceClient);
			RedisItemReader<String, KeyDump<String>> reader = keyDumpReader().build();
			RedisItemWriter<String, String, KeyDump<String>> writer = keyDumpWriter().build();
			LiveRedisItemReader<String, KeyDump<String>> liveReader = liveReader(liveKeyDumpReader(), 100000);
			RedisItemWriter<String, String, KeyDump<String>> liveWriter = keyDumpWriter().build();
			Assertions.assertTrue(liveReplication(reader, writer, liveReader, liveWriter));
		}

		@Test
		void liveSet() throws Exception {
			enableKeyspaceNotifications(sourceClient);
			String key = "myset";
			sourceConnection.sync().sadd(key, "1", "2", "3", "4", "5");
			LiveRedisItemReader<String, DataStructure<String>> reader = liveReader(liveDataStructureReader(), 100);
			JobExecution execution = runAsync(reader, dataStructureWriter().build());
			awaitOpen(reader);
			sourceConnection.sync().srem(key, "5");
			awaitTermination(execution);
			assertEquals(sourceConnection.sync().smembers(key), targetConnection.sync().smembers(key));
		}

		@Test
		void hyperLogLog() throws Exception {
			String key1 = "hll:1";
			sourceConnection.sync().pfadd(key1, "member:1", "member:2");
			String key2 = "hll:2";
			sourceConnection.sync().pfadd(key2, "member:1", "member:2", "member:3");
			RedisItemReader<byte[], DataStructure<byte[]>> reader = RedisItemReader
					.dataStructure(sourceByteArrayPool, jobRunner).build();
			RedisItemWriter<byte[], byte[], DataStructure<byte[]>> writer = RedisItemWriter
					.dataStructure(targetByteArrayPool).build();
			run(reader, writer);
			RedisModulesCommands<String, String> sourceSync = sourceConnection.sync();
			RedisModulesCommands<String, String> targetSync = targetConnection.sync();
			assertEquals(sourceSync.pfcount(key1), targetSync.pfcount(key1));
		}

		@Test
		void liveDataStructures() throws Exception {
			enableKeyspaceNotifications(sourceClient);
			RedisItemReader<String, DataStructure<String>> reader = dataStructureReader().build();
			RedisItemWriter<String, String, DataStructure<String>> writer = dataStructureWriter().build();
			LiveRedisItemReader<String, DataStructure<String>> liveReader = liveReader(liveDataStructureReader(),
					100000);
			RedisItemWriter<String, String, DataStructure<String>> liveWriter = dataStructureWriter().build();
			Assertions.assertTrue(liveReplication(reader, writer, liveReader, liveWriter));
		}

		private <T extends KeyValue<String>> boolean liveReplication(RedisItemReader<String, T> reader,
				RedisItemWriter<String, String, T> writer, LiveRedisItemReader<String, T> liveReader,
				RedisItemWriter<String, String, T> liveWriter) throws Exception {
			String name = name();
			reader.setName(name + "-reader");
			writer.setName(name + "-writer");
			liveReader.setName(name + "-liveReader");
			liveWriter.setName(name + "-liveWriter");
			generate(GeneratorReaderOptions.builder()
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
			generate(GeneratorReaderOptions.builder().types(Type.HASH, Type.LIST, Type.SET, Type.STRING, Type.ZSET)
					.expiration(IntRange.is(100)).range(IntRange.between(300, 1000)).build());
			awaitTermination(execution);
			awaitClosed(reader);
			awaitClosed(liveReader);
			awaitClosed(writer);
			awaitClosed(liveWriter);
			return compare();
		}

	}

	private void enableKeyspaceNotifications(AbstractRedisClient client) {
		sourceConnection.sync().configSet("notify-keyspace-events", "AK");
	}

	private void generate(GeneratorReaderOptions options) throws JobExecutionException {
		GeneratorItemReader reader = new GeneratorItemReader(options);
		reader.setMaxItemCount(options.getKeyRange().getMax() - options.getKeyRange().getMin() + 1);
		run(reader, null, RedisItemWriter.dataStructure(sourcePool).build(), DEFAULT_STEP_OPTIONS);
	}

	@Nested
	class Other {

		@Test
		void scanSizeEstimator() throws Exception {
			String pattern = GeneratorReaderOptions.DEFAULT_KEYSPACE + "*";
			int count = 12345;
			generate(GeneratorReaderOptions.builder().count(count).build());
			long expectedCount = sourceConnection.sync().dbsize();
			Builder estimator = ScanSizeEstimator.builder(sourcePool);
			assertEquals(expectedCount,
					estimator.options(ScanSizeEstimatorOptions.builder().match(pattern).build()).build().execute(),
					expectedCount / 10);
			assertEquals(expectedCount / GeneratorReaderOptions.defaultTypes().size(), estimator
					.options(ScanSizeEstimatorOptions.builder().type(Type.HASH.getString()).build()).build().execute(),
					expectedCount / 10);
		}

		@Test
		void compare() throws Exception {
			generate(GeneratorReaderOptions.builder().count(120).build());
			run(keyDumpReader().build(), keyDumpWriter().build());
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
				Type type = Type.of(targetConnection.sync().type(key));
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
				targetConnection.sync().set(key, "blah");
			}
			RedisItemReader<String, KeyComparison<String>> reader = comparisonReader();
			KeyComparisonCountItemWriter<String> writer = new KeyComparisonCountItemWriter<>();
			run(reader, writer);
			Results results = writer.getResults();
			long sourceCount = sourceConnection.sync().dbsize();
			assertEquals(sourceCount, results.getTotalCount());
			assertEquals(sourceCount, targetConnection.sync().dbsize() + deleted);
			assertEquals(typeChanges.size(), results.getCount(Status.TYPE));
			assertEquals(valueChanges.size(), results.getCount(Status.VALUE));
			assertEquals(ttlChanges.size(), results.getCount(Status.TTL));
			assertEquals(deleted, results.getCount(Status.MISSING));
		}

	}

}
