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
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.support.IteratorItemReader;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.Beers;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.search.IndexInfo;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.CreateOptions;
import com.redis.lettucemod.timeseries.DuplicatePolicy;
import com.redis.lettucemod.timeseries.RangeOptions;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.lettucemod.timeseries.TimeRange;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.DataStructure.Type;
import com.redis.spring.batch.common.IntRange;
import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Utils;
import com.redis.spring.batch.convert.GeoValueConverter;
import com.redis.spring.batch.convert.IdentityConverter;
import com.redis.spring.batch.convert.ScoredValueConverter;
import com.redis.spring.batch.reader.GeneratorReaderOptions;
import com.redis.spring.batch.reader.GeneratorReaderOptions.StreamOptions;
import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.spring.batch.reader.KeyEventType;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.QueueOptions;
import com.redis.spring.batch.reader.ScanKeyItemReader;
import com.redis.spring.batch.reader.ScanOptions;
import com.redis.spring.batch.reader.ScanSizeEstimator;
import com.redis.spring.batch.reader.ScanSizeEstimator.Builder;
import com.redis.spring.batch.reader.ScanSizeEstimatorOptions;
import com.redis.spring.batch.reader.SlotRangeFilter;
import com.redis.spring.batch.reader.StreamItemReader;
import com.redis.spring.batch.reader.StreamItemReader.StreamBuilder;
import com.redis.spring.batch.reader.StreamReaderOptions;
import com.redis.spring.batch.reader.StreamReaderOptions.AckPolicy;
import com.redis.spring.batch.writer.KeyComparisonCountItemWriter;
import com.redis.spring.batch.writer.KeyComparisonCountItemWriter.Results;
import com.redis.spring.batch.writer.WaitForReplication;
import com.redis.spring.batch.writer.WriterOptions;
import com.redis.spring.batch.writer.operation.Geoadd;
import com.redis.spring.batch.writer.operation.Hset;
import com.redis.spring.batch.writer.operation.JsonSet;
import com.redis.spring.batch.writer.operation.TsAdd;
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
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.models.stream.PendingMessages;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.search.Search;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

abstract class AbstractBatchTests extends AbstractTestBase {

	private static final QueueOptions NOTIFICATION_QUEUE_OPTIONS = QueueOptions.builder().capacity(100000).build();
	private static final String[] NOTIFICATION_PATTERNS = LiveRedisItemReader.patterns(0,
			LiveRedisItemReader.defaultKeyPatterns());
	private static final String JSON_BEER_1 = "{\"id\":\"1\",\"brewery_id\":\"812\",\"name\":\"Hocus Pocus\",\"abv\":\"4.5\",\"ibu\":\"0\",\"srm\":\"0\",\"upc\":\"0\",\"filepath\":\"\",\"descript\":\"Our take on a classic summer ale.  A toast to weeds, rays, and summer haze.  A light, crisp ale for mowing lawns, hitting lazy fly balls, and communing with nature, Hocus Pocus is offered up as a summer sacrifice to clodless days.\\n\\nIts malty sweetness finishes tart and crisp and is best apprediated with a wedge of orange.\",\"add_user\":\"0\",\"last_mod\":\"2010-07-22 20:00:20 UTC\",\"style_name\":\"Light American Wheat Ale or Lager\",\"cat_name\":\"Other Style\"}";
	private static final int BEER_COUNT = 1019;
	private static final int STREAM_MESSAGE_COUNT = 57;
	private static final String DEFAULT_CONSUMER_GROUP = "consumerGroup";

	private void enableKeyspaceNotifications(AbstractRedisClient client) {
		RedisModulesUtils.connection(client).sync().configSet("notify-keyspace-events", "AK");
	}

	@Nested
	class Modules {

		@Test
		void jsonSet(TestInfo testInfo) throws Exception {
			JsonSet<String, String, JsonNode> jsonSet = JsonSet
					.<String, JsonNode>key(n -> "beer:" + n.get("id").asText()).value(JsonNode::toString).path(".")
					.build();
			RedisItemWriter<String, String, JsonNode> writer = sourceWriter().operation(jsonSet);
			IteratorItemReader<JsonNode> reader = new IteratorItemReader<>(Beers.jsonNodeIterator());
			run(testInfo, reader, writer);
			Assertions.assertEquals(BEER_COUNT, sourceConnection.sync().keys("beer:*").size());
			Assertions.assertEquals(new ObjectMapper().readTree(JSON_BEER_1),
					new ObjectMapper().readTree(sourceConnection.sync().jsonGet("beer:1")));
		}

		@Test
		void tsAdd(TestInfo testInfo) throws Exception {
			String key = "ts:1";
			Random random = new Random();
			int count = 100;
			List<Sample> samples = new ArrayList<>(count);
			for (int index = 0; index < count; index++) {
				long timestamp = System.currentTimeMillis() - count + (index % (count / 2));
				samples.add(Sample.of(timestamp, random.nextDouble()));
			}
			ListItemReader<Sample> reader = new ListItemReader<>(samples);
			TsAdd<String, String, Sample> tsadd = TsAdd.<Sample>key(key).<String>sample(IdentityConverter.instance())
					.options(v -> AddOptions.<String, String>builder().policy(DuplicatePolicy.LAST).build()).build();
			RedisItemWriter<String, String, Sample> writer = sourceWriter().operation(tsadd);
			run(testInfo, reader, writer);
			Assertions.assertEquals(count / 2,
					sourceConnection.sync().tsRange(key, TimeRange.unbounded(), RangeOptions.builder().build()).size(),
					2);
		}

		@Test
		void beerIndex() throws Exception {
			Beers.populateIndex(sourceConnection);
			IndexInfo indexInfo = RedisModulesUtils.indexInfo(sourceConnection.sync().ftInfo(Beers.INDEX));
			Assertions.assertEquals(BEER_COUNT, indexInfo.getNumDocs());
		}

		@Test
		void tsComparator(TestInfo testInfo) throws Exception {
			sourceConnection.sync().tsAdd("ts:1", Sample.of(123));
			RedisItemReader<String, KeyComparison> reader = comparisonReader();
			KeyComparisonCountItemWriter writer = new KeyComparisonCountItemWriter();
			run(testInfo, reader, writer);
			Assertions.assertEquals(1, writer.getResults().getCount(Status.MISSING));
		}

		@Test
		void replicateJSON(TestInfo testInfo) throws Exception {
			sourceConnection.sync().jsonSet("json:1", "$", JSON_BEER_1);
			sourceConnection.sync().jsonSet("json:2", "$", JSON_BEER_1);
			sourceConnection.sync().jsonSet("json:3", "$", JSON_BEER_1);
			run(testInfo, sourceReader().dataStructure(), targetWriter().dataStructure());
			Assertions.assertTrue(compare(testInfo));
		}

		@Test
		void replicateTimeSeries(TestInfo testInfo) throws Exception {
			String key = "ts:1";
			sourceConnection.sync().tsCreate(key,
					CreateOptions.<String, String>builder().policy(DuplicatePolicy.LAST).build());
			sourceConnection.sync().tsAdd(key, Sample.of(1000, 1));
			sourceConnection.sync().tsAdd(key, Sample.of(1001, 2));
			sourceConnection.sync().tsAdd(key, Sample.of(1003, 3));
			run(testInfo, sourceReader().dataStructure(), targetWriter().dataStructure());
			Assertions.assertTrue(compare(testInfo));
		}
	}

	@Nested
	class Writer {

		@Test
		void writeWait(TestInfo testInfo) throws Exception {
			List<Map<String, String>> maps = new ArrayList<>();
			for (int index = 0; index < 100; index++) {
				Map<String, String> body = new HashMap<>();
				body.put("id", String.valueOf(index));
				body.put("field1", "value1");
				body.put("field2", "value2");
				maps.add(body);
			}
			ListItemReader<Map<String, String>> reader = new ListItemReader<>(maps);
			RedisItemWriter<String, String, Map<String, String>> writer = sourceWriter()
					.options(WriterOptions.builder()
							.waitForReplication(WaitForReplication.of(1, Duration.ofMillis(300))).build())
					.operation(Hset.<String, Map<String, String>>key(m -> "hash:" + m.remove("id"))
							.map(IdentityConverter.instance()).build());
			Job job = job(testInfo, step(testInfo, reader, null, writer));
			JobExecution execution = run(job);
			List<Throwable> exceptions = execution.getAllFailureExceptions();
			assertEquals("Insufficient replication level - expected: 1, actual: 0", exceptions.get(0).getMessage());
		}

		@Test
		void writeHash(TestInfo testInfo) throws Exception {
			List<Map<String, String>> maps = new ArrayList<>();
			for (int index = 0; index < 100; index++) {
				Map<String, String> body = new HashMap<>();
				body.put("id", String.valueOf(index));
				body.put("field1", "value1");
				body.put("field2", "value2");
				maps.add(body);
			}
			ListItemReader<Map<String, String>> reader = new ListItemReader<>(maps);
			RedisItemWriter<String, String, Map<String, String>> writer = sourceWriter()
					.operation(Hset.<String, Map<String, String>>key(m -> "hash:" + m.remove("id"))
							.map(IdentityConverter.instance()).build());
			run(testInfo, reader, writer);
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
		void writeGeo(TestInfo testInfo) throws Exception {
			ListItemReader<Geo> reader = new ListItemReader<>(
					Arrays.asList(new Geo("Venice Breakwater", -118.476056, 33.985728),
							new Geo("Long Beach National", -73.667022, 40.582739)));
			GeoValueConverter<String, Geo> value = new GeoValueConverter<>(Geo::getMember, Geo::getLongitude,
					Geo::getLatitude);
			RedisItemWriter<String, String, Geo> writer = sourceWriter()
					.operation(Geoadd.<String, Geo>key("geoset").value(value).build());
			run(testInfo, reader, writer);
			Set<String> radius1 = sourceConnection.sync().georadius("geoset", -118, 34, 100, GeoArgs.Unit.mi);
			assertEquals(1, radius1.size());
			assertTrue(radius1.contains("Venice Breakwater"));
		}

		@Test
		void writeHashDel(TestInfo testInfo) throws Exception {
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
			RedisItemWriter<String, String, Map.Entry<String, Map<String, String>>> writer = sourceWriter()
					.operation(Hset.<String, Entry<String, Map<String, String>>>key(e -> "hash:" + e.getKey())
							.map(Map.Entry::getValue).build());
			run(testInfo, reader, writer);
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
		void writeSortedSet(TestInfo testInfo) throws Exception {
			List<ZValue> values = new ArrayList<>();
			for (int index = 0; index < 100; index++) {
				values.add(new ZValue(String.valueOf(index), index % 10));
			}
			ListItemReader<ZValue> reader = new ListItemReader<>(values);
			ScoredValueConverter<String, ZValue> converter = new ScoredValueConverter<>(ZValue::getMember,
					ZValue::getScore);
			RedisItemWriter<String, String, ZValue> writer = sourceWriter()
					.operation(Zadd.<String, ZValue>key("zset").value(converter).build());
			run(testInfo, reader, writer);
			RedisModulesCommands<String, String> sync = sourceConnection.sync();
			assertEquals(1, sync.dbsize());
			assertEquals(values.size(), sync.zcard("zset"));
			assertEquals(60,
					sync.zrangebyscore("zset", Range.from(Range.Boundary.including(0), Range.Boundary.including(5)))
							.size());
		}

		@Test
		void writeDataStructures(TestInfo testInfo) throws Exception {
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
			RedisItemWriter<String, String, DataStructure<String>> writer = sourceWriter().dataStructure();
			run(testInfo, reader, writer);
			RedisModulesCommands<String, String> sync = sourceConnection.sync();
			List<String> keys = sync.keys("hash:*");
			assertEquals(count, keys.size());
		}
	}

	@Nested
	class Reader {

		@Test
		void metrics(TestInfo testInfo) throws Exception {
			Metrics.globalRegistry.getMeters().forEach(Metrics.globalRegistry::remove);
			SimpleMeterRegistry registry = new SimpleMeterRegistry(new SimpleConfig() {
				@Override
				public String get(String key) {
					return null;
				}

				@Override
				public Duration step() {
					return Duration.ofMillis(1);
				}
			}, Clock.SYSTEM);
			Metrics.addRegistry(registry);
			generate(testInfo);
			RedisItemReader<String, DataStructure<String>> reader = sourceReader().dataStructure();
			reader.open(new ExecutionContext());
			Search search = registry.find("spring.batch.redis.reader.queue.size");
			Assertions.assertNotNull(search.gauge());
			reader.close();
			registry.close();
			Metrics.globalRegistry.getMeters().forEach(Metrics.globalRegistry::remove);
		}

		@Test
		void filterKeySlot(TestInfo testInfo) throws Exception {
			enableKeyspaceNotifications(sourceClient);
			LiveRedisItemReader<String, DataStructure<String>> reader = sourceReader().live()
					.flushingOptions(DEFAULT_FLUSHING_OPTIONS).keyFilter(SlotRangeFilter.of(0, 8000)).dataStructure();
			SynchronizedListItemWriter<DataStructure<String>> writer = new SynchronizedListItemWriter<>();
			JobExecution execution = runAsync(testInfo, reader, writer);
			int count = 100;
			generate(testInfo, GeneratorReaderOptions.builder().count(count).build());
			awaitTermination(execution);
			Assertions.assertFalse(writer.getWrittenItems().stream().map(DataStructure::getKey).map(SlotHash::getSlot)
					.anyMatch(s -> s < 0 || s > 8000));
		}

		@Test
		void keyspaceNotificationsReader(TestInfo testInfo) throws Exception {
			enableKeyspaceNotifications(sourceClient);
			try (KeyspaceNotificationItemReader<String, String> reader = new KeyspaceNotificationItemReader<>(
					sourceClient, StringCodec.UTF8, NOTIFICATION_QUEUE_OPTIONS, NOTIFICATION_PATTERNS)) {
				reader.open(new ExecutionContext());
				generate(testInfo, GeneratorReaderOptions.builder().types(Type.HASH, Type.LIST, Type.SET, Type.STREAM,
						Type.STRING, Type.ZSET, Type.TIMESERIES, Type.JSON).build());
				awaitUntil(() -> reader.getQueue().size() == 100);
				Assertions.assertEquals(KeyEventType.SET, reader.getQueue().remove().getEventType());
				assertEventTypes(reader, KeyEventType.SET, KeyEventType.HSET, KeyEventType.JSON_SET, KeyEventType.RPUSH,
						KeyEventType.SADD, KeyEventType.ZADD, KeyEventType.XADD, KeyEventType.TS_ADD);
			}
		}

		@Test
		void scanKeyItemReader(TestInfo testInfo)
				throws UnexpectedInputException, ParseException, NonTransientResourceException, Exception {
			int count = 100;
			generate(testInfo, GeneratorReaderOptions.builder().count(count).build());
			try (ScanKeyItemReader<String, String> reader = new ScanKeyItemReader<>(sourceClient, StringCodec.UTF8,
					ScanOptions.builder().build())) {
				reader.open(new ExecutionContext());
				Assertions.assertEquals(count, Utils.readAll(reader).size());
			}
		}

		private void assertEventTypes(KeyspaceNotificationItemReader<String, String> reader,
				KeyEventType... expectedEventTypes) {
			Set<KeyEventType> actualEventTypes = new LinkedHashSet<>();
			while (!reader.getQueue().isEmpty()) {
				actualEventTypes.add(reader.getQueue().remove().getEventType());
			}
			Assertions.assertEquals(new LinkedHashSet<>(Arrays.asList(expectedEventTypes)), actualEventTypes);
		}

		@Test
		void dedupeKeyspaceNotifications() throws Exception {
			enableKeyspaceNotifications(sourceClient);
			try (KeyspaceNotificationItemReader<String, String> reader = new KeyspaceNotificationItemReader<>(
					sourceClient, StringCodec.UTF8, NOTIFICATION_QUEUE_OPTIONS, NOTIFICATION_PATTERNS)) {
				reader.open(new ExecutionContext());
				RedisModulesCommands<String, String> commands = sourceConnection.sync();
				String key = "key1";
				commands.zadd(key, 1, "member1");
				commands.zadd(key, 2, "member2");
				commands.zadd(key, 3, "member3");
				awaitUntil(() -> reader.getQueue().size() == 1);
				Assertions.assertEquals(key, reader.read());
			}
		}

		@Test
		void readThreads(TestInfo testInfo) throws Exception {
			generate(testInfo);
			RedisItemReader<String, DataStructure<String>> reader = sourceReader().dataStructure();
			setName(reader, testInfo);
			SynchronizedItemStreamReader<DataStructure<String>> synchronizedReader = new SynchronizedItemStreamReader<>();
			synchronizedReader.setDelegate(reader);
			synchronizedReader.afterPropertiesSet();
			SynchronizedListItemWriter<DataStructure<String>> writer = new SynchronizedListItemWriter<>();
			int threads = 4;
			ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
			taskExecutor.setMaxPoolSize(threads);
			taskExecutor.setCorePoolSize(threads);
			taskExecutor.afterPropertiesSet();
			TaskletStep step = step(testInfo, synchronizedReader, writer).taskExecutor(taskExecutor)
					.throttleLimit(threads).build();
			Job job = job(testInfo).start(step).build();
			run(job);
			awaitClosed(reader);
			awaitUntilFalse(writer::isOpen);
			assertEquals(sourceConnection.sync().dbsize(), writer.getWrittenItems().size());
		}

		@Test
		void readLive(TestInfo testInfo) throws Exception {
			enableKeyspaceNotifications(sourceClient);
			LiveRedisItemReader<String, KeyDump<String>> reader = sourceLiveReader(10000).keyDump();
			SynchronizedListItemWriter<KeyDump<String>> writer = new SynchronizedListItemWriter<>();
			JobExecution execution = runAsync(testInfo, reader, writer);
			awaitOpen(reader);
			generate(testInfo, GeneratorReaderOptions.builder().count(123).types(Type.HASH, Type.STRING).build());
			awaitTermination(execution);
			awaitClosed(reader);
			Assertions.assertEquals(sourceConnection.sync().dbsize(), writer.getWrittenItems().size());
		}

		@Test
		void scanSizeEstimator(TestInfo testInfo) throws Exception {
			String pattern = GeneratorReaderOptions.DEFAULT_KEYSPACE + "*";
			int count = 12345;
			generate(testInfo, GeneratorReaderOptions.builder().count(count).build());
			long expectedCount = sourceConnection.sync().dbsize();
			Builder estimator = ScanSizeEstimator.client(sourceClient);
			assertEquals(expectedCount,
					estimator.options(ScanSizeEstimatorOptions.builder().match(pattern).build()).build().execute(),
					expectedCount / 10);
			assertEquals(expectedCount / GeneratorReaderOptions.defaultTypes().size(), estimator
					.options(ScanSizeEstimatorOptions.builder().type(Type.HASH.getString()).build()).build().execute(),
					expectedCount / 10);
		}

	}

	@Nested
	class Stream {

		private void generateStreams(TestInfo testInfo) throws JobExecutionException {
			generate(testInfo(testInfo, "streams"),
					GeneratorReaderOptions.builder().types(Type.STREAM)
							.streamOptions(StreamOptions.builder().messageCount(STREAM_MESSAGE_COUNT).build()).count(3)
							.build());
		}

		private StreamBuilder<String, String> streamReader() {
			if (sourceClient instanceof RedisModulesClusterClient) {
				return StreamItemReader.client((RedisModulesClusterClient) sourceClient);
			}
			return StreamItemReader.client((RedisModulesClient) sourceClient);
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

		@Test
		void writeStreamTx(TestInfo testInfo) throws Exception {
			String stream = "stream:1";
			List<Map<String, String>> messages = new ArrayList<>();
			for (int index = 0; index < 100; index++) {
				Map<String, String> body = new HashMap<>();
				body.put("field1", "value1");
				body.put("field2", "value2");
				messages.add(body);
			}
			ListItemReader<Map<String, String>> reader = new ListItemReader<>(messages);
			RedisItemWriter<String, String, Map<String, String>> writer = sourceWriter()
					.options(WriterOptions.builder().multiExec(true).build()).operation(
							Xadd.<String, Map<String, String>>key(stream).body(IdentityConverter.instance()).build());
			run(testInfo, reader, writer);
			RedisStreamCommands<String, String> sync = sourceConnection.sync();
			Assertions.assertEquals(messages.size(), sync.xlen(stream));
			List<StreamMessage<String, String>> xrange = sync.xrange(stream, Range.create("-", "+"));
			for (int index = 0; index < xrange.size(); index++) {
				StreamMessage<String, String> message = xrange.get(index);
				Assertions.assertEquals(messages.get(index), message.getBody());
			}
		}

		@Test
		void readStreamAutoAck() throws InterruptedException {
			String stream = "stream1";
			String consumerGroup = "batchtests-readStreamAutoAck";
			Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
			final StreamItemReader<String, String> reader = streamReader().stream(stream).consumer(consumer)
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
			awaitUntil(() -> messages.addAll(reader.readMessages()));
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
			final StreamItemReader<String, String> reader = streamReader().stream(stream).consumer(consumer)
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
			awaitUntil(() -> messages.addAll(reader.readMessages()));
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
			final StreamItemReader<String, String> reader = streamReader().stream(stream).consumer(consumer)
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
			awaitUntil(() -> messages.addAll(reader.readMessages()));
			Assertions.assertEquals(3, messages.size());

			List<StreamMessage<String, String>> recoveredMessages = new ArrayList<>();
			sourceConnection.sync().xadd(stream, body);
			sourceConnection.sync().xadd(stream, body);
			sourceConnection.sync().xadd(stream, body);

			reader.close();

			final StreamItemReader<String, String> reader2 = streamReader().stream(stream).consumer(consumer)
					.options(StreamReaderOptions.builder().ackPolicy(AckPolicy.MANUAL).build()).build();
			reader2.open(new ExecutionContext());

			awaitUntil(() -> recoveredMessages.addAll(reader2.readMessages()));
			awaitUntil(() -> !recoveredMessages.addAll(reader2.readMessages()));

			Assertions.assertEquals(6, recoveredMessages.size());
		}

		@Test
		void readStreamManualAckRecoverUncommitted() throws InterruptedException {
			String stream = "stream1";
			String consumerGroup = "batchtests-readStreamManualAckRecoverUncommitted";
			Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
			final StreamItemReader<String, String> reader = streamReader().stream(stream).consumer(consumer)
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
			awaitUntil(() -> messages.addAll(reader.readMessages()));
			Assertions.assertEquals(3, messages.size());
			sourceConnection.sync().xack(stream, consumerGroup, messages.get(0).getId(), messages.get(1).getId());

			List<StreamMessage<String, String>> recoveredMessages = new ArrayList<>();
			String id4 = sourceConnection.sync().xadd(stream, body);
			String id5 = sourceConnection.sync().xadd(stream, body);
			String id6 = sourceConnection.sync().xadd(stream, body);
			reader.close();

			final StreamItemReader<String, String> reader2 = streamReader().stream(stream).consumer(consumer).options(
					StreamReaderOptions.builder().ackPolicy(AckPolicy.MANUAL).offset(messages.get(1).getId()).build())
					.build();
			reader2.open(new ExecutionContext());

			// Wait until task.poll() doesn't return any more records
			awaitUntil(() -> recoveredMessages.addAll(reader2.readMessages()));
			awaitUntil(() -> !recoveredMessages.addAll(reader2.readMessages()));
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
			final StreamItemReader<String, String> reader = streamReader().stream(stream).consumer(consumer)
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
			awaitUntil(() -> sourceRecords.addAll(reader.readMessages()));
			Assertions.assertEquals(3, sourceRecords.size());

			List<StreamMessage<String, String>> recoveredRecords = new ArrayList<>();
			String id4 = sourceConnection.sync().xadd(stream, body);
			String id5 = sourceConnection.sync().xadd(stream, body);
			String id6 = sourceConnection.sync().xadd(stream, body);

			reader.close();

			final StreamItemReader<String, String> reader2 = streamReader().stream(stream).consumer(consumer)
					.options(StreamReaderOptions.builder().ackPolicy(AckPolicy.MANUAL).offset(id3).build()).build();
			reader2.open(new ExecutionContext());

			// Wait until task.poll() doesn't return any more records
			awaitUntil(() -> recoveredRecords.addAll(reader2.readMessages()));
			awaitUntil(() -> !recoveredRecords.addAll(reader2.readMessages()));
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
			final StreamItemReader<String, String> reader = streamReader().stream(stream).consumer(consumer)
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
			awaitUntil(() -> sourceRecords.addAll(reader.readMessages()));
			Assertions.assertEquals(3, sourceRecords.size());

			List<StreamMessage<String, String>> recoveredRecords = new ArrayList<>();
			String id4 = sourceConnection.sync().xadd(stream, body);
			String id5 = sourceConnection.sync().xadd(stream, body);
			String id6 = sourceConnection.sync().xadd(stream, body);
			reader.close();

			final StreamItemReader<String, String> reader2 = streamReader().stream(stream).consumer(consumer)
					.options(StreamReaderOptions.builder().ackPolicy(AckPolicy.AUTO).build()).build();
			reader2.open(new ExecutionContext());

			// Wait until task.poll() doesn't return any more records
			awaitUntil(() -> recoveredRecords.addAll(reader2.readMessages()));
			awaitUntil(() -> !recoveredRecords.addAll(reader2.readMessages()));
			List<String> recoveredIds = recoveredRecords.stream().map(StreamMessage::getId)
					.collect(Collectors.toList());
			Assertions.assertEquals(Arrays.asList(id4, id5, id6), recoveredIds, "recoveredIds");

			PendingMessages pending = sourceConnection.sync().xpending(stream, consumerGroup);
			Assertions.assertEquals(0, pending.getCount(), "pending message count");
			reader2.close();
		}

		@Test
		void readMessages(TestInfo testInfo) throws Exception {
			generateStreams(testInfo);
			List<String> keys = ScanIterator
					.scan(sourceConnection.sync(), KeyScanArgs.Builder.type(Type.STREAM.getString())).stream()
					.collect(Collectors.toList());
			for (String key : keys) {
				long count = sourceConnection.sync().xlen(key);
				StreamItemReader<String, String> reader = streamReader().stream(key)
						.consumer(Consumer.from("batchtests-readmessages", "consumer1")).build();
				reader.open(new ExecutionContext());
				List<StreamMessage<String, String>> messages = new ArrayList<>();
				awaitUntil(() -> {
					messages.addAll(reader.readMessages());
					return messages.size() == count;
				});
				assertMessageBody(messages);
				awaitUntil(() -> reader.ack(reader.readMessages()) == 0);
				reader.close();
			}
		}

		@Test
		void streamReaderJob(TestInfo testInfo) throws Exception {
			generateStreams(testInfo);
			List<String> keys = ScanIterator
					.scan(sourceConnection.sync(), KeyScanArgs.Builder.type(Type.STREAM.getString())).stream()
					.collect(Collectors.toList());
			for (String key : keys) {
				Consumer<String> consumer = Consumer.from("batchtests-readstreamjob", "consumer1");
				StreamItemReader<String, String> reader = streamReader().stream(key).consumer(consumer).build();
				SynchronizedListItemWriter<StreamMessage<String, String>> writer = new SynchronizedListItemWriter<>();
				run(testInfo(testInfo, key), reader, writer);
				Assertions.assertEquals(STREAM_MESSAGE_COUNT, writer.getWrittenItems().size());
				assertMessageBody(writer.getWrittenItems());
			}
		}

		@Test
		void readMultipleStreams(TestInfo testInfo) throws Exception {
			generateStreams(testInfo(testInfo, "streams"));
			final List<String> keys = ScanIterator
					.scan(sourceConnection.sync(), KeyScanArgs.Builder.type(Type.STREAM.getString())).stream()
					.collect(Collectors.toList());
			for (String key : keys) {
				StreamItemReader<String, String> reader1 = streamReader().stream(key)
						.consumer(Consumer.from(DEFAULT_CONSUMER_GROUP, "consumer1"))
						.options(StreamReaderOptions.builder().ackPolicy(AckPolicy.MANUAL).build()).build();
				StreamItemReader<String, String> reader2 = streamReader().stream(key)
						.consumer(Consumer.from(DEFAULT_CONSUMER_GROUP, "consumer2"))
						.options(StreamReaderOptions.builder().ackPolicy(AckPolicy.MANUAL).build()).build();
				SynchronizedListItemWriter<StreamMessage<String, String>> writer1 = new SynchronizedListItemWriter<>();
				JobExecution execution1 = runAsync(testInfo(testInfo, key, "1"), reader1, writer1);
				SynchronizedListItemWriter<StreamMessage<String, String>> writer2 = new SynchronizedListItemWriter<>();
				JobExecution execution2 = runAsync(testInfo(testInfo, key, "2"), reader2, writer2);
				awaitTermination(execution1);
				awaitTermination(execution2);
				awaitClosed(reader1);
				awaitClosed(reader2);
				awaitUntil(() -> STREAM_MESSAGE_COUNT == writer1.getWrittenItems().size()
						+ writer2.getWrittenItems().size());
				assertMessageBody(writer1.getWrittenItems());
				assertMessageBody(writer2.getWrittenItems());
				RedisModulesCommands<String, String> sync = sourceConnection.sync();
				Assertions.assertEquals(STREAM_MESSAGE_COUNT, sync.xpending(key, DEFAULT_CONSUMER_GROUP).getCount());
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
		void writeStream(TestInfo testInfo) throws Exception {
			String stream = "stream:0";
			List<Map<String, String>> messages = new ArrayList<>();
			for (int index = 0; index < 100; index++) {
				Map<String, String> body = new HashMap<>();
				body.put("field1", "value1");
				body.put("field2", "value2");
				messages.add(body);
			}
			ListItemReader<Map<String, String>> reader = new ListItemReader<>(messages);
			RedisItemWriter<String, String, Map<String, String>> writer = sourceWriter().operation(
					Xadd.<String, Map<String, String>>key(stream).<String>body(IdentityConverter.instance()).build());
			run(testInfo, reader, writer);
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
		void dataStructures(TestInfo testInfo) throws Exception {
			generate(testInfo, GeneratorReaderOptions.builder().count(100).build());
			RedisItemReader<String, DataStructure<String>> reader = sourceReader().dataStructure();
			run(testInfo, reader, targetDataStructureWriter());
			awaitClosed(reader);
			Assertions.assertTrue(compare(testInfo));
		}

		@Test
		void dumpAndRestore(TestInfo testInfo) throws Exception {
			generate(testInfo, GeneratorReaderOptions.builder().count(100).build());
			RedisItemReader<String, KeyDump<String>> reader = sourceReader().keyDump();
			run(testInfo, reader, targetKeyDumpWriter());
			awaitClosed(reader);
			Assertions.assertTrue(compare(testInfo));
		}

		@Test
		void byteArrayCodec(TestInfo testInfo) throws Exception {
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
			run(testInfo, sourceReader().keyDump(), targetKeyDumpWriter());
			Awaitility.await().until(() -> sourceConnection.sync().dbsize() == targetConnection.sync().dbsize());
		}

		@Test
		void liveOnly(TestInfo testInfo) throws JobExecutionException {
			enableKeyspaceNotifications(sourceClient);
			LiveRedisItemReader<String, KeyDump<String>> reader = sourceLiveReader(100000).keyDump();
			RedisItemWriter<String, String, KeyDump<String>> writer = targetKeyDumpWriter();
			JobExecution execution = runAsync(testInfo, reader, writer);
			awaitOpen(reader);
			generate(testInfo, GeneratorReaderOptions.builder()
					.types(Type.HASH, Type.LIST, Type.SET, Type.STRING, Type.ZSET).build());
			awaitTermination(execution);
			awaitClosed(reader);
			awaitClosed(writer);
			Assertions.assertTrue(compare(testInfo));
		}

		@Test
		void liveReplication(TestInfo testInfo) throws Exception {
			enableKeyspaceNotifications(sourceClient);
			RedisItemReader<String, KeyDump<String>> reader = sourceReader().keyDump();
			RedisItemWriter<String, String, KeyDump<String>> writer = targetKeyDumpWriter();
			LiveRedisItemReader<String, KeyDump<String>> liveReader = sourceLiveReader(100000).keyDump();
			RedisItemWriter<String, String, KeyDump<String>> liveWriter = targetKeyDumpWriter();
			Assertions.assertTrue(liveReplication(testInfo, reader, writer, liveReader, liveWriter));
		}

		@Test
		void liveSet(TestInfo testInfo) throws Exception {
			enableKeyspaceNotifications(sourceClient);
			String key = "myset";
			sourceConnection.sync().sadd(key, "1", "2", "3", "4", "5");
			LiveRedisItemReader<String, DataStructure<String>> reader = sourceLiveReader(100).dataStructure();
			JobExecution execution = runAsync(testInfo, reader, targetDataStructureWriter());
			awaitOpen(reader);
			sourceConnection.sync().srem(key, "5");
			awaitTermination(execution);
			assertEquals(sourceConnection.sync().smembers(key), targetConnection.sync().smembers(key));
		}

		@Test
		void invalidConnection(TestInfo testInfo) throws Exception {
			try (RedisModulesClient badSourceClient = RedisModulesClient.create("redis://badhost:6379");
					RedisModulesClient badTargetClient = RedisModulesClient.create("redis://badhost:6379")) {
				RedisItemReader<byte[], DataStructure<byte[]>> reader = reader(badSourceClient, ByteArrayCodec.INSTANCE)
						.dataStructure();
				RedisItemWriter<byte[], byte[], DataStructure<byte[]>> writer = writer(badTargetClient,
						ByteArrayCodec.INSTANCE).dataStructure();
				JobExecution execution = run(testInfo, reader, writer);
				Assertions.assertTrue(execution.getStatus().isUnsuccessful());
			}
		}

		@Test
		void hyperLogLog(TestInfo testInfo) throws Exception {
			String key1 = "hll:1";
			sourceConnection.sync().pfadd(key1, "member:1", "member:2");
			String key2 = "hll:2";
			sourceConnection.sync().pfadd(key2, "member:1", "member:2", "member:3");
			RedisItemReader<byte[], DataStructure<byte[]>> reader = sourceReader(ByteArrayCodec.INSTANCE)
					.dataStructure();
			RedisItemWriter<byte[], byte[], DataStructure<byte[]>> writer = targetWriter(ByteArrayCodec.INSTANCE)
					.dataStructure();
			run(testInfo, reader, writer);
			RedisModulesCommands<String, String> sourceSync = sourceConnection.sync();
			RedisModulesCommands<String, String> targetSync = targetConnection.sync();
			assertEquals(sourceSync.pfcount(key1), targetSync.pfcount(key1));
		}

		@Test
		void liveDataStructures(TestInfo testInfo) throws Exception {
			enableKeyspaceNotifications(sourceClient);
			RedisItemReader<String, DataStructure<String>> reader = sourceReader().dataStructure();
			RedisItemWriter<String, String, DataStructure<String>> writer = targetDataStructureWriter();
			LiveRedisItemReader<String, DataStructure<String>> liveReader = sourceLiveReader(100000).dataStructure();
			RedisItemWriter<String, String, DataStructure<String>> liveWriter = targetDataStructureWriter();
			Assertions.assertTrue(liveReplication(testInfo, reader, writer, liveReader, liveWriter));
		}

		private <T extends KeyValue<String>> boolean liveReplication(TestInfo testInfo,
				RedisItemReader<String, T> reader, RedisItemWriter<String, String, T> writer,
				LiveRedisItemReader<String, T> liveReader, RedisItemWriter<String, String, T> liveWriter)
				throws Exception {
			generate(testInfo(testInfo, "generate"), GeneratorReaderOptions.builder()
					.types(Type.HASH, Type.LIST, Type.SET, Type.STREAM, Type.STRING, Type.ZSET).count(300).build());
			TaskletStep step = step(testInfo(testInfo, "step"), reader, null, writer).build();
			SimpleFlow flow = new FlowBuilder<SimpleFlow>(name(testInfo(testInfo, "snapshotFlow"))).start(step).build();
			TaskletStep liveStep = flushingStep(testInfo(testInfo, "liveStep"), liveReader, liveWriter).build();
			SimpleFlow liveFlow = new FlowBuilder<SimpleFlow>(name(testInfo(testInfo, "liveFlow"))).start(liveStep)
					.build();
			Job job = job(testInfo).start(new FlowBuilder<SimpleFlow>(name(testInfo(testInfo, "flow")))
					.split(new SimpleAsyncTaskExecutor()).add(liveFlow, flow).build()).build().build();
			JobExecution execution = runAsync(job);
			awaitOpen(liveReader);
			generate(testInfo(testInfo, "generateLive"),
					GeneratorReaderOptions.builder().types(Type.HASH, Type.LIST, Type.SET, Type.STRING, Type.ZSET)
							.expiration(IntRange.is(100)).keyRange(IntRange.between(300, 1000)).build());
			awaitTermination(execution);
			awaitClosed(reader);
			awaitClosed(liveReader);
			awaitClosed(writer);
			awaitClosed(liveWriter);
			return compare(testInfo);
		}

		@Test
		void compararator(TestInfo testInfo) throws Exception {
			generate(testInfo, GeneratorReaderOptions.builder().count(120).build());
			run(testInfo(testInfo, "replicate"), sourceReader().keyDump(), targetKeyDumpWriter());
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
			RedisItemReader<String, KeyComparison> reader = comparisonReader();
			KeyComparisonCountItemWriter writer = new KeyComparisonCountItemWriter();
			run(testInfo(testInfo, "compare"), reader, writer);
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
