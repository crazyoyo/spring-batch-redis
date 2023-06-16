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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.support.IteratorItemReader;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.util.Assert;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.Beers;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.search.IndexInfo;
import com.redis.lettucemod.search.Suggestion;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.CreateOptions;
import com.redis.lettucemod.timeseries.DuplicatePolicy;
import com.redis.lettucemod.timeseries.RangeOptions;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.lettucemod.timeseries.TimeRange;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.RedisItemWriter.WriterBuilder;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.FilteringItemProcessor;
import com.redis.spring.batch.common.IntRange;
import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Utils;
import com.redis.spring.batch.convert.GeoValueConverter;
import com.redis.spring.batch.convert.SampleConverter;
import com.redis.spring.batch.convert.ScoredValueConverter;
import com.redis.spring.batch.convert.SuggestionConverter;
import com.redis.spring.batch.reader.DataStructureReadOperation;
import com.redis.spring.batch.reader.GeneratorItemReader;
import com.redis.spring.batch.reader.GeneratorItemReader.HashOptions;
import com.redis.spring.batch.reader.GeneratorItemReader.StreamOptions;
import com.redis.spring.batch.reader.GeneratorItemReader.Type;
import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.spring.batch.reader.KeyDumpReadOperation;
import com.redis.spring.batch.reader.KeyEventType;
import com.redis.spring.batch.reader.KeyspaceNotification;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.QueueOptions;
import com.redis.spring.batch.reader.ScanKeyItemReader;
import com.redis.spring.batch.reader.ScanSizeEstimator;
import com.redis.spring.batch.reader.SlotRangeFilter;
import com.redis.spring.batch.reader.StreamAckPolicy;
import com.redis.spring.batch.reader.StreamItemReader;
import com.redis.spring.batch.reader.StringDataStructureReadOperation;
import com.redis.spring.batch.writer.KeyComparisonCountItemWriter;
import com.redis.spring.batch.writer.KeyComparisonCountItemWriter.Results;
import com.redis.spring.batch.writer.MergePolicy;
import com.redis.spring.batch.writer.operation.Geoadd;
import com.redis.spring.batch.writer.operation.Hset;
import com.redis.spring.batch.writer.operation.JsonSet;
import com.redis.spring.batch.writer.operation.Sadd;
import com.redis.spring.batch.writer.operation.Sugadd;
import com.redis.spring.batch.writer.operation.SugaddIncr;
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
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.ScoredValue;
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

	private static final String JSON_BEER_1 = "{\"id\":\"1\",\"brewery_id\":\"812\",\"name\":\"Hocus Pocus\",\"abv\":\"4.5\",\"ibu\":\"0\",\"srm\":\"0\",\"upc\":\"0\",\"filepath\":\"\",\"descript\":\"Our take on a classic summer ale.  A toast to weeds, rays, and summer haze.  A light, crisp ale for mowing lawns, hitting lazy fly balls, and communing with nature, Hocus Pocus is offered up as a summer sacrifice to clodless days.\\n\\nIts malty sweetness finishes tart and crisp and is best apprediated with a wedge of orange.\",\"add_user\":\"0\",\"last_mod\":\"2010-07-22 20:00:20 UTC\",\"style_name\":\"Light American Wheat Ale or Lager\",\"cat_name\":\"Other Style\"}";
	private static final int BEER_COUNT = 1019;
	private static final int STREAM_MESSAGE_COUNT = 57;
	private static final String DEFAULT_CONSUMER_GROUP = "consumerGroup";

	private void enableKeyspaceNotifications(AbstractRedisClient client) {
		RedisModulesUtils.connection(client).sync().configSet("notify-keyspace-events", "AK");
	}

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
		Hset<String, String, Map<String, String>> hset = new Hset<>(m -> "hash:" + m.remove("id"), Function.identity());
		RedisItemWriter<String, String, Map<String, String>> writer = new WriterBuilder<>(sourceClient,
				StringCodec.UTF8).waitReplicas(1).waitTimeout(Duration.ofMillis(300)).operation(hset);
		JobExecution execution = run(testInfo, reader, writer);
		List<Throwable> exceptions = execution.getAllFailureExceptions();
		assertEquals("Insufficient replication level - expected: 1, actual: 0",
				exceptions.get(0).getCause().getMessage());
	}

	private <I, O> Job job(TestInfo testInfo, ItemReader<I> reader, ItemWriter<O> writer) {
		return job(testInfo).start(step(testInfo, reader, writer).build()).build();
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
		Hset<String, String, Map<String, String>> hset = new Hset<>(m -> "hash:" + m.remove("id"), Function.identity());
		RedisItemWriter<String, String, Map<String, String>> writer = new WriterBuilder<>(sourceClient,
				StringCodec.UTF8).operation(hset);
		run(testInfo, reader, writer);
		assertEquals(maps.size(), sourceConnection.sync().keys("hash:*").size());
		for (int index = 0; index < maps.size(); index++) {
			Map<String, String> hash = sourceConnection.sync().hgetall("hash:" + index);
			assertEquals(maps.get(index), hash);
		}
	}

	@Test
	void writeDataStructuresOverwrite(TestInfo testInfo) throws Exception {
		GeneratorItemReader gen1 = new GeneratorItemReader();
		gen1.setMaxItemCount(100);
		gen1.withTypes(Arrays.asList(Type.HASH));
		gen1.withHashOptions(HashOptions.builder().fieldCount(IntRange.is(5)).build());
		generate(testInfo, sourceClient, gen1);
		GeneratorItemReader gen2 = new GeneratorItemReader();
		gen2.setMaxItemCount(100);
		gen2.withTypes(Arrays.asList(Type.HASH));
		gen2.withHashOptions(HashOptions.builder().fieldCount(IntRange.is(10)).build());
		generate(testInfo, targetClient, gen2);
		RedisItemReader<String, String, DataStructure<String>> reader = dataStructureSourceReader();
		RedisItemWriter<String, String, DataStructure<String>> writer = new WriterBuilder<>(targetClient,
				StringCodec.UTF8).mergePolicy(MergePolicy.OVERWRITE).dataStructure();
		run(testInfo, reader, writer);
		assertEquals(sourceConnection.sync().hgetall("gen:1"), targetConnection.sync().hgetall("gen:1"));
	}

	@Test
	void writeDataStructuresMerge(TestInfo testInfo) throws Exception {
		GeneratorItemReader gen1 = new GeneratorItemReader();
		gen1.setMaxItemCount(100);
		gen1.withTypes(Arrays.asList(Type.HASH));
		gen1.withHashOptions(HashOptions.builder().fieldCount(IntRange.is(5)).build());
		generate(testInfo, sourceClient, gen1);
		GeneratorItemReader gen2 = new GeneratorItemReader();
		gen2.setMaxItemCount(100);
		gen2.withTypes(Arrays.asList(Type.HASH));
		gen2.withHashOptions(HashOptions.builder().fieldCount(IntRange.is(10)).build());
		generate(testInfo, targetClient, gen2);
		RedisItemReader<String, String, DataStructure<String>> reader = dataStructureSourceReader();
		RedisItemWriter<String, String, DataStructure<String>> writer = new WriterBuilder<>(targetClient,
				StringCodec.UTF8).mergePolicy(MergePolicy.MERGE).dataStructure();
		run(testInfo, reader, writer);
		Map<String, String> actual = targetConnection.sync().hgetall("gen:1");
		assertEquals(10, actual.size());
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
		Geoadd<String, String, Geo> geoadd = new Geoadd<>(t -> "geoset", value);
		RedisItemWriter<String, String, Geo> writer = new WriterBuilder<>(sourceClient, StringCodec.UTF8)
				.operation(geoadd);
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
		Hset<String, String, Entry<String, Map<String, String>>> hset = new Hset<>(e -> "hash:" + e.getKey(),
				Entry::getValue);
		RedisItemWriter<String, String, Entry<String, Map<String, String>>> writer = new WriterBuilder<>(sourceClient,
				StringCodec.UTF8).operation(hset);
		run(testInfo, reader, writer);
		assertEquals(100, sync.keys("hash:*").size());
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
	void writeZset(TestInfo testInfo) throws Exception {
		String key = "zadd";
		List<ZValue> values = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			values.add(new ZValue(String.valueOf(index), index % 10));
		}
		ListItemReader<ZValue> reader = new ListItemReader<>(values);
		ScoredValueConverter<String, ZValue> converter = new ScoredValueConverter<>(ZValue::getMember,
				ZValue::getScore);
		Zadd<String, String, ZValue> zadd = new Zadd<>(t -> key, converter);
		RedisItemWriter<String, String, ZValue> writer = new WriterBuilder<>(sourceClient, StringCodec.UTF8)
				.operation(zadd);
		run(testInfo, reader, writer);
		RedisModulesCommands<String, String> sync = sourceConnection.sync();
		assertEquals(1, sync.dbsize());
		assertEquals(values.size(), sync.zcard(key));
		assertEquals(60,
				sync.zrangebyscore(key, Range.from(Range.Boundary.including(0), Range.Boundary.including(5))).size());
	}

	@Test
	void writeSet(TestInfo testInfo) throws Exception {
		String key = "sadd";
		List<String> values = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			values.add(String.valueOf(index));
		}
		ListItemReader<String> reader = new ListItemReader<>(values);
		Sadd<String, String, String> sadd = new Sadd<>(t -> key, Function.identity());
		RedisItemWriter<String, String, String> writer = new WriterBuilder<>(sourceClient, StringCodec.UTF8)
				.operation(sadd);
		run(testInfo, reader, writer);
		RedisModulesCommands<String, String> sync = sourceConnection.sync();
		assertEquals(1, sync.dbsize());
		assertEquals(values.size(), sync.scard(key));
	}

	@Test
	void writeSug(TestInfo testInfo) throws Exception {
		String key = "sugadd";
		List<Suggestion<String>> values = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			values.add(Suggestion.string("word" + index).score(index + 1).payload("payload" + index).build());
		}
		ListItemReader<Suggestion<String>> reader = new ListItemReader<>(values);
		SuggestionConverter<String, Suggestion<String>> converter = new SuggestionConverter<>(Suggestion::getString,
				Suggestion::getScore, Suggestion::getPayload);
		Sugadd<String, String, Suggestion<String>> sugadd = new Sugadd<>(t -> key, converter);
		RedisItemWriter<String, String, Suggestion<String>> writer = new WriterBuilder<>(sourceClient, StringCodec.UTF8)
				.operation(sugadd);
		run(testInfo, reader, writer);
		RedisModulesCommands<String, String> sync = sourceConnection.sync();
		assertEquals(1, sync.dbsize());
		assertEquals(values.size(), sync.ftSuglen(key));
	}

	@Test
	void writeSugIncr(TestInfo testInfo) throws Exception {
		String key = "sugaddIncr";
		List<Suggestion<String>> values = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			values.add(Suggestion.string("word" + index).score(index + 1).payload("payload" + index).build());
		}
		ListItemReader<Suggestion<String>> reader = new ListItemReader<>(values);
		SuggestionConverter<String, Suggestion<String>> converter = new SuggestionConverter<>(Suggestion::getString,
				Suggestion::getScore, Suggestion::getPayload);
		SugaddIncr<String, String, Suggestion<String>> sugadd = new SugaddIncr<>(t -> key, converter);
		RedisItemWriter<String, String, Suggestion<String>> writer = new WriterBuilder<>(sourceClient, StringCodec.UTF8)
				.operation(sugadd);
		run(testInfo, reader, writer);
		RedisModulesCommands<String, String> sync = sourceConnection.sync();
		assertEquals(1, sync.dbsize());
		assertEquals(values.size(), sync.ftSuglen(key));
	}

	@Test
	void writeSamples(TestInfo testInfo) throws Exception {
		String key = "ts:1";
		List<Sample> values = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			values.add(Sample.of(System.currentTimeMillis() - 1000 + index, index));
		}
		ListItemReader<Sample> reader = new ListItemReader<>(values);
		SampleConverter<Sample> converter = new SampleConverter<>(Sample::getTimestamp, Sample::getValue);
		TsAdd<String, String, Sample> tsAdd = new TsAdd<>(t -> key, converter);
		RedisItemWriter<String, String, Sample> writer = new WriterBuilder<>(sourceClient, StringCodec.UTF8)
				.operation(tsAdd);
		run(testInfo, reader, writer);
		RedisModulesCommands<String, String> sync = sourceConnection.sync();
		assertEquals(1, sync.dbsize());
		assertEquals(values.size(), sync.tsRange(key, TimeRange.unbounded()).size());
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
			ds.setType(DataStructure.HASH);
			ds.setValue(map);
			list.add(ds);
		}
		ListItemReader<DataStructure<String>> reader = new ListItemReader<>(list);
		RedisItemWriter<String, String, DataStructure<String>> writer = new WriterBuilder<>(sourceClient,
				StringCodec.UTF8).dataStructure();
		run(testInfo, reader, writer);
		RedisModulesCommands<String, String> sync = sourceConnection.sync();
		List<String> keys = sync.keys("hash:*");
		assertEquals(count, keys.size());
	}

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
		RedisItemReader<String, String, DataStructure<String>> reader = dataStructureSourceReader();
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
		RedisItemReader<String, String, DataStructure<String>> reader = liveReader(sourceClient).dataStructure();
		reader.withKeyProcessor(new FilteringItemProcessor<>(SlotRangeFilter.of(0, 8000)));
		SynchronizedListItemWriter<DataStructure<String>> writer = new SynchronizedListItemWriter<>();
		JobExecution execution = runAsync(job(testInfo, reader, writer));
		int count = 100;
		GeneratorItemReader gen = new GeneratorItemReader();
		gen.setMaxItemCount(count);
		generate(testInfo, gen);
		awaitTermination(execution);
		Assertions.assertFalse(writer.getItems().stream().map(DataStructure::getKey).map(SlotHash::getSlot)
				.anyMatch(s -> s < 0 || s > 8000));
	}

	@Test
	void readKeyspaceNotifications(TestInfo testInfo) throws Exception {
		enableKeyspaceNotifications(sourceClient);
		KeyspaceNotificationItemReader<String, String> reader = new KeyspaceNotificationItemReader<>(sourceClient,
				StringCodec.UTF8);
		reader.open(new ExecutionContext());
		GeneratorItemReader gen = new GeneratorItemReader();
		gen.setMaxItemCount(100);
		gen.withTypes(Arrays.asList(Type.HASH, Type.LIST, Type.SET, Type.STREAM, Type.STRING, Type.ZSET,
				Type.TIMESERIES, Type.JSON));
		generate(testInfo, gen);
		awaitUntil(() -> reader.getQueue().size() > 0);
		Assertions.assertEquals(KeyEventType.SET, reader.getQueue().remove().getEventType());
		Set<KeyEventType> eventTypes = new LinkedHashSet<>(
				Arrays.asList(KeyEventType.SET, KeyEventType.HSET, KeyEventType.JSON_SET, KeyEventType.RPUSH,
						KeyEventType.SADD, KeyEventType.ZADD, KeyEventType.XADD, KeyEventType.TS_ADD));
		KeyspaceNotification notification;
		while ((notification = reader.getQueue().poll()) != null) {
			Assertions.assertTrue(eventTypes.contains(notification.getEventType()));
		}
		reader.close();
	}

	@Test
	void readKeyspaceNotificationsWithType(TestInfo testInfo) throws Exception {
		enableKeyspaceNotifications(sourceClient);
		KeyspaceNotificationItemReader<String, String> reader = new KeyspaceNotificationItemReader<>(sourceClient,
				StringCodec.UTF8);
		reader.withTypes(DataStructure.HASH);
		reader.open(new ExecutionContext());
		GeneratorItemReader gen = new GeneratorItemReader();
		gen.setMaxItemCount(100);
		gen.withTypes(Arrays.asList(Type.HASH, Type.LIST, Type.SET, Type.STREAM, Type.STRING, Type.ZSET,
				Type.TIMESERIES, Type.JSON));
		generate(testInfo, gen);
		BlockingQueue<KeyspaceNotification> queue = reader.getQueue();
		awaitUntil(() -> reader.getQueue().size() > 0);
		KeyspaceNotification notification;
		while ((notification = queue.poll()) != null) {
			Assertions.assertEquals(DataStructure.HASH, notification.getEventType().getType());
		}
		reader.close();
	}

	@Test
	void scanKeyItemReader(TestInfo testInfo)
			throws UnexpectedInputException, ParseException, NonTransientResourceException, Exception {
		int count = 100;
		GeneratorItemReader gen = new GeneratorItemReader();
		gen.setMaxItemCount(count);
		generate(testInfo, gen);
		ScanKeyItemReader<String, String> reader = new ScanKeyItemReader<>(sourceClient, StringCodec.UTF8);
		reader.open(new ExecutionContext());
		Assertions.assertEquals(count, Utils.readAll(reader).size());
		reader.close();
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

	@Test
	void readThreads(TestInfo testInfo) throws Exception {
		generate(testInfo);
		RedisItemReader<String, String, DataStructure<String>> reader = dataStructureSourceReader();
		SynchronizedListItemWriter<DataStructure<String>> writer = new SynchronizedListItemWriter<>();
		int threads = 4;
		SimpleStepBuilder<DataStructure<String>, DataStructure<String>> step = step(testInfo, reader, writer);
		Utils.multiThread(step, threads);
		JobExecution execution = run(job(testInfo).start(step.build()).build());
		awaitTermination(execution);
		awaitUntilFalse(reader::isOpen);
		awaitUntilFalse(writer::isOpen);
		assertEquals(sourceConnection.sync().dbsize(),
				writer.getItems().stream().collect(Collectors.toMap(DataStructure::getKey, t -> t)).size());
	}

	@Test
	void readLive(TestInfo testInfo) throws Exception {
		enableKeyspaceNotifications(sourceClient);
		LiveRedisItemReader<byte[], byte[], KeyDump<byte[]>> reader = liveReader(sourceClient)
				.notificationQueueOptions(QueueOptions.builder().capacity(10000).build()).keyDump();
		SynchronizedListItemWriter<KeyDump<byte[]>> writer = new SynchronizedListItemWriter<>();
		JobExecution execution = runAsync(job(testInfo, reader, writer));
		GeneratorItemReader gen = new GeneratorItemReader();
		int count = 123;
		gen.setMaxItemCount(count);
		gen.withTypes(Arrays.asList(Type.HASH, Type.STRING));
		generate(testInfo, gen);
		awaitTermination(execution);
		awaitUntilFalse(reader::isOpen);
		awaitUntilFalse(writer::isOpen);
		Set<String> keys = writer.getItems().stream()
				.map(d -> StringCodec.UTF8.decodeKey(ByteArrayCodec.INSTANCE.encodeKey(d.getKey())))
				.collect(Collectors.toSet());
		Assertions.assertEquals(sourceConnection.sync().dbsize(), keys.size());
	}

	@Test
	void scanSizeEstimator(TestInfo testInfo) throws Exception {
		String pattern = GeneratorItemReader.DEFAULT_KEYSPACE + "*";
		int count = 12345;
		GeneratorItemReader gen = new GeneratorItemReader();
		gen.setMaxItemCount(count);
		gen.withKeyRange(IntRange.to(12345));
		generate(testInfo, gen);
		long expectedCount = sourceConnection.sync().dbsize();
		ScanSizeEstimator estimator = new ScanSizeEstimator(sourceClient);
		estimator.withMatch(pattern);
		assertEquals(expectedCount, estimator.getAsLong(), expectedCount / 10);
		estimator = new ScanSizeEstimator(sourceClient);
		estimator.withSampleSize(200);
		estimator.withType(DataStructure.HASH);
		assertEquals(expectedCount / GeneratorItemReader.defaultTypes().size(), estimator.getAsLong(),
				expectedCount / 10);
	}

	private void generateStreams(TestInfo testInfo) throws JobExecutionException {
		GeneratorItemReader gen = new GeneratorItemReader();
		gen.withTypes(Arrays.asList(Type.STREAM));
		gen.setMaxItemCount(3);
		gen.withStreamOptions(StreamOptions.builder().messageCount(STREAM_MESSAGE_COUNT).build());
		generate(testInfo(testInfo, "streams"), gen);
	}

	private StreamItemReader<String, String> streamReader(String stream) {
		return new StreamItemReader<>(sourceClient, StringCodec.UTF8, stream);
	}

	private void assertMessageBody(List<? extends StreamMessage<String, String>> items) {
		for (StreamMessage<String, String> message : items) {
			assertTrue(message.getBody().containsKey("field1"));
			assertTrue(message.getBody().containsKey("field2"));
		}
	}

	private void assertStreamEquals(String expectedId, Map<String, String> expectedBody, String expectedStream,
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
		Xadd<String, String, Map<String, String>> xadd = new Xadd<>(t -> stream, Function.identity(), m -> null);
		RedisItemWriter<String, String, Map<String, String>> writer = new WriterBuilder<>(sourceClient,
				StringCodec.UTF8).operation(xadd);
		run(testInfo, reader, writer);
		RedisModulesCommands<String, String> sync = sourceConnection.sync();
		Assertions.assertEquals(messages.size(), sync.xlen(stream));
		List<StreamMessage<String, String>> xrange = sync.xrange(stream, Range.create("-", "+"));
		for (int index = 0; index < xrange.size(); index++) {
			StreamMessage<String, String> message = xrange.get(index);
			Assertions.assertEquals(messages.get(index), message.getBody());
		}
	}

	@Test
	void writeStreamMultiExec(TestInfo testInfo) throws Exception {
		String stream = "stream:1";
		List<Map<String, String>> messages = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			Map<String, String> body = new HashMap<>();
			body.put("field1", "value1");
			body.put("field2", "value2");
			messages.add(body);
		}
		ListItemReader<Map<String, String>> reader = new ListItemReader<>(messages);
		Xadd<String, String, Map<String, String>> xadd = new Xadd<>(t -> stream, Function.identity(), m -> null);
		RedisItemWriter<String, String, Map<String, String>> writer = new WriterBuilder<>(sourceClient,
				StringCodec.UTF8).multiExec(true).operation(xadd);
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
		final StreamItemReader<String, String> reader = streamReader(stream).withConsumer(consumer)
				.withAckPolicy(StreamAckPolicy.AUTO);
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
		assertStreamEquals(id1, body, stream, messages.get(0));
		assertStreamEquals(id2, body, stream, messages.get(1));
		assertStreamEquals(id3, body, stream, messages.get(2));
		reader.close();
		Assertions.assertEquals(0, sourceConnection.sync().xpending(stream, consumerGroup).getCount(),
				"pending messages");
	}

	@Test
	void readStreamManualAck() throws Exception {
		String stream = "stream1";
		String consumerGroup = "batchtests-readStreamManualAck";
		Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
		final StreamItemReader<String, String> reader = streamReader(stream).withConsumer(consumer)
				.withAckPolicy(StreamAckPolicy.MANUAL);
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

		assertStreamEquals(id1, body, stream, messages.get(0));
		assertStreamEquals(id2, body, stream, messages.get(1));
		assertStreamEquals(id3, body, stream, messages.get(2));
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
		final StreamItemReader<String, String> reader = streamReader(stream).withConsumer(consumer)
				.withAckPolicy(StreamAckPolicy.MANUAL);
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

		final StreamItemReader<String, String> reader2 = streamReader(stream).withConsumer(consumer)
				.withAckPolicy(StreamAckPolicy.MANUAL);
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
		final StreamItemReader<String, String> reader = streamReader(stream).withConsumer(consumer)
				.withAckPolicy(StreamAckPolicy.MANUAL);
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

		final StreamItemReader<String, String> reader2 = streamReader(stream).withConsumer(consumer)
				.withAckPolicy(StreamAckPolicy.MANUAL).withOffset(messages.get(1).getId());
		reader2.open(new ExecutionContext());

		// Wait until task.poll() doesn't return any more records
		awaitUntil(() -> recoveredMessages.addAll(reader2.readMessages()));
		awaitUntil(() -> !recoveredMessages.addAll(reader2.readMessages()));
		List<String> recoveredIds = recoveredMessages.stream().map(StreamMessage::getId).collect(Collectors.toList());
		Assertions.assertEquals(Arrays.<String>asList(id3, id4, id5, id6), recoveredIds, "recoveredIds");
		reader2.close();
	}

	@Test
	void readStreamManualAckRecoverFromOffset() throws Exception {
		String stream = "stream1";
		String consumerGroup = "batchtests-readStreamManualAckRecoverFromOffset";
		Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
		final StreamItemReader<String, String> reader = streamReader(stream).withConsumer(consumer)
				.withAckPolicy(StreamAckPolicy.MANUAL);
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

		final StreamItemReader<String, String> reader2 = streamReader(stream).withConsumer(consumer)
				.withAckPolicy(StreamAckPolicy.MANUAL).withOffset(id3);
		reader2.open(new ExecutionContext());

		// Wait until task.poll() doesn't return any more records
		awaitUntil(() -> recoveredRecords.addAll(reader2.readMessages()));
		awaitUntil(() -> !recoveredRecords.addAll(reader2.readMessages()));
		List<String> recoveredIds = recoveredRecords.stream().map(StreamMessage::getId).collect(Collectors.toList());
		Assertions.assertEquals(Arrays.<String>asList(id4, id5, id6), recoveredIds, "recoveredIds");
		reader2.close();
	}

	@Test
	void readStreamRecoverManualAckToAutoAck() throws InterruptedException {
		String stream = "stream1";
		String consumerGroup = "readStreamRecoverManualAckToAutoAck";
		Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
		final StreamItemReader<String, String> reader = streamReader(stream).withConsumer(consumer)
				.withAckPolicy(StreamAckPolicy.MANUAL);
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

		final StreamItemReader<String, String> reader2 = streamReader(stream).withConsumer(consumer)
				.withAckPolicy(StreamAckPolicy.AUTO);
		reader2.open(new ExecutionContext());

		// Wait until task.poll() doesn't return any more records
		awaitUntil(() -> recoveredRecords.addAll(reader2.readMessages()));
		awaitUntil(() -> !recoveredRecords.addAll(reader2.readMessages()));
		List<String> recoveredIds = recoveredRecords.stream().map(StreamMessage::getId).collect(Collectors.toList());
		Assertions.assertEquals(Arrays.asList(id4, id5, id6), recoveredIds, "recoveredIds");

		PendingMessages pending = sourceConnection.sync().xpending(stream, consumerGroup);
		Assertions.assertEquals(0, pending.getCount(), "pending message count");
		reader2.close();
	}

	@Test
	void readMessages(TestInfo testInfo) throws Exception {
		generateStreams(testInfo);
		List<String> keys = ScanIterator.scan(sourceConnection.sync(), KeyScanArgs.Builder.type(DataStructure.STREAM))
				.stream().collect(Collectors.toList());
		for (String key : keys) {
			long count = sourceConnection.sync().xlen(key);
			StreamItemReader<String, String> reader = streamReader(key)
					.withConsumer(Consumer.from("batchtests-readmessages", "consumer1"));
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
		List<String> keys = ScanIterator.scan(sourceConnection.sync(), KeyScanArgs.Builder.type(DataStructure.STREAM))
				.stream().collect(Collectors.toList());
		for (String key : keys) {
			Consumer<String> consumer = Consumer.from("batchtests-readstreamjob", "consumer1");
			StreamItemReader<String, String> reader = streamReader(key).withConsumer(consumer);
			SynchronizedListItemWriter<StreamMessage<String, String>> writer = new SynchronizedListItemWriter<>();
			run(testInfo(testInfo, key), reader, writer);
			Assertions.assertEquals(STREAM_MESSAGE_COUNT, writer.getItems().size());
			assertMessageBody(writer.getItems());
		}
	}

	@Test
	void readMultipleStreams(TestInfo testInfo) throws Exception {
		generateStreams(testInfo(testInfo, "streams"));
		final List<String> keys = ScanIterator
				.scan(sourceConnection.sync(), KeyScanArgs.Builder.type(DataStructure.STREAM)).stream()
				.collect(Collectors.toList());
		for (String key : keys) {
			StreamItemReader<String, String> reader1 = streamReader(key)
					.withConsumer(Consumer.from(DEFAULT_CONSUMER_GROUP, "consumer1"))
					.withAckPolicy(StreamAckPolicy.MANUAL);
			StreamItemReader<String, String> reader2 = streamReader(key)
					.withConsumer(Consumer.from(DEFAULT_CONSUMER_GROUP, "consumer2"))
					.withAckPolicy(StreamAckPolicy.MANUAL);
			SynchronizedListItemWriter<StreamMessage<String, String>> writer1 = new SynchronizedListItemWriter<>();
			JobExecution execution1 = runAsync(job(testInfo(testInfo, key, "1"), reader1, writer1));
			SynchronizedListItemWriter<StreamMessage<String, String>> writer2 = new SynchronizedListItemWriter<>();
			JobExecution execution2 = runAsync(job(testInfo(testInfo, key, "2"), reader2, writer2));
			awaitTermination(execution1);
			awaitUntilFalse(reader1::isOpen);
			awaitUntilFalse(writer1::isOpen);
			awaitTermination(execution2);
			awaitUntilFalse(reader2::isOpen);
			awaitUntilFalse(writer2::isOpen);
			Assertions.assertEquals(STREAM_MESSAGE_COUNT, writer1.getItems().size() + writer2.getItems().size());
			assertMessageBody(writer1.getItems());
			assertMessageBody(writer2.getItems());
			RedisModulesCommands<String, String> sync = sourceConnection.sync();
			Assertions.assertEquals(STREAM_MESSAGE_COUNT, sync.xpending(key, DEFAULT_CONSUMER_GROUP).getCount());
			reader1 = streamReader(key).withConsumer(Consumer.from(DEFAULT_CONSUMER_GROUP, "consumer1"))
					.withAckPolicy(StreamAckPolicy.MANUAL);
			reader1.open(new ExecutionContext());
			reader1.ack(writer1.getItems());
			reader1.close();
			reader2 = streamReader(key).withConsumer(Consumer.from(DEFAULT_CONSUMER_GROUP, "consumer2"))
					.withAckPolicy(StreamAckPolicy.MANUAL);
			reader2.open(new ExecutionContext());
			reader2.ack(writer2.getItems());
			reader2.close();
			Assertions.assertEquals(0, sync.xpending(key, DEFAULT_CONSUMER_GROUP).getCount());
		}
	}

	@Test
	void writeJSON(TestInfo testInfo) throws Exception {
		JsonSet<String, String, JsonNode> jsonSet = new JsonSet<>(n -> "beer:" + n.get("id").asText(),
				JsonNode::toString, t -> ".");
		RedisItemWriter<String, String, JsonNode> writer = new WriterBuilder<>(sourceClient, StringCodec.UTF8)
				.operation(jsonSet);
		IteratorItemReader<JsonNode> reader = new IteratorItemReader<>(Beers.jsonNodeIterator());
		run(testInfo, reader, writer);
		Assertions.assertEquals(BEER_COUNT, sourceConnection.sync().keys("beer:*").size());
		Assertions.assertEquals(new ObjectMapper().readTree(JSON_BEER_1),
				new ObjectMapper().readTree(sourceConnection.sync().jsonGet("beer:1")));
	}

	@Test
	void writeTS(TestInfo testInfo) throws Exception {
		String key = "ts:1";
		Random random = new Random();
		int count = 100;
		List<Sample> samples = new ArrayList<>(count);
		for (int index = 0; index < count; index++) {
			long timestamp = System.currentTimeMillis() - count + (index % (count / 2));
			samples.add(Sample.of(timestamp, random.nextDouble()));
		}
		ListItemReader<Sample> reader = new ListItemReader<>(samples);
		AddOptions<String, String> addOptions = AddOptions.<String, String>builder().policy(DuplicatePolicy.LAST)
				.build();
		TsAdd<String, String, Sample> tsadd = new TsAdd<>(t -> key, Function.identity(), t -> addOptions);
		RedisItemWriter<String, String, Sample> writer = new WriterBuilder<>(sourceClient, StringCodec.UTF8)
				.operation(tsadd);
		run(testInfo, reader, writer);
		Assertions.assertEquals(count / 2,
				sourceConnection.sync().tsRange(key, TimeRange.unbounded(), RangeOptions.builder().build()).size(), 2);
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
		RedisItemReader<String, String, KeyComparison> reader = comparisonReader();
		KeyComparisonCountItemWriter writer = new KeyComparisonCountItemWriter();
		run(testInfo, reader, writer);
		Assertions.assertEquals(1, writer.getResults().getCount(Status.MISSING));
	}

	@Test
	void replicateJSON(TestInfo testInfo) throws Exception {
		sourceConnection.sync().jsonSet("json:1", "$", JSON_BEER_1);
		sourceConnection.sync().jsonSet("json:2", "$", JSON_BEER_1);
		sourceConnection.sync().jsonSet("json:3", "$", JSON_BEER_1);
		run(testInfo, dataStructureSourceReader(), dataStructureTargetWriter());
		compare(testInfo);
	}

	private RedisItemWriter<String, String, DataStructure<String>> dataStructureTargetWriter() {
		return new WriterBuilder<>(targetClient, StringCodec.UTF8).dataStructure();
	}

	@Test
	void replicateTimeSeries(TestInfo testInfo) throws Exception {
		String key = "ts:1";
		sourceConnection.sync().tsCreate(key,
				CreateOptions.<String, String>builder().policy(DuplicatePolicy.LAST).build());
		sourceConnection.sync().tsAdd(key, Sample.of(1000, 1));
		sourceConnection.sync().tsAdd(key, Sample.of(1001, 2));
		sourceConnection.sync().tsAdd(key, Sample.of(1003, 3));
		run(testInfo, dataStructureSourceReader(), dataStructureTargetWriter());
		compare(testInfo);
	}

	@Test
	void dataStructures(TestInfo testInfo) throws Exception {
		GeneratorItemReader gen = new GeneratorItemReader();
		gen.setMaxItemCount(100);
		generate(testInfo, gen);
		RedisItemReader<String, String, DataStructure<String>> reader = dataStructureSourceReader();
		run(testInfo, reader, dataStructureTargetWriter());
		compare(testInfo);
	}

	@Test
	void dumpAndRestore(TestInfo testInfo) throws Exception {
		GeneratorItemReader gen = new GeneratorItemReader();
		gen.setMaxItemCount(100);
		generate(testInfo, gen);
		RedisItemReader<byte[], byte[], KeyDump<byte[]>> reader = keyDumpSourceReader();
		run(testInfo, reader, keyDumpWriter(targetClient));
		compare(testInfo);
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
		run(testInfo, keyDumpSourceReader(), keyDumpWriter(targetClient));
		Awaitility.await().until(() -> sourceConnection.sync().dbsize() == targetConnection.sync().dbsize());
	}

	@Test
	void liveOnlyReplication(TestInfo testInfo) throws Exception {
		enableKeyspaceNotifications(sourceClient);
		RedisItemReader<byte[], byte[], KeyDump<byte[]>> reader = liveReader(sourceClient)
				.notificationQueueOptions(QueueOptions.builder().capacity(100000).build())
				.idleTimeout(DEFAULT_IDLE_TIMEOUT).keyDump();
		RedisItemWriter<byte[], byte[], KeyDump<byte[]>> writer = keyDumpWriter(targetClient);
		JobExecution execution = runAsync(job(testInfo, reader, writer));
		GeneratorItemReader gen = new GeneratorItemReader();
		gen.setMaxItemCount(100);
		gen.withTypes(Arrays.asList(Type.HASH, Type.LIST, Type.SET, Type.STRING, Type.ZSET));
		generate(testInfo, gen);
		awaitTermination(execution);
		compare(testInfo);
	}

	@Test
	void liveDumpAndRestoreReplication(TestInfo testInfo) throws Exception {
		enableKeyspaceNotifications(sourceClient);
		RedisItemReader<byte[], byte[], KeyDump<byte[]>> reader = keyDumpSourceReader();
		RedisItemWriter<byte[], byte[], KeyDump<byte[]>> writer = keyDumpWriter(targetClient);
		LiveRedisItemReader<byte[], byte[], KeyDump<byte[]>> liveReader = liveReader(sourceClient).keyDump();
		RedisItemWriter<byte[], byte[], KeyDump<byte[]>> liveWriter = keyDumpWriter(targetClient);
		liveReplication(testInfo, reader, writer, liveReader, liveWriter);
	}

	@Test
	void liveSetReplication(TestInfo testInfo) throws Exception {
		enableKeyspaceNotifications(sourceClient);
		String key = "myset";
		sourceConnection.sync().sadd(key, "1", "2", "3", "4", "5");
		RedisItemReader<String, String, DataStructure<String>> reader = liveReader(sourceClient)
				.notificationQueueOptions(QueueOptions.builder().capacity(100).build()).dataStructure();
		RedisItemWriter<String, String, DataStructure<String>> writer = dataStructureTargetWriter();
		JobExecution execution = runAsync(job(testInfo, reader, writer));
		sourceConnection.sync().srem(key, "5");
		awaitTermination(execution);
		assertEquals(sourceConnection.sync().smembers(key), targetConnection.sync().smembers(key));
	}

	@Test
	void invalidConnection(TestInfo testInfo) throws Exception {
		try (RedisModulesClient badSourceClient = RedisModulesClient.create("redis://badhost:6379");
				RedisModulesClient badTargetClient = RedisModulesClient.create("redis://badhost:6379")) {
			RedisItemReader<String, String, DataStructure<String>> reader = dataStructureReader(badSourceClient,
					StringCodec.UTF8);
			RedisItemWriter<String, String, DataStructure<String>> writer = new WriterBuilder<>(badTargetClient,
					StringCodec.UTF8).dataStructure();
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
		RedisItemReader<byte[], byte[], DataStructure<byte[]>> reader = dataStructureReader(sourceClient,
				ByteArrayCodec.INSTANCE);
		RedisItemWriter<byte[], byte[], DataStructure<byte[]>> writer = new WriterBuilder<>(targetClient,
				ByteArrayCodec.INSTANCE).dataStructure();
		run(testInfo, reader, writer);
		RedisModulesCommands<String, String> sourceSync = sourceConnection.sync();
		RedisModulesCommands<String, String> targetSync = targetConnection.sync();
		assertEquals(sourceSync.pfcount(key1), targetSync.pfcount(key1));
	}

	@Test
	void liveTypeBasedReplication(TestInfo testInfo) throws Exception {
		enableKeyspaceNotifications(sourceClient);
		RedisItemReader<String, String, DataStructure<String>> reader = dataStructureSourceReader();
		RedisItemWriter<String, String, DataStructure<String>> writer = dataStructureTargetWriter();
		LiveRedisItemReader<String, String, DataStructure<String>> liveReader = liveReader(sourceClient)
				.dataStructure();
		RedisItemWriter<String, String, DataStructure<String>> liveWriter = dataStructureTargetWriter();
		liveReplication(testInfo, reader, writer, liveReader, liveWriter);
	}

	private <K, V, T extends KeyValue<K>> void liveReplication(TestInfo testInfo, RedisItemReader<K, V, T> reader,
			RedisItemWriter<K, V, T> writer, LiveRedisItemReader<K, V, T> liveReader,
			RedisItemWriter<K, V, T> liveWriter) throws Exception {
		GeneratorItemReader gen = new GeneratorItemReader();
		gen.setMaxItemCount(300);
		gen.withTypes(Arrays.asList(Type.HASH, Type.LIST, Type.SET, Type.STREAM, Type.STRING, Type.ZSET));
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
		liveGen.withTypes(Arrays.asList(Type.HASH, Type.LIST, Type.SET, Type.STRING, Type.ZSET));
		liveGen.withExpiration(IntRange.is(100));
		liveGen.withKeyRange(IntRange.between(300, 1000));
		generate(testInfo(testInfo, "generateLive"), liveGen);
		try {
			awaitTermination(execution);
		} catch (ConditionTimeoutException e) {
			// ignore
		}
		awaitUntilFalse(reader::isOpen);
		awaitUntilFalse(writer::isOpen);
		awaitUntilFalse(liveReader::isOpen);
		awaitUntilFalse(liveWriter::isOpen);
		compare(testInfo);
	}

	@Test
	void comparator(TestInfo testInfo) throws Exception {
		GeneratorItemReader gen = new GeneratorItemReader();
		gen.setMaxItemCount(120);
		generate(testInfo, gen);
		run(testInfo(testInfo, "replicate"), keyDumpSourceReader(), keyDumpWriter(targetClient));
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
			if (type.equalsIgnoreCase(DataStructure.STRING)) {
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
		RedisItemReader<String, String, KeyComparison> reader = comparisonReader();
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

	@Test
	void testLuaHash() throws InterruptedException, ExecutionException {
		String key = "myhash";
		Map<String, String> hash = new HashMap<>();
		hash.put("field1", "value1");
		hash.put("field2", "value2");
		sourceConnection.sync().hset(key, hash);
		long ttl = System.currentTimeMillis() + 123456;
		sourceConnection.sync().pexpireat(key, ttl);
		StringDataStructureReadOperation operation = new StringDataStructureReadOperation(sourceClient);
		operation.open(new ExecutionContext());
		Future<DataStructure<String>> future = operation.execute(sourceConnection.async(), key);
		DataStructure<String> ds = future.get();
		Assertions.assertEquals(key, ds.getKey());
		Assertions.assertEquals(ttl, ds.getTtl());
		Assertions.assertEquals(DataStructure.HASH, ds.getType());
		Assertions.assertEquals(hash, ds.getValue());
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void testLuaZset() throws InterruptedException, ExecutionException {
		String key = "myzset";
		ScoredValue[] values = { ScoredValue.just(123.456, "value1"), ScoredValue.just(654.321, "value2") };
		sourceConnection.sync().zadd(key, values);
		StringDataStructureReadOperation operation = new StringDataStructureReadOperation(sourceClient);
		operation.open(new ExecutionContext());
		Future<DataStructure<String>> future = operation.execute(sourceConnection.async(), key);
		DataStructure<String> ds = future.get();
		Assertions.assertEquals(key, ds.getKey());
		Assertions.assertEquals(DataStructure.ZSET, ds.getType());
		Assertions.assertEquals(Arrays.asList(values), ds.getValue());
	}

	@Test
	void testLuaList() throws InterruptedException, ExecutionException {
		String key = "mylist";
		List<String> values = Arrays.asList("value1", "value2");
		sourceConnection.sync().rpush(key, values.toArray(new String[0]));
		StringDataStructureReadOperation operation = new StringDataStructureReadOperation(sourceClient);
		operation.open(new ExecutionContext());
		Future<DataStructure<String>> future = operation.execute(sourceConnection.async(), key);
		DataStructure<String> ds = future.get();
		Assertions.assertEquals(key, ds.getKey());
		Assertions.assertEquals(DataStructure.LIST, ds.getType());
		Assertions.assertEquals(values, ds.getValue());
	}

	@Test
	void testLuaStream() throws InterruptedException, ExecutionException {
		String key = "mystream";
		Map<String, String> body = new HashMap<>();
		body.put("field1", "value1");
		body.put("field2", "value2");
		sourceConnection.sync().xadd(key, body);
		sourceConnection.sync().xadd(key, body);
		StringDataStructureReadOperation operation = new StringDataStructureReadOperation(sourceClient);
		operation.open(new ExecutionContext());
		Future<DataStructure<String>> future = operation.execute(sourceConnection.async(), key);
		DataStructure<String> ds = future.get();
		Assertions.assertEquals(key, ds.getKey());
		Assertions.assertEquals(DataStructure.STREAM, ds.getType());
		List<StreamMessage<String, String>> messages = ds.getValue();
		Assertions.assertEquals(2, messages.size());
		for (StreamMessage<String, String> message : messages) {
			Assertions.assertEquals(body, message.getBody());
			Assertions.assertNotNull(message.getId());
		}
	}

	@Test
	void testLuaStreamKeyDump() throws InterruptedException, ExecutionException {
		String key = "mystream";
		Map<String, String> body = new HashMap<>();
		body.put("field1", "value1");
		body.put("field2", "value2");
		sourceConnection.sync().xadd(key, body);
		sourceConnection.sync().xadd(key, body);
		long ttl = System.currentTimeMillis() + 123456;
		sourceConnection.sync().pexpireat(key, ttl);
		StatefulRedisModulesConnection<byte[], byte[]> byteConnection = RedisModulesUtils.connection(sourceClient,
				ByteArrayCodec.INSTANCE);
		KeyDumpReadOperation operation = new KeyDumpReadOperation(sourceClient);
		operation.open(new ExecutionContext());
		Future<KeyDump<byte[]>> future = operation.execute(byteConnection.async(), keyBytes(key));
		KeyDump<byte[]> keyDump = future.get();
		Assertions.assertArrayEquals(keyBytes(key), keyDump.getKey());
		Assertions.assertEquals(ttl, keyDump.getTtl());
		sourceConnection.sync().del(key);
		sourceConnection.sync().restore(key, keyDump.getDump(), RestoreArgs.Builder.ttl(ttl).absttl());
		Assertions.assertEquals(DataStructure.STREAM, sourceConnection.sync().type(key));
	}

	@Test
	void testLuaStreamByteArray() throws InterruptedException, ExecutionException {
		String key = "mystream";
		Map<String, String> body = new HashMap<>();
		body.put("field1", "value1");
		body.put("field2", "value2");
		sourceConnection.sync().xadd(key, body);
		sourceConnection.sync().xadd(key, body);
		DataStructureReadOperation<byte[], byte[]> operation = new DataStructureReadOperation<>(sourceClient,
				ByteArrayCodec.INSTANCE);
		operation.open(new ExecutionContext());
		StatefulRedisModulesConnection<byte[], byte[]> byteConnection = RedisModulesUtils.connection(sourceClient,
				ByteArrayCodec.INSTANCE);
		Future<DataStructure<byte[]>> future = operation.execute(byteConnection.async(), keyBytes(key));
		DataStructure<byte[]> ds = future.get();
		Assertions.assertArrayEquals(keyBytes(key), ds.getKey());
		Assertions.assertEquals(DataStructure.STREAM, ds.getType());
		List<StreamMessage<byte[], byte[]>> messages = ds.getValue();
		Assertions.assertEquals(2, messages.size());
		for (StreamMessage<byte[], byte[]> message : messages) {
			Map<byte[], byte[]> actual = message.getBody();
			Assertions.assertEquals(2, actual.size());
			Map<String, String> actualString = new HashMap<>();
			actual.forEach((k, v) -> actualString.put(keyString(k), valueString(v)));
			Assertions.assertEquals(body, actualString);
		}
	}

	@Test
	void testLuaTimeSeries() throws InterruptedException, ExecutionException {
		String key = "myts";
		Sample[] samples = { Sample.of(System.currentTimeMillis(), 1.1),
				Sample.of(System.currentTimeMillis() + 10, 2.2) };
		for (Sample sample : samples) {
			sourceConnection.sync().tsAdd(key, sample);
		}
		StringDataStructureReadOperation operation = new StringDataStructureReadOperation(sourceClient);
		operation.open(new ExecutionContext());
		Future<DataStructure<String>> future = operation.execute(sourceConnection.async(), key);
		DataStructure<String> ds = future.get();
		Assertions.assertEquals(key, ds.getKey());
		Assertions.assertEquals(DataStructure.TIMESERIES, ds.getType());
		Assertions.assertEquals(Arrays.asList(samples), ds.getValue());
	}

	@Test
	void testLuaTimeSeriesByteArray() throws InterruptedException, ExecutionException {
		String key = "myts";
		Sample[] samples = { Sample.of(System.currentTimeMillis(), 1.1),
				Sample.of(System.currentTimeMillis() + 10, 2.2) };
		for (Sample sample : samples) {
			sourceConnection.sync().tsAdd(key, sample);
		}
		DataStructureReadOperation<byte[], byte[]> operation = new DataStructureReadOperation<>(sourceClient,
				ByteArrayCodec.INSTANCE);
		operation.open(new ExecutionContext());
		StatefulRedisModulesConnection<byte[], byte[]> byteConnection = RedisModulesUtils.connection(sourceClient,
				ByteArrayCodec.INSTANCE);
		Future<DataStructure<byte[]>> future = operation.execute(byteConnection.async(), keyBytes(key));
		DataStructure<byte[]> ds = future.get();
		Assertions.assertArrayEquals(keyBytes(key), ds.getKey());
		Assertions.assertEquals(DataStructure.TIMESERIES, ds.getType());
		Assertions.assertEquals(Arrays.asList(samples), ds.getValue());
	}

	@Test
	void testLuaHLL() throws InterruptedException, ExecutionException {
		String key1 = "hll:1";
		sourceConnection.sync().pfadd(key1, "member:1", "member:2");
		String key2 = "hll:2";
		sourceConnection.sync().pfadd(key2, "member:1", "member:2", "member:3");
		StringDataStructureReadOperation operation = new StringDataStructureReadOperation(sourceClient);
		operation.open(new ExecutionContext());
		Future<DataStructure<String>> future = operation.execute(sourceConnection.async(), key1);
		DataStructure<String> ds1 = future.get();
		Assertions.assertEquals(key1, ds1.getKey());
		Assertions.assertEquals(DataStructure.STRING, ds1.getType());
		Assertions.assertEquals(sourceConnection.sync().get(key1), ds1.getValue());
	}

	private String keyString(byte[] bytes) {
		return StringCodec.UTF8.decodeKey(ByteArrayCodec.INSTANCE.encodeKey(bytes));
	}

	private String valueString(byte[] bytes) {
		return StringCodec.UTF8.decodeValue(ByteArrayCodec.INSTANCE.encodeValue(bytes));
	}

	private byte[] keyBytes(String string) {
		return ByteArrayCodec.INSTANCE.decodeKey(StringCodec.UTF8.encodeKey(string));
	}

}
