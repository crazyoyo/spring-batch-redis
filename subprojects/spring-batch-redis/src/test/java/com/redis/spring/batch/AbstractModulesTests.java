package com.redis.spring.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
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
import org.springframework.batch.item.support.IteratorItemReader;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.Beers;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.search.IndexInfo;
import com.redis.lettucemod.search.Suggestion;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.DuplicatePolicy;
import com.redis.lettucemod.timeseries.RangeOptions;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.lettucemod.timeseries.TimeRange;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.RedisItemWriter.WriterBuilder;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.IntRange;
import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.convert.SampleConverter;
import com.redis.spring.batch.convert.SuggestionConverter;
import com.redis.spring.batch.reader.DataStructureReadOperation;
import com.redis.spring.batch.reader.GeneratorItemReader;
import com.redis.spring.batch.reader.GeneratorItemReader.HashOptions;
import com.redis.spring.batch.reader.GeneratorItemReader.Type;
import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.spring.batch.reader.KeyComparisonItemReader;
import com.redis.spring.batch.reader.KeyEventType;
import com.redis.spring.batch.reader.KeyspaceNotification;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.QueueOptions;
import com.redis.spring.batch.reader.StringDataStructureReadOperation;
import com.redis.spring.batch.writer.KeyComparisonCountItemWriter;
import com.redis.spring.batch.writer.KeyComparisonCountItemWriter.Results;
import com.redis.spring.batch.writer.MergePolicy;
import com.redis.spring.batch.writer.operation.JsonSet;
import com.redis.spring.batch.writer.operation.Sugadd;
import com.redis.spring.batch.writer.operation.SugaddIncr;
import com.redis.spring.batch.writer.operation.TsAdd;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;

abstract class AbstractModulesTests extends AbstractTests {

	private static final String JSON_BEER_1 = "{\"id\":\"1\",\"brewery_id\":\"812\",\"name\":\"Hocus Pocus\",\"abv\":\"4.5\",\"ibu\":\"0\",\"srm\":\"0\",\"upc\":\"0\",\"filepath\":\"\",\"descript\":\"Our take on a classic summer ale.  A toast to weeds, rays, and summer haze.  A light, crisp ale for mowing lawns, hitting lazy fly balls, and communing with nature, Hocus Pocus is offered up as a summer sacrifice to clodless days.\\n\\nIts malty sweetness finishes tart and crisp and is best apprediated with a wedge of orange.\",\"add_user\":\"0\",\"last_mod\":\"2010-07-22 20:00:20 UTC\",\"style_name\":\"Light American Wheat Ale or Lager\",\"cat_name\":\"Other Style\"}";
	private static final int BEER_COUNT = 1019;

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

	@Test
	void readKeyspaceNotifications(TestInfo testInfo) throws Exception {
		enableKeyspaceNotifications(sourceClient);
		KeyspaceNotificationItemReader<String, String> reader = new KeyspaceNotificationItemReader<>(sourceClient,
				StringCodec.UTF8);
		reader.open(new ExecutionContext());
		GeneratorItemReader gen = new GeneratorItemReader();
		gen.setMaxItemCount(100);
		gen.setTypes(Arrays.asList(Type.HASH, Type.LIST, Type.SET, Type.STREAM, Type.STRING, Type.ZSET, Type.TIMESERIES,
				Type.JSON));
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
		reader.getOptions().setType(KeyValue.HASH);
		reader.open(new ExecutionContext());
		GeneratorItemReader gen = new GeneratorItemReader();
		gen.setMaxItemCount(100);
		gen.setTypes(Arrays.asList(Type.HASH, Type.LIST, Type.SET, Type.STREAM, Type.STRING, Type.ZSET, Type.TIMESERIES,
				Type.JSON));
		generate(testInfo, gen);
		BlockingQueue<KeyspaceNotification> queue = reader.getQueue();
		awaitUntil(() -> reader.getQueue().size() > 0);
		KeyspaceNotification notification;
		while ((notification = queue.poll()) != null) {
			Assertions.assertEquals(KeyValue.HASH, notification.getEventType().getType());
		}
		reader.close();
	}

	@Test
	void writeJSON(TestInfo testInfo) throws Exception {
		JsonSet<String, String, JsonNode> jsonSet = new JsonSet<>(n -> "beer:" + n.get("id").asText(),
				JsonNode::toString, t -> ".");
		RedisItemWriter<String, String, JsonNode> writer = new WriterBuilder(sourceClient).operation(jsonSet);
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
		RedisItemWriter<String, String, Sample> writer = new WriterBuilder(sourceClient).operation(tsadd);
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
		KeyComparisonItemReader reader = comparisonReader();
		KeyComparisonCountItemWriter writer = new KeyComparisonCountItemWriter();
		run(testInfo, reader, writer);
		Assertions.assertEquals(1, writer.getResults().getCount(Status.MISSING));
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
		Future<DataStructure<String>> future = operation.execute(sourceConnection.async(), key);
		DataStructure<String> ds = future.get();
		Assertions.assertEquals(key, ds.getKey());
		Assertions.assertEquals(KeyValue.TIMESERIES, ds.getType());
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
		StatefulRedisModulesConnection<byte[], byte[]> byteConnection = RedisModulesUtils.connection(sourceClient,
				ByteArrayCodec.INSTANCE);
		Future<DataStructure<byte[]>> future = operation.execute(byteConnection.async(), keyBytes(key));
		DataStructure<byte[]> ds = future.get();
		Assertions.assertArrayEquals(keyBytes(key), ds.getKey());
		Assertions.assertEquals(KeyValue.TIMESERIES, ds.getType());
		Assertions.assertEquals(Arrays.asList(samples), ds.getValue());
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
		RedisItemWriter<String, String, Sample> writer = new WriterBuilder(sourceClient).operation(tsAdd);
		run(testInfo, reader, writer);
		RedisModulesCommands<String, String> sync = sourceConnection.sync();
		assertEquals(1, sync.dbsize());
		assertEquals(values.size(), sync.tsRange(key, TimeRange.unbounded()).size());
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
		RedisItemWriter<String, String, Suggestion<String>> writer = new WriterBuilder(sourceClient).operation(sugadd);
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
		RedisItemWriter<String, String, Suggestion<String>> writer = new WriterBuilder(sourceClient).operation(sugadd);
		run(testInfo, reader, writer);
		RedisModulesCommands<String, String> sync = sourceConnection.sync();
		assertEquals(1, sync.dbsize());
		assertEquals(values.size(), sync.ftSuglen(key));
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
		TestInfo finalTestInfo = testInfo(testInfo, "compare");
		KeyComparisonItemReader reader = comparisonReader();
		SynchronizedListItemWriter<KeyComparison> writer = new SynchronizedListItemWriter<>();
		run(job(finalTestInfo).start(step(finalTestInfo, reader, writer).build()).build());
		awaitClosed(reader);
		awaitClosed(writer);
		Assertions.assertFalse(writer.getItems().isEmpty());
		return writer.getItems();
	}

	protected boolean isOk(List<? extends KeyComparison> comparisons) {
		return comparisons.stream().allMatch(c -> c.getStatus() == Status.OK);
	}

	protected KeyComparisonItemReader comparisonReader() throws Exception {
		return new KeyComparisonItemReader.Builder(sourceClient, targetClient).jobRepository(jobRepository)
				.ttlTolerance(Duration.ofMillis(100)).build();
	}

	@Test
	void writeDataStructuresOverwrite(TestInfo testInfo) throws Exception {
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
		RedisItemReader<String, String, DataStructure<String>> reader = dataStructureSourceReader();
		RedisItemWriter<String, String, DataStructure<String>> writer = new WriterBuilder(targetClient)
				.mergePolicy(MergePolicy.OVERWRITE).dataStructure();
		run(testInfo, reader, writer);
		assertEquals(sourceConnection.sync().hgetall("gen:1"), targetConnection.sync().hgetall("gen:1"));
	}

	@Test
	void writeDataStructuresMerge(TestInfo testInfo) throws Exception {
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
		RedisItemReader<String, String, DataStructure<String>> reader = dataStructureSourceReader();
		RedisItemWriter<String, String, DataStructure<String>> writer = new WriterBuilder(targetClient)
				.mergePolicy(MergePolicy.MERGE).dataStructure();
		run(testInfo, reader, writer);
		Map<String, String> actual = targetConnection.sync().hgetall("gen:1");
		assertEquals(10, actual.size());
	}

	@Test
	void setComparator(TestInfo testInfo) throws Exception {
		sourceConnection.sync().sadd("set:1", "value1", "value2");
		targetConnection.sync().sadd("set:1", "value2", "value1");
		KeyComparisonItemReader reader = comparisonReader();
		SynchronizedListItemWriter<KeyComparison> writer = new SynchronizedListItemWriter<>();
		run(testInfo, reader, writer);
		Assertions.assertEquals(KeyComparison.Status.OK, writer.getItems().get(0).getStatus());
	}

	protected RedisItemWriter<String, String, DataStructure<String>> dataStructureTargetWriter() {
		return new WriterBuilder(targetClient).dataStructure();
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
		RedisItemReader<byte[], byte[], KeyDump<byte[]>> reader = keyDumpSourceReader();
		RedisItemWriter<byte[], byte[], KeyDump<byte[]>> writer = keyDumpWriter(targetClient);
		run(testInfo, reader, writer);
		Assertions.assertEquals(sourceConnection.sync().dbsize(), targetConnection.sync().dbsize());
	}

	@Test
	void liveOnlyReplication(TestInfo testInfo) throws Exception {
		enableKeyspaceNotifications(sourceClient);
		LiveRedisItemReader<byte[], byte[], KeyDump<byte[]>> reader = liveReader(sourceClient).keyDump();
		reader.getKeyspaceNotificationOptions().setQueueOptions(QueueOptions.builder().capacity(100000).build());
		reader.getFlushingOptions().setIdleTimeout(DEFAULT_IDLE_TIMEOUT);
		RedisItemWriter<byte[], byte[], KeyDump<byte[]>> writer = keyDumpWriter(targetClient);
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
		LiveRedisItemReader<String, String, DataStructure<String>> reader = liveReader(sourceClient).dataStructure();
		reader.getKeyspaceNotificationOptions().setQueueOptions(QueueOptions.builder().capacity(100).build());
		RedisItemWriter<String, String, DataStructure<String>> writer = dataStructureTargetWriter();
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
		RedisItemReader<byte[], byte[], DataStructure<byte[]>> reader = dataStructureReader(sourceClient,
				ByteArrayCodec.INSTANCE);
		RedisItemWriter<byte[], byte[], DataStructure<byte[]>> writer = new WriterBuilder(targetClient)
				.dataStructure(ByteArrayCodec.INSTANCE);
		run(testInfo, reader, writer);
		RedisModulesCommands<String, String> sourceSync = sourceConnection.sync();
		RedisModulesCommands<String, String> targetSync = targetConnection.sync();
		assertEquals(sourceSync.pfcount(key1), targetSync.pfcount(key1));
	}

	protected <K, V, T extends KeyValue<K>> void liveReplication(TestInfo testInfo, RedisItemReader<K, V, T> reader,
			RedisItemWriter<K, V, T> writer, LiveRedisItemReader<K, V, T> liveReader,
			RedisItemWriter<K, V, T> liveWriter) throws Exception {
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
		liveGen.setKeyRange(IntRange.between(300, 1000));
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
		LiveRedisItemReader<byte[], byte[], KeyDump<byte[]>> reader = liveReader(sourceClient).keyDump();
		reader.getKeyspaceNotificationOptions().setQueueOptions(QueueOptions.builder().capacity(10000).build());
		SynchronizedListItemWriter<KeyDump<byte[]>> writer = new SynchronizedListItemWriter<>();
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
