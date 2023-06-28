package com.redis.spring.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.IteratorItemReader;
import org.springframework.batch.item.support.ListItemReader;

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
import com.redis.spring.batch.convert.SampleConverter;
import com.redis.spring.batch.convert.SuggestionConverter;
import com.redis.spring.batch.reader.DataStructureReadOperation;
import com.redis.spring.batch.reader.GeneratorItemReader;
import com.redis.spring.batch.reader.GeneratorItemReader.Type;
import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.spring.batch.reader.KeyEventType;
import com.redis.spring.batch.reader.KeyspaceNotification;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader;
import com.redis.spring.batch.reader.StringDataStructureReadOperation;
import com.redis.spring.batch.writer.KeyComparisonCountItemWriter;
import com.redis.spring.batch.writer.operation.JsonSet;
import com.redis.spring.batch.writer.operation.Sugadd;
import com.redis.spring.batch.writer.operation.SugaddIncr;
import com.redis.spring.batch.writer.operation.TsAdd;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;

public abstract class AbstractModulesBatchTests extends AbstractBatchTests {

	private static final String JSON_BEER_1 = "{\"id\":\"1\",\"brewery_id\":\"812\",\"name\":\"Hocus Pocus\",\"abv\":\"4.5\",\"ibu\":\"0\",\"srm\":\"0\",\"upc\":\"0\",\"filepath\":\"\",\"descript\":\"Our take on a classic summer ale.  A toast to weeds, rays, and summer haze.  A light, crisp ale for mowing lawns, hitting lazy fly balls, and communing with nature, Hocus Pocus is offered up as a summer sacrifice to clodless days.\\n\\nIts malty sweetness finishes tart and crisp and is best apprediated with a wedge of orange.\",\"add_user\":\"0\",\"last_mod\":\"2010-07-22 20:00:20 UTC\",\"style_name\":\"Light American Wheat Ale or Lager\",\"cat_name\":\"Other Style\"}";
	private static final int BEER_COUNT = 1019;

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
		reader.setTypes(DataStructure.HASH);
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
			Assertions.assertEquals(DataStructure.HASH, notification.getEventType().getType());
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
		RedisItemReader<String, String, KeyComparison> reader = comparisonReader();
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
		StatefulRedisModulesConnection<byte[], byte[]> byteConnection = RedisModulesUtils.connection(sourceClient,
				ByteArrayCodec.INSTANCE);
		Future<DataStructure<byte[]>> future = operation.execute(byteConnection.async(), keyBytes(key));
		DataStructure<byte[]> ds = future.get();
		Assertions.assertArrayEquals(keyBytes(key), ds.getKey());
		Assertions.assertEquals(DataStructure.TIMESERIES, ds.getType());
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

}
