package com.redis.spring.batch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.IteratorItemReader;
import org.springframework.batch.item.support.ListItemReader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.Beers;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.search.IndexInfo;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.CreateOptions;
import com.redis.lettucemod.timeseries.DuplicatePolicy;
import com.redis.lettucemod.timeseries.RangeOptions;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.lettucemod.timeseries.TimeRange;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.convert.IdentityConverter;
import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.spring.batch.writer.KeyComparisonCountItemWriter;
import com.redis.spring.batch.writer.WriterOptions;
import com.redis.spring.batch.writer.operation.JsonSet;
import com.redis.spring.batch.writer.operation.TsAdd;
import com.redis.spring.batch.writer.operation.Xadd;

import io.lettuce.core.Range;
import io.lettuce.core.StreamMessage;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.search.Search;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

abstract class AbstractModulesTests extends AbstractTestBase {

	private static final String JSON_BEER_1 = "{\"id\":\"1\",\"brewery_id\":\"812\",\"name\":\"Hocus Pocus\",\"abv\":\"4.5\",\"ibu\":\"0\",\"srm\":\"0\",\"upc\":\"0\",\"filepath\":\"\",\"descript\":\"Our take on a classic summer ale.  A toast to weeds, rays, and summer haze.  A light, crisp ale for mowing lawns, hitting lazy fly balls, and communing with nature, Hocus Pocus is offered up as a summer sacrifice to clodless days.\\n\\nIts malty sweetness finishes tart and crisp and is best apprediated with a wedge of orange.\",\"add_user\":\"0\",\"last_mod\":\"2010-07-22 20:00:20 UTC\",\"style_name\":\"Light American Wheat Ale or Lager\",\"cat_name\":\"Other Style\"}";

	private static final int BEER_COUNT = 1019;

	@Test
	void jsonSet() throws Exception {
		JsonSet<String, String, JsonNode> jsonSet = JsonSet.<String, JsonNode>key(n -> "beer:" + n.get("id").asText())
				.value(JsonNode::toString).path(".").build();
		RedisItemWriter<String, String, JsonNode> writer = RedisItemWriter.operation(sourcePool, jsonSet).build();
		IteratorItemReader<JsonNode> reader = new IteratorItemReader<>(Beers.jsonNodeIterator());
		run(reader, null, writer, DEFAULT_STEP_OPTIONS);
		Assertions.assertEquals(BEER_COUNT, sourceConnection.sync().keys("beer:*").size());
		Assertions.assertEquals(new ObjectMapper().readTree(JSON_BEER_1),
				new ObjectMapper().readTree(sourceConnection.sync().jsonGet("beer:1")));
	}

	@Test
	void tsAdd() throws Exception {
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
		RedisItemWriter<String, String, Sample> writer = RedisItemWriter.operation(sourcePool, tsadd).build();
		run(reader, null, writer, DEFAULT_STEP_OPTIONS);
		Assertions.assertEquals(count / 2,
				sourceConnection.sync().tsRange(key, TimeRange.unbounded(), RangeOptions.builder().build()).size(), 2);
	}

	void beerIndex() throws Exception {
		Beers.populateIndex(sourceConnection);
		IndexInfo indexInfo = RedisModulesUtils.indexInfo(sourceConnection.sync().ftInfo(Beers.INDEX));
		Assertions.assertEquals(BEER_COUNT, indexInfo.getNumDocs());
	}

	@Test
	void metrics() throws Exception {
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
		generate();
		RedisItemReader<String, DataStructure<String>> reader = RedisItemReader.dataStructure(sourcePool, jobRunner)
				.build();
		reader.open(new ExecutionContext());
		Search search = registry.find("spring.batch.redis.reader.queue.size");
		Assertions.assertNotNull(search.gauge());
		reader.close();
	}

	@Test
	void writeStreamTx() throws Exception {
		String stream = "stream:1";
		List<Map<String, String>> messages = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			Map<String, String> body = new HashMap<>();
			body.put("field1", "value1");
			body.put("field2", "value2");
			messages.add(body);
		}
		ListItemReader<Map<String, String>> reader = new ListItemReader<>(messages);
		RedisItemWriter<String, String, Map<String, String>> writer = RedisItemWriter
				.operation(sourcePool,
						Xadd.<String, Map<String, String>>key(stream).body(IdentityConverter.instance()).build())
				.options(WriterOptions.builder().multiExec(true).build()).build();
		run(reader, null, writer, DEFAULT_STEP_OPTIONS);
		RedisModulesCommands<String, String> sync = sourceConnection.sync();
		Assertions.assertEquals(messages.size(), sync.xlen(stream));
		List<StreamMessage<String, String>> xrange = sync.xrange(stream, Range.create("-", "+"));
		for (int index = 0; index < xrange.size(); index++) {
			StreamMessage<String, String> message = xrange.get(index);
			Assertions.assertEquals(messages.get(index), message.getBody());
		}
	}

	@Test
	void comparator() throws Exception {
		sourceConnection.sync().tsAdd("ts:1", Sample.of(123));
		RedisItemReader<String, KeyComparison<String>> reader = comparisonReader();
		KeyComparisonCountItemWriter<String> writer = new KeyComparisonCountItemWriter<>();
		run(reader, writer);
		Assertions.assertEquals(1, writer.getResults().getCount(Status.MISSING));
	}

	@Test
	void replicateJSON() throws Exception {
		sourceConnection.sync().jsonSet("json:1", "$", JSON_BEER_1);
		sourceConnection.sync().jsonSet("json:2", "$", JSON_BEER_1);
		sourceConnection.sync().jsonSet("json:3", "$", JSON_BEER_1);
		run(dataStructureReader().build(), RedisItemWriter.dataStructure(targetPool).build());
		Assertions.assertTrue(compare());
	}

	@Test
	void replicateTimeSeries() throws Exception {
		String key = "ts:1";
		sourceConnection.sync().tsCreate(key,
				CreateOptions.<String, String>builder().policy(DuplicatePolicy.LAST).build());
		sourceConnection.sync().tsAdd(key, Sample.of(1000, 1));
		sourceConnection.sync().tsAdd(key, Sample.of(1001, 2));
		sourceConnection.sync().tsAdd(key, Sample.of(1003, 3));
		run(dataStructureReader().build(), RedisItemWriter.dataStructure(targetPool).build());
		Assertions.assertTrue(compare());
	}
}
