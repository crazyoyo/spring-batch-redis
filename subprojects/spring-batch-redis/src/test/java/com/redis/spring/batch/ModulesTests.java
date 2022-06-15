package com.redis.spring.batch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.IteratorItemReader;
import org.springframework.batch.item.support.ListItemReader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.RedisModulesUtils;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.search.IndexInfo;
import com.redis.lettucemod.test.Beers;
import com.redis.lettucemod.timeseries.CreateOptions;
import com.redis.lettucemod.timeseries.CreateOptions.DuplicatePolicy;
import com.redis.spring.batch.compare.KeyComparator;
import com.redis.spring.batch.compare.KeyComparisonResults;
import com.redis.spring.batch.writer.operation.JsonSet;
import com.redis.spring.batch.writer.operation.Xadd;
import com.redis.testcontainers.RedisModulesContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

import io.lettuce.core.Range;
import io.lettuce.core.StreamMessage;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.search.Search;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

class ModulesTests extends AbstractTestBase {

	protected static final RedisModulesContainer REDISMOD = new RedisModulesContainer(
			RedisModulesContainer.DEFAULT_IMAGE_NAME.withTag(RedisModulesContainer.DEFAULT_TAG));

	private static final RedisModulesContainer TARGET = new RedisModulesContainer(
			RedisModulesContainer.DEFAULT_IMAGE_NAME.withTag(RedisModulesContainer.DEFAULT_TAG));

	private static final String JSON_BEER_1 = "{\"id\":\"1\",\"brewery_id\":\"812\",\"name\":\"Hocus Pocus\",\"abv\":\"4.5\",\"ibu\":\"0\",\"srm\":\"0\",\"upc\":\"0\",\"filepath\":\"\",\"descript\":\"Our take on a classic summer ale.  A toast to weeds, rays, and summer haze.  A light, crisp ale for mowing lawns, hitting lazy fly balls, and communing with nature, Hocus Pocus is offered up as a summer sacrifice to clodless days.\\n\\nIts malty sweetness finishes tart and crisp and is best apprediated with a wedge of orange.\",\"add_user\":\"0\",\"last_mod\":\"2010-07-22 20:00:20 UTC\",\"style_name\":\"Light American Wheat Ale or Lager\",\"cat_name\":\"Other Style\"}";

	@Override
	protected Collection<RedisServer> redisServers() {
		return Arrays.asList(REDISMOD, TARGET);
	}

	@Override
	protected Collection<RedisServer> testRedisServers() {
		return Arrays.asList(REDISMOD);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testJsonSet(RedisTestContext context) throws Exception {
		JsonSet<String, String, JsonNode> jsonSet = JsonSet
				.<String, String, JsonNode>key(n -> "beer:" + n.get("id").asText()).path(".").value(JsonNode::toString)
				.build();
		RedisItemWriter<String, String, JsonNode> writer = operationWriter(context, jsonSet).build();
		IteratorItemReader<JsonNode> reader = new IteratorItemReader<>(Beers.jsonNodeIterator());
		run(reader, writer);
		Assertions.assertEquals(4432, context.sync().keys("beer:*").size());
		Assertions.assertEquals(new ObjectMapper().readTree(JSON_BEER_1),
				new ObjectMapper().readTree(context.sync().jsonGet("beer:1")));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testBeerIndex(RedisTestContext context) throws Exception {
		Beers.populateIndex(context.getConnection());
		IndexInfo indexInfo = RedisModulesUtils.indexInfo(context.sync().indexInfo(Beers.INDEX));
		Assertions.assertEquals(4432, indexInfo.getNumDocs());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testMetrics(RedisTestContext context) throws Exception {
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
		generate(context);
		RedisItemReader<String, DataStructure<String>> reader = reader(context).dataStructure().build();
		reader.open(new ExecutionContext());
		Search search = registry.find("spring.batch.redis.reader.queue.size");
		Assertions.assertNotNull(search.gauge());
		reader.close();
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testStreamTransactionWriter(RedisTestContext context) throws Exception {
		String stream = "stream:1";
		List<Map<String, String>> messages = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			Map<String, String> body = new HashMap<>();
			body.put("field1", "value1");
			body.put("field2", "value2");
			messages.add(body);
		}
		ListItemReader<Map<String, String>> reader = new ListItemReader<>(messages);
		RedisItemWriter<String, String, Map<String, String>> writer = operationWriter(context,
				Xadd.<String, String, Map<String, String>>key(stream).body(t -> t).build()).multiExec().build();
		run(reader, writer);
		RedisModulesCommands<String, String> sync = context.sync();
		Assertions.assertEquals(messages.size(), sync.xlen(stream));
		List<StreamMessage<String, String>> xrange = sync.xrange(stream, Range.create("-", "+"));
		for (int index = 0; index < xrange.size(); index++) {
			StreamMessage<String, String> message = xrange.get(index);
			Assertions.assertEquals(messages.get(index), message.getBody());
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testComparator(RedisTestContext server) throws Exception {
		RedisTestContext target = getContext(TARGET);
		server.sync().addAutoTimestamp("ts:1", 123);
		KeyComparator comparator = comparator(server, target).build();
		KeyComparisonResults results = comparator.call();
		Assertions.assertEquals(1, results.getMissing());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testJSONReplication(RedisTestContext server) throws Exception {
		server.sync().jsonSet("json:1", "$", JSON_BEER_1);
		server.sync().jsonSet("json:2", "$", JSON_BEER_1);
		server.sync().jsonSet("json:3", "$", JSON_BEER_1);
		RedisItemReader<String, DataStructure<String>> reader = dataStructureReader(server);
		RedisTestContext target = getContext(TARGET);
		run(reader, dataStructureWriter(target));
		compare(server, target);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testTimeSeriesReplication(RedisTestContext server) throws Exception {
		String key = "ts:1";
		server.sync().add(key, 1000, 1, CreateOptions.<String, String>builder().policy(DuplicatePolicy.LAST).build());
		server.sync().add(key, 1001, 2);
		server.sync().add(key, 1003, 3);
		RedisItemReader<String, DataStructure<String>> reader = dataStructureReader(server);
		RedisTestContext target = getContext(TARGET);
		run(reader, dataStructureWriter(target));
		compare(server, target);
	}
}
