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

	protected static final RedisModulesContainer REDIS = new RedisModulesContainer(
			RedisModulesContainer.DEFAULT_IMAGE_NAME.withTag(RedisModulesContainer.DEFAULT_TAG));

	@Override
	protected Collection<RedisServer> redisServers() {
		return Arrays.asList(REDIS);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testJsonSet(RedisTestContext context) throws Exception {
		JsonSet<String, String, JsonNode> jsonSet = JsonSet
				.<String, String, JsonNode>key(n -> "beer:" + n.get("id").asText()).path(".").value(JsonNode::toString)
				.build();
		RedisItemWriter<String, String, JsonNode> writer = operationWriter(context, jsonSet).build();
		IteratorItemReader<JsonNode> reader = new IteratorItemReader<>(Beers.jsonNodeIterator());
		run(context, "json-set", reader, writer);
		Assertions.assertEquals(4432, context.sync().keys("beer:*").size());
		String beer1 = "{\"id\":\"1\",\"brewery_id\":\"812\",\"name\":\"Hocus Pocus\",\"abv\":\"4.5\",\"ibu\":\"0\",\"srm\":\"0\",\"upc\":\"0\",\"filepath\":\"\",\"descript\":\"Our take on a classic summer ale.  A toast to weeds, rays, and summer haze.  A light, crisp ale for mowing lawns, hitting lazy fly balls, and communing with nature, Hocus Pocus is offered up as a summer sacrifice to clodless days.\\n\\nIts malty sweetness finishes tart and crisp and is best apprediated with a wedge of orange.\",\"add_user\":\"0\",\"last_mod\":\"2010-07-22 20:00:20 UTC\",\"style_name\":\"Light American Wheat Ale or Lager\",\"cat_name\":\"Other Style\"}";
		Assertions.assertEquals(new ObjectMapper().readTree(beer1),
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
		dataGenerator(context, "metrics").build().call();
		RedisItemReader<String, DataStructure<String>> reader = configureJobRepository(
				reader(context).dataStructureIntrospect()).build();
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
		run(context, "stream-tx-writer", reader, writer);
		RedisModulesCommands<String, String> sync = context.sync();
		Assertions.assertEquals(messages.size(), sync.xlen(stream));
		List<StreamMessage<String, String>> xrange = sync.xrange(stream, Range.create("-", "+"));
		for (int index = 0; index < xrange.size(); index++) {
			StreamMessage<String, String> message = xrange.get(index);
			Assertions.assertEquals(messages.get(index), message.getBody());
		}
	}
}
