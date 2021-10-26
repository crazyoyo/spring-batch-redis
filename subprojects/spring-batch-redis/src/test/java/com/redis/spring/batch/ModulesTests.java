package com.redis.spring.batch;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.Utils;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.search.IndexInfo;
import com.redis.lettucemod.api.sync.RedisJSONCommands;
import com.redis.spring.batch.support.operation.JsonSet;
import com.redis.testcontainers.RedisModulesContainer;

@Testcontainers
public class ModulesTests extends AbstractTestBase {

	@Container
	protected static final RedisModulesContainer REDIS = new RedisModulesContainer();

	private RedisModulesClient client;
	private StatefulRedisModulesConnection<String, String> connection;

	@BeforeEach
	public void setup() {
		client = RedisModulesClient.create(REDIS.getRedisURI());
		connection = client.connect();
		connection.sync().flushall();
	}

	@AfterEach
	public void teardown() {
		connection.close();
		client.shutdown();
		client.getResources().shutdown();
	}

	@Test
	public void testJsonSet() throws Exception {
		connection.sync().flushall();
		RedisItemWriter<String, String, JsonNode> writer = RedisItemWriter.operation(
				JsonSet.<JsonNode>key(n -> "beer:" + n.get("id").asText()).path(".").value(JsonNode::toString).build())
				.client(client).build();
		run(REDIS, "json-set", Beers.jsonNodeReader(), writer);
		Assertions.assertEquals(4432, connection.sync().keys("beer:*").size());
		String beer1 = "{\"id\":\"1\",\"brewery_id\":\"812\",\"name\":\"Hocus Pocus\",\"abv\":\"4.5\",\"ibu\":\"0\",\"srm\":\"0\",\"upc\":\"0\",\"filepath\":\"\",\"descript\":\"Our take on a classic summer ale.  A toast to weeds, rays, and summer haze.  A light, crisp ale for mowing lawns, hitting lazy fly balls, and communing with nature, Hocus Pocus is offered up as a summer sacrifice to clodless days.\\n\\nIts malty sweetness finishes tart and crisp and is best apprediated with a wedge of orange.\",\"add_user\":\"0\",\"last_mod\":\"2010-07-22 20:00:20 UTC\",\"style_name\":\"Light American Wheat Ale or Lager\",\"cat_name\":\"Other Style\"}";
		RedisJSONCommands<String, String> jsonCommands = connection.sync();
		Assertions.assertEquals(new ObjectMapper().readTree(beer1),
				new ObjectMapper().readTree(jsonCommands.jsonGet("beer:1")));
	}

	@Test
	public void testBeerIndex() throws Exception {
		Beers.createIndex(connection.sync());
		Beers.populateIndex(jobRepository, transactionManager, client);
		IndexInfo indexInfo = Utils.indexInfo(connection.sync().indexInfo(Beers.INDEX));
		Assertions.assertEquals(4432, indexInfo.getNumDocs());
	}
}
