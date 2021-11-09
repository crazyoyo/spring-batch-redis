package com.redis.spring.batch;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.item.support.ListItemReader;

import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.convert.GeoValueConverter;
import com.redis.spring.batch.support.convert.ScoredValueConverter;
import com.redis.spring.batch.support.operation.Geoadd;
import com.redis.spring.batch.support.operation.Hset;
import com.redis.spring.batch.support.operation.Xadd;
import com.redis.spring.batch.support.operation.Zadd;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.GeoArgs;
import io.lettuce.core.Range;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.StreamMessage;

class RedisItemWriterTests extends AbstractRedisTestBase {

	@ParameterizedTest
	@MethodSource("servers")
	void testStreamWriter(RedisServer redis) throws Exception {
		String stream = "stream:0";
		List<Map<String, String>> messages = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			Map<String, String> body = new HashMap<>();
			body.put("field1", "value1");
			body.put("field2", "value2");
			messages.add(body);
		}
		ListItemReader<Map<String, String>> reader = new ListItemReader<>(messages);
		RedisItemWriter<String, String, Map<String, String>> writer = redisItemWriter(redis,
				Xadd.<String, String, Map<String, String>>key(stream).body(t -> t).build()).build();
		run(redis, "stream-writer", reader, writer);
		RedisModulesCommands<String, String> sync = sync(redis);
		Assertions.assertEquals(messages.size(), sync.xlen(stream));
		List<StreamMessage<String, String>> xrange = sync.xrange(stream, Range.create("-", "+"));
		for (int index = 0; index < xrange.size(); index++) {
			StreamMessage<String, String> message = xrange.get(index);
			Assertions.assertEquals(messages.get(index), message.getBody());
		}
	}

	@Test
	void testStreamTransactionWriter() throws Exception {
		String stream = "stream:1";
		List<Map<String, String>> messages = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			Map<String, String> body = new HashMap<>();
			body.put("field1", "value1");
			body.put("field2", "value2");
			messages.add(body);
		}
		ListItemReader<Map<String, String>> reader = new ListItemReader<>(messages);
		RedisItemWriter<String, String, Map<String, String>> writer = redisItemWriter(REDIS,
				Xadd.<String, String, Map<String, String>>key(stream).body(t -> t).build()).multiExec().build();
		run(REDIS, "stream-tx-writer", reader, writer);
		RedisModulesCommands<String, String> sync = sync(REDIS);
		Assertions.assertEquals(messages.size(), sync.xlen(stream));
		List<StreamMessage<String, String>> xrange = sync.xrange(stream, Range.create("-", "+"));
		for (int index = 0; index < xrange.size(); index++) {
			StreamMessage<String, String> message = xrange.get(index);
			Assertions.assertEquals(messages.get(index), message.getBody());
		}
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testRedisItemWriterWait(RedisServer server) throws Exception {
		List<Map<String, String>> maps = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			Map<String, String> body = new HashMap<>();
			body.put("id", String.valueOf(index));
			body.put("field1", "value1");
			body.put("field2", "value2");
			maps.add(body);
		}
		ListItemReader<Map<String, String>> reader = new ListItemReader<>(maps);
		RedisItemWriter<String, String, Map<String, String>> writer = redisItemWriter(server,
				Hset.<String, String, Map<String, String>>key(m -> "hash:" + m.remove("id")).map(m -> m).build())
						.waitForReplication(1, 300).build();
		JobExecution execution = run(server, "writer-wait", reader, writer);
		Assertions.assertEquals(ExitStatus.FAILED.getExitCode(), execution.getExitStatus().getExitCode());
		Assertions.assertEquals(1, execution.getAllFailureExceptions().size());
		Assertions.assertEquals(RedisCommandExecutionException.class,
				execution.getAllFailureExceptions().get(0).getClass());
		Assertions.assertEquals("Insufficient replication level - expected: 1, actual: 0",
				execution.getAllFailureExceptions().get(0).getMessage());
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testHashWriter(RedisServer server) throws Exception {
		List<Map<String, String>> maps = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			Map<String, String> body = new HashMap<>();
			body.put("id", String.valueOf(index));
			body.put("field1", "value1");
			body.put("field2", "value2");
			maps.add(body);
		}
		ListItemReader<Map<String, String>> reader = new ListItemReader<>(maps);
		RedisItemWriter<String, String, Map<String, String>> writer = redisItemWriter(server,
				Hset.<String, String, Map<String, String>>key(m -> "hash:" + m.remove("id")).map(m -> m).build())
						.build();
		run(server, "hash-writer", reader, writer);
		RedisModulesCommands<String, String> sync = sync(server);
		Assertions.assertEquals(maps.size(), sync.keys("hash:*").size());
		RedisModulesCommands<String, String> hashCommands = sync(server);
		for (int index = 0; index < maps.size(); index++) {
			Map<String, String> hash = hashCommands.hgetall("hash:" + index);
			Assertions.assertEquals(maps.get(index), hash);
		}
	}

	private static class Geo {
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
	@MethodSource("servers")
	void testGeoaddWriter(RedisServer redis) throws Exception {
		ListItemReader<Geo> reader = new ListItemReader<>(
				Arrays.asList(new Geo("Venice Breakwater", -118.476056, 33.985728),
						new Geo("Long Beach National", -73.667022, 40.582739)));
		GeoValueConverter<String, Geo> value = new GeoValueConverter<>(Geo::getMember, Geo::getLongitude,
				Geo::getLatitude);
		RedisItemWriter<String, String, Geo> writer = redisItemWriter(redis,
				Geoadd.<String, String, Geo>key("geoset").value(value).build()).build();
		run(redis, "geoadd-writer", reader, writer);
		RedisModulesCommands<String, String> sync = sync(redis);
		Set<String> radius1 = sync.georadius("geoset", -118, 34, 100, GeoArgs.Unit.mi);
		Assertions.assertEquals(1, radius1.size());
		Assertions.assertTrue(radius1.contains("Venice Breakwater"));
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testHashDelWriter(RedisServer server) throws Exception {
		List<Entry<String, Map<String, String>>> hashes = new ArrayList<>();
		RedisModulesCommands<String, String> commands = sync(server);
		for (int index = 0; index < 100; index++) {
			String key = String.valueOf(index);
			Map<String, String> value = new HashMap<>();
			value.put("field1", "value1");
			commands.hset("hash:" + key, value);
			Map<String, String> body = new HashMap<>();
			body.put("field2", "value2");
			hashes.add(new AbstractMap.SimpleEntry<>(key, index < 50 ? null : body));
		}
		RedisModulesCommands<String, String> sync = sync(server);
		ListItemReader<Map.Entry<String, Map<String, String>>> reader = new ListItemReader<>(hashes);
		RedisItemWriter<String, String, Map.Entry<String, Map<String, String>>> writer = redisItemWriter(server,
				Hset.<String, String, Entry<String, Map<String, String>>>key(e -> "hash:" + e.getKey())
						.map(Map.Entry::getValue).build()).build();
		run(server, "hash-del-writer", reader, writer);
		Assertions.assertEquals(50, sync.keys("hash:*").size());
		Assertions.assertEquals(2, commands.hgetall("hash:50").size());
	}

	private static class ZValue {

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
	@MethodSource("servers")
	void testSortedSetWriter(RedisServer server) throws Exception {
		List<ZValue> values = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			values.add(new ZValue(String.valueOf(index), index % 10));
		}
		ListItemReader<ZValue> reader = new ListItemReader<>(values);
		ScoredValueConverter<String, ZValue> converter = new ScoredValueConverter<>(ZValue::getMember,
				ZValue::getScore);
		RedisItemWriter<String, String, ZValue> writer = redisItemWriter(server,
				Zadd.<String, String, ZValue>key("zset").value(converter).build()).build();
		run(server, "sorted-set-writer", reader, writer);
		RedisModulesCommands<String, String> sync = sync(server);
		Assertions.assertEquals(1, sync.dbsize());
		Assertions.assertEquals(values.size(), sync.zcard("zset"));
		List<String> range = sync.zrangebyscore("zset",
				Range.from(Range.Boundary.including(0), Range.Boundary.including(5)));
		Assertions.assertEquals(60, range.size());
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testDataStructureWriter(RedisServer redis) throws Exception {
		List<DataStructure<String>> list = new ArrayList<>();
		long count = 100;
		for (int index = 0; index < count; index++) {
			Map<String, String> map = new HashMap<>();
			map.put("field1", "value1");
			map.put("field2", "value2");
			list.add(DataStructure.hash("hash:" + index, map));
		}
		ListItemReader<DataStructure<String>> reader = new ListItemReader<>(list);
		RedisItemWriter<String, String, DataStructure<String>> writer = dataStructureWriter(redis);
		run(redis, "value-writer", reader, writer);
		RedisModulesCommands<String, String> sync = sync(redis);
		List<String> keys = sync.keys("hash:*");
		Awaitility.await().until(() -> count == keys.size());
	}

}
