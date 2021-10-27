package com.redis.spring.batch;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.core.convert.converter.Converter;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.spring.batch.RedisItemWriter.RedisItemWriterBuilder;
import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.DataStructureValueReader;
import com.redis.spring.batch.support.KeyValue;
import com.redis.spring.batch.support.LiveRedisItemReader;
import com.redis.spring.batch.support.PollableItemReader;
import com.redis.spring.batch.support.RedisOperation;
import com.redis.spring.batch.support.convert.ArrayConverter;
import com.redis.spring.batch.support.convert.GeoValueConverter;
import com.redis.spring.batch.support.convert.KeyMaker;
import com.redis.spring.batch.support.convert.MapFlattener;
import com.redis.spring.batch.support.convert.ScoredValueConverter;
import com.redis.spring.batch.support.generator.Generator.DataType;
import com.redis.spring.batch.support.operation.Geoadd;
import com.redis.spring.batch.support.operation.Hset;
import com.redis.spring.batch.support.operation.Xadd;
import com.redis.spring.batch.support.operation.Zadd;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.GeoArgs;
import io.lettuce.core.GeoValue;
import io.lettuce.core.Range;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import lombok.Builder;
import lombok.Data;

@SuppressWarnings("unchecked")
public class BatchTests extends AbstractRedisTestBase {

	@ParameterizedTest
	@MethodSource("servers")
	public void testFlushingStep(RedisServer redis) throws Exception {
		String name = "flushing-step";
		PollableItemReader<String> reader = keyspaceNotificationReader(redis);
		ListItemWriter<String> writer = new ListItemWriter<>();
		JobExecution execution = runFlushing(redis, name, reader, null, writer);
		execute(dataGenerator(redis, name).end(3).dataTypes(DataType.STRING, DataType.HASH));
		awaitTermination(execution);
		RedisModulesCommands<String, String> commands = sync(redis);
		Assertions.assertEquals(commands.dbsize(), writer.getWrittenItems().size());
	}

	private PollableItemReader<String> keyspaceNotificationReader(RedisServer redis) {
		return LiveRedisItemReader.dataStructure(jobRepository, transactionManager, client(redis)).live().keyReader();
	}

	@ParameterizedTest
	@MethodSource("servers")
	public void testKeyspaceNotificationReader(RedisServer redis) throws Exception {
		String name = "keyspace-notification-reader";
		PollableItemReader<String> reader = keyspaceNotificationReader(redis);
		reader.open(new ExecutionContext());
		execute(dataGenerator(redis, name).end(100));
		int actualCount = 0;
		while (reader.read() != null) {
			actualCount++;
		}
		Assertions.assertEquals(sync(redis).dbsize(), actualCount);
		reader.close();
	}

	@ParameterizedTest
	@MethodSource("servers")
	public void testDataStructureReader(RedisServer redis) throws Exception {
		String name = "ds-reader";
		populateSource(redis, name);
		RedisItemReader<String, DataStructure<String>> reader = dataStructureReader(redis, name);
		ListItemWriter<DataStructure<String>> writer = new ListItemWriter<>();
		run(redis, name, reader, writer);
		RedisModulesCommands<String, String> sync = sync(redis);
		Assertions.assertEquals(sync.dbsize(), writer.getWrittenItems().size());
	}

	private void populateSource(RedisServer server, String name) throws Exception {
		JsonItemReader<Map<String, Object>> reader = Beers.mapReader();
		RedisItemWriter<String, String, Map<String, String>> writer = redisItemWriter(server,
				Hset.<Map<String, String>>key(t -> t.get("id")).map(t -> t).build()).build();
		run(server, name + "-populate", reader, new MapFlattener(), writer);
	}

	private <T> RedisItemWriterBuilder<String, String, T> redisItemWriter(RedisServer server,
			RedisOperation<String, String, T> operation) {
		if (server.isCluster()) {
			return RedisItemWriter.operation(redisClusterClient(server), operation);
		}
		return RedisItemWriter.operation(redisClient(server), operation);
	}

	@ParameterizedTest
	@MethodSource("servers")
	public void testMultiThreadedReader(RedisServer server) throws Exception {
		String name = "multi-threaded-reader";
		populateSource(server, name);
		RedisItemReader<String, DataStructure<String>> reader = dataStructureReader(server, name);
		SynchronizedItemStreamReader<DataStructure<String>> synchronizedReader = new SynchronizedItemStreamReader<>();
		synchronizedReader.setDelegate(reader);
		synchronizedReader.afterPropertiesSet();
		SynchronizedListItemWriter<DataStructure<String>> writer = new SynchronizedListItemWriter<>();
		int threads = 4;
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setMaxPoolSize(threads);
		taskExecutor.setCorePoolSize(threads);
		taskExecutor.afterPropertiesSet();
		run(job(server, name, step(server, name, synchronizedReader, null, writer).taskExecutor(taskExecutor)
				.throttleLimit(threads).build()).build());
		RedisModulesCommands<String, String> sync = sync(server);
		Assertions.assertEquals(sync.dbsize(), writer.getWrittenItems().size());
	}

	private static class SynchronizedListItemWriter<T> implements ItemWriter<T> {

		private final List<T> writtenItems = Collections.synchronizedList(new ArrayList<>());

		@Override
		public void write(List<? extends T> items) {
			writtenItems.addAll(items);
		}

		public List<? extends T> getWrittenItems() {
			return this.writtenItems;
		}
	}

	@ParameterizedTest
	@MethodSource("servers")
	public void testStreamWriter(RedisServer redis) throws Exception {
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
				Xadd.<Map<String, String>>key(stream).body(t -> t).build()).build();
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
	public void testStreamTransactionWriter() throws Exception {
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
				Xadd.<Map<String, String>>key(stream).body(t -> t).build()).multiExec().build();
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
	public void testRedisItemWriterWait(RedisServer server) throws Exception {
		List<Map<String, String>> maps = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			Map<String, String> body = new HashMap<>();
			body.put("id", String.valueOf(index));
			body.put("field1", "value1");
			body.put("field2", "value2");
			maps.add(body);
		}
		ListItemReader<Map<String, String>> reader = new ListItemReader<>(maps);
		KeyMaker<Map<String, String>> keyConverter = KeyMaker.<Map<String, String>>builder().prefix("hash")
				.converters(h -> h.remove("id")).build();
		RedisItemWriter<String, String, Map<String, String>> writer = redisItemWriter(server,
				Hset.key(keyConverter).map(m -> m).build()).waitForReplication(1, 300).build();
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
	public void testHashWriter(RedisServer server) throws Exception {
		List<Map<String, String>> maps = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			Map<String, String> body = new HashMap<>();
			body.put("id", String.valueOf(index));
			body.put("field1", "value1");
			body.put("field2", "value2");
			maps.add(body);
		}
		ListItemReader<Map<String, String>> reader = new ListItemReader<>(maps);
		KeyMaker<Map<String, String>> keyConverter = KeyMaker.<Map<String, String>>builder().prefix("hash")
				.converters(h -> h.remove("id")).build();
		RedisItemWriter<String, String, Map<String, String>> writer = redisItemWriter(server,
				Hset.key(keyConverter).map(m -> m).build()).build();
		run(server, "hash-writer", reader, writer);
		RedisModulesCommands<String, String> sync = sync(server);
		Assertions.assertEquals(maps.size(), sync.keys("hash:*").size());
		RedisModulesCommands<String, String> hashCommands = sync(server);
		for (int index = 0; index < maps.size(); index++) {
			Map<String, String> hash = hashCommands.hgetall("hash:" + index);
			Assertions.assertEquals(maps.get(index), hash);
		}
	}

	@Data
	@Builder
	private static class Geo {
		private final String member;
		private final double longitude;
		private final double latitude;
	}

	@SuppressWarnings("rawtypes")
	@ParameterizedTest
	@MethodSource("servers")
	public void testGeoaddWriter(RedisServer redis) throws Exception {
		ListItemReader<Geo> reader = new ListItemReader<>(Arrays.asList(
				Geo.builder().longitude(-118.476056).latitude(33.985728).member("Venice Breakwater").build(),
				Geo.builder().longitude(-73.667022).latitude(40.582739).member("Long Beach National").build()));
		Converter<Geo, GeoValue<String>> value = new GeoValueConverter<>(Geo::getMember, Geo::getLongitude,
				Geo::getLatitude);
		Converter<Geo, GeoValue<String>[]> converter = (Converter) new ArrayConverter<>(GeoValue.class, value);
		RedisItemWriter<String, String, Geo> writer = redisItemWriter(redis,
				Geoadd.<Geo>key("geoset").values(converter).build()).build();
		run(redis, "geoadd-writer", reader, writer);
		RedisModulesCommands<String, String> sync = sync(redis);
		Set<String> radius1 = sync.georadius("geoset", -118, 34, 100, GeoArgs.Unit.mi);
		Assertions.assertEquals(1, radius1.size());
		Assertions.assertTrue(radius1.contains("Venice Breakwater"));
	}

	@ParameterizedTest
	@MethodSource("servers")
	public void testHashDelWriter(RedisServer server) throws Exception {
		List<Map.Entry<String, Map<String, String>>> hashes = new ArrayList<>();
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
		KeyMaker<Map.Entry<String, Map<String, String>>> keyConverter = KeyMaker.<Map.Entry<String, Map<String, String>>>builder()
				.prefix("hash").converters(Entry<String, Map<String, String>>::getKey).build();
		RedisItemWriter<String, String, Map.Entry<String, Map<String, String>>> writer = redisItemWriter(server,
				Hset.key(keyConverter).map(Map.Entry::getValue).build()).build();
		run(server, "hash-del-writer", reader, writer);
		Assertions.assertEquals(50, sync.keys("hash:*").size());
		Assertions.assertEquals(2, commands.hgetall("hash:50").size());
	}

	@Data
	@Builder
	private static class ZValue {
		private final String member;
		private final double score;
	}

	@SuppressWarnings("rawtypes")
	@ParameterizedTest
	@MethodSource("servers")
	public void testSortedSetWriter(RedisServer server) throws Exception {
		List<ZValue> values = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			values.add(ZValue.builder().member(String.valueOf(index)).score(index % 10).build());
		}
		Converter<ZValue, ScoredValue<String>[]> converter = (Converter) new ArrayConverter<>(ScoredValue.class,
				new ScoredValueConverter<>(ZValue::getMember, ZValue::getScore));
		ListItemReader<ZValue> reader = new ListItemReader<>(values);
		RedisItemWriter<String, String, ZValue> writer = redisItemWriter(server,
				Zadd.<ZValue>key("zset").values(converter).build()).build();
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
	public void testDataStructureWriter(RedisServer redis) throws Exception {
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
		Assertions.assertEquals(count, keys.size());
	}

	@ParameterizedTest
	@MethodSource("servers")
	public void testLiveReader(RedisServer redis) throws Exception {
		String name = "live-reader";
		LiveRedisItemReader<String, KeyValue<String, byte[]>> reader = liveKeyDumpReader(redis, name, 10000);
		ListItemWriter<KeyValue<String, byte[]>> writer = new ListItemWriter<>();
		JobExecution execution = runFlushing(redis, name, reader, null, writer);
		execute(dataGenerator(redis, name).end(123).dataTypes(DataType.HASH, DataType.STRING));
		awaitTermination(execution);
		RedisModulesCommands<String, String> sync = sync(redis);
		Assertions.assertEquals(sync.dbsize(), writer.getWrittenItems().size());
	}

	@ParameterizedTest
	@MethodSource("servers")
	public void testKeyValueItemReaderFaultTolerance(RedisServer redis) throws Exception {
		String name = "reader-ft";
		execute(dataGenerator(redis, name).dataTypes(DataType.STRING));
		List<String> keys = IntStream.range(0, 100).boxed().map(i -> DataType.STRING + ":" + i)
				.collect(Collectors.toList());
		DelegatingPollableItemReader<String> keyReader = DelegatingPollableItemReader.<String>builder()
				.delegate(new ListItemReader<>(keys)).exceptionSupplier(TimeoutException::new).interval(2).build();
		DataStructureValueReader<String, String> valueReader = dataStructureValueReader(redis);
		RedisItemReader<String, DataStructure<String>> reader = new RedisItemReader<>(jobRepository, transactionManager,
				keyReader, valueReader, 1, 1, new LinkedBlockingQueue<>(1000), Duration.ofMillis(100),
				new AlwaysSkipItemSkipPolicy());
		reader.setName(name(redis, name + "-reader"));
		ListItemWriter<DataStructure<String>> writer = new ListItemWriter<>();
		run(redis, name, reader, writer);
		Assertions.assertEquals(50, writer.getWrittenItems().size());
	}

}
