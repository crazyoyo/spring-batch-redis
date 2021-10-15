package org.springframework.batch.item.redis;

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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.redis.support.AbstractKeyspaceNotificationItemReader;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.DataStructureValueReader;
import org.springframework.batch.item.redis.support.JobFactory;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.KeyValueItemReader;
import org.springframework.batch.item.redis.support.LiveKeyValueItemReader;
import org.springframework.batch.item.redis.support.RedisClusterKeyspaceNotificationItemReader;
import org.springframework.batch.item.redis.support.RedisKeyspaceNotificationItemReader;
import org.springframework.batch.item.redis.support.Timer;
import org.springframework.batch.item.redis.support.convert.GeoValueConverter;
import org.springframework.batch.item.redis.support.convert.KeyMaker;
import org.springframework.batch.item.redis.support.convert.MapFlattener;
import org.springframework.batch.item.redis.support.operation.Geoadd;
import org.springframework.batch.item.redis.support.operation.Hset;
import org.springframework.batch.item.redis.support.operation.Xadd;
import org.springframework.batch.item.redis.support.operation.Zadd;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.GeoArgs;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import io.lettuce.core.api.sync.RedisGeoCommands;
import io.lettuce.core.api.sync.RedisHashCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import io.lettuce.core.api.sync.RedisSortedSetCommands;
import io.lettuce.core.api.sync.RedisStreamCommands;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuppressWarnings("unchecked")
public class BatchTests extends AbstractRedisTestBase {

	@ParameterizedTest
	@MethodSource("servers")
	void testFlushingStep(RedisServer redis) throws Throwable {
		AbstractKeyspaceNotificationItemReader<?> reader = keyEventReader(redis);
		ListItemWriter<String> writer = new ListItemWriter<>();
		JobFactory.JobExecutionWrapper execution = jobFactory.runFlushing(name(redis, "flushing"), reader, writer);
		dataGenerator(redis).end(3).maxExpire(Duration.ofMillis(0)).dataTypes(DataStructure.STRING, DataStructure.HASH)
				.build().call();
		execution.awaitTermination();
		RedisServerCommands<String, String> commands = sync(redis);
		Assertions.assertEquals(commands.dbsize(), writer.getWrittenItems().size());
	}

	private AbstractKeyspaceNotificationItemReader<?> keyEventReader(RedisServer redis) {
		int queueCapacity = KeyValueItemReader.LiveKeyValueItemReaderBuilder.DEFAULT_QUEUE_CAPACITY;
		if (redis.isCluster()) {
			RedisModulesClusterClient client = redisClusterClient(redis);
			return new RedisClusterKeyspaceNotificationItemReader(client::connectPubSub,
					KeyValueItemReader.LiveKeyValueItemReaderBuilder.DEFAULT_PUBSUB_PATTERNS, queueCapacity);
		}
		RedisClient client = redisClient(redis);
		return new RedisKeyspaceNotificationItemReader(client::connectPubSub,
				KeyValueItemReader.LiveKeyValueItemReaderBuilder.DEFAULT_PUBSUB_PATTERNS, queueCapacity);
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testKeyspaceNotificationReader(RedisServer redis) throws Exception {
		BlockingQueue<String> queue = new LinkedBlockingDeque<>(10000);
		AbstractKeyspaceNotificationItemReader<?> reader = keyspaceNotificationReader(redis, queue);
		reader.open(new ExecutionContext());
		BaseRedisAsyncCommands<String, String> async = async(redis);
		async.setAutoFlushCommands(false);
		List<String> keys = new ArrayList<>();
		List<RedisFuture<?>> futures = new ArrayList<>();
		for (int index = 0; index < 4321; index++) {
			String key = "key" + index;
			futures.add(((RedisStringAsyncCommands<String, String>) async).set(key, "value"));
			if (futures.size() == 50) {
				async.flushCommands();
				LettuceFutures.awaitAll(1, TimeUnit.SECONDS, futures.toArray(new RedisFuture[0]));
				futures.clear();
			}
			keys.add(key);
		}
		async.flushCommands();
		LettuceFutures.awaitAll(1, TimeUnit.SECONDS, futures.toArray(new RedisFuture[0]));
		new Timer(Duration.ofSeconds(1), 1).await(() -> queue.size() == keys.size());
		Assertions.assertEquals(keys.size(), queue.size());
		log.info("Closing reader");
		reader.close();
		async.setAutoFlushCommands(true);
	}

	private AbstractKeyspaceNotificationItemReader<?> keyspaceNotificationReader(RedisServer server,
			BlockingQueue<String> queue) {
		if (server.isCluster()) {
			RedisModulesClusterClient client = redisClusterClient(server);
			return new RedisClusterKeyspaceNotificationItemReader(client::connectPubSub,
					KeyValueItemReader.LiveKeyValueItemReaderBuilder.DEFAULT_PUBSUB_PATTERNS, queue);
		}
		RedisClient client = redisClient(server);
		return new RedisKeyspaceNotificationItemReader(client::connectPubSub,
				KeyValueItemReader.LiveKeyValueItemReaderBuilder.DEFAULT_PUBSUB_PATTERNS, queue);
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testDataStructureReader(RedisServer redis) throws Throwable {
		populateSource("scan-reader-populate", redis);
		KeyValueItemReader<DataStructure> reader = dataStructureReader(redis);
		ListItemWriter<DataStructure> writer = new ListItemWriter<>();
		jobFactory.run(name(redis, "scan-reader"), reader, null, writer);
		RedisServerCommands<String, String> sync = sync(redis);
		Assertions.assertEquals(sync.dbsize(), writer.getWrittenItems().size());
	}

	private void populateSource(String name, RedisServer server) throws Throwable {
		JsonItemReader<Map<String, Object>> reader = Beers.mapReader();
		OperationItemWriter<String, String, Map<String, String>> writer = OperationItemWriter.client(client(server))
				.operation(Hset.<String, Map<String, String>>key(t -> t.get("id")).map(t -> t).build()).build();
		jobFactory.run(name(server, name), reader, new MapFlattener(), writer);
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testMultiThreadedReader(RedisServer server) throws Throwable {
		populateSource("multithreaded-scan-reader-populate", server);
		SynchronizedItemStreamReader<DataStructure> synchronizedReader = new SynchronizedItemStreamReader<>();
		synchronizedReader.setDelegate(dataStructureReader(server));
		synchronizedReader.afterPropertiesSet();
		SynchronizedListItemWriter<DataStructure> writer = new SynchronizedListItemWriter<>();
		String name = "multithreaded-scan-reader";
		int threads = 4;
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setMaxPoolSize(threads);
		taskExecutor.setCorePoolSize(threads);
		taskExecutor.afterPropertiesSet();
		jobFactory.run(name(server, name), jobFactory.step(name, synchronizedReader, writer).taskExecutor(taskExecutor)
				.throttleLimit(threads).build());
		RedisServerCommands<String, String> sync = sync(server);
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
	void testStreamWriter(RedisServer redis) throws Throwable {
		String stream = "stream:0";
		List<Map<String, String>> messages = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			Map<String, String> body = new HashMap<>();
			body.put("field1", "value1");
			body.put("field2", "value2");
			messages.add(body);
		}
		ListItemReader<Map<String, String>> reader = new ListItemReader<>(messages);
		OperationItemWriter<String, String, Map<String, String>> writer = OperationItemWriter.client(client(redis))
				.operation(Xadd.<String, Map<String, String>>key(stream).body(t -> t).build()).build();
		jobFactory.run(name(redis, "stream-writer"), reader, writer);
		RedisStreamCommands<String, String> sync = sync(redis);
		Assertions.assertEquals(messages.size(), sync.xlen(stream));
		List<StreamMessage<String, String>> xrange = sync.xrange(stream, Range.create("-", "+"));
		for (int index = 0; index < xrange.size(); index++) {
			StreamMessage<String, String> message = xrange.get(index);
			Assertions.assertEquals(messages.get(index), message.getBody());
		}
	}

	@Test
	public void testStreamTransactionWriter() throws Throwable {
		String stream = "stream:1";
		List<Map<String, String>> messages = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			Map<String, String> body = new HashMap<>();
			body.put("field1", "value1");
			body.put("field2", "value2");
			messages.add(body);
		}
		ListItemReader<Map<String, String>> reader = new ListItemReader<>(messages);
		OperationItemWriter<String, String, Map<String, String>> writer = OperationItemWriter.client(client(REDIS))
				.operation(Xadd.<String, Map<String, String>>key(stream).body(t -> t).build()).multiExec().build();
		jobFactory.run(name(REDIS, "stream-tx-writer"), reader, writer);
		RedisStreamCommands<String, String> sync = sync(REDIS);
		Assertions.assertEquals(messages.size(), sync.xlen(stream));
		List<StreamMessage<String, String>> xrange = sync.xrange(stream, Range.create("-", "+"));
		for (int index = 0; index < xrange.size(); index++) {
			StreamMessage<String, String> message = xrange.get(index);
			Assertions.assertEquals(messages.get(index), message.getBody());
		}
	}

	@ParameterizedTest
	@MethodSource("servers")
	public void testHashWriter(RedisServer server) throws Throwable {
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
		OperationItemWriter<String, String, Map<String, String>> writer = OperationItemWriter.client(client(server))
				.operation(Hset.key(keyConverter).map(m -> m).build()).build();
		jobFactory.run(name(server, "hash-writer"), reader, writer);
		RedisKeyCommands<String, String> sync = sync(server);
		Assertions.assertEquals(maps.size(), sync.keys("hash:*").size());
		RedisHashCommands<String, String> hashCommands = sync(server);
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

	@ParameterizedTest
	@MethodSource("servers")
	public void testGeoaddWriter(RedisServer redis) throws Throwable {
		AbstractRedisClient client = client(redis);
		ListItemReader<Geo> reader = new ListItemReader<>(Arrays.asList(
				Geo.builder().longitude(-118.476056).latitude(33.985728).member("Venice Breakwater").build(),
				Geo.builder().longitude(-73.667022).latitude(40.582739).member("Long Beach National").build()));
		GeoValueConverter<String, Geo> values = new GeoValueConverter<>(Geo::getMember, Geo::getLongitude,
				Geo::getLatitude);
		OperationItemWriter<String, String, Geo> writer = OperationItemWriter.client(client)
				.operation(Geoadd.<Geo>key("geoset").values(values).build()).build();
		jobFactory.run(name(redis, "geoadd-writer"), reader, writer);
		RedisGeoCommands<String, String> sync = sync(redis);
		Set<String> radius1 = sync.georadius("geoset", -118, 34, 100, GeoArgs.Unit.mi);
		Assertions.assertEquals(1, radius1.size());
		Assertions.assertTrue(radius1.contains("Venice Breakwater"));
	}

	@ParameterizedTest
	@MethodSource("servers")
	public void testHashDelWriter(RedisServer server) throws Throwable {
		List<Map.Entry<String, Map<String, String>>> hashes = new ArrayList<>();
		RedisHashCommands<String, String> commands = sync(server);
		for (int index = 0; index < 100; index++) {
			String key = String.valueOf(index);
			Map<String, String> value = new HashMap<>();
			value.put("field1", "value1");
			commands.hset("hash:" + key, value);
			Map<String, String> body = new HashMap<>();
			body.put("field2", "value2");
			hashes.add(new AbstractMap.SimpleEntry<>(key, index < 50 ? null : body));
		}
		RedisKeyCommands<String, String> sync = sync(server);
		ListItemReader<Map.Entry<String, Map<String, String>>> reader = new ListItemReader<>(hashes);
		KeyMaker<Map.Entry<String, Map<String, String>>> keyConverter = KeyMaker.<Map.Entry<String, Map<String, String>>>builder()
				.prefix("hash").converters(Entry<String, Map<String, String>>::getKey).build();
		OperationItemWriter<String, String, Map.Entry<String, Map<String, String>>> writer = OperationItemWriter
				.client(client(server)).operation(Hset.key(keyConverter).map(Map.Entry::getValue).build()).build();
		jobFactory.run(name(server, "hash-del-writer"), reader, writer);
		Assertions.assertEquals(50, sync.keys("hash:*").size());
		Assertions.assertEquals(2, commands.hgetall("hash:50").size());
	}

	@ParameterizedTest
	@MethodSource("servers")
	public void testSortedSetWriter(RedisServer server) throws Throwable {
		List<ScoredValue<String>> values = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			values.add((ScoredValue<String>) ScoredValue.fromNullable(index % 10, String.valueOf(index)));
		}
		ListItemReader<ScoredValue<String>> reader = new ListItemReader<>(values);
		OperationItemWriter<String, String, ScoredValue<String>> writer = OperationItemWriter.client(client(server))
				.operation(Zadd.<ScoredValue<String>>key("zset").values(v -> new ScoredValue[] { v }).build()).build();
		jobFactory.run(name(server, "sorted-set-writer"), reader, writer);
		RedisServerCommands<String, String> sync = sync(server);
		Assertions.assertEquals(1, sync.dbsize());
		Assertions.assertEquals(values.size(), ((RedisSortedSetCommands<String, String>) sync).zcard("zset"));
		List<String> range = ((RedisSortedSetCommands<String, String>) sync).zrangebyscore("zset",
				Range.from(Range.Boundary.including(0), Range.Boundary.including(5)));
		Assertions.assertEquals(60, range.size());
	}

	@ParameterizedTest
	@MethodSource("servers")
	public void testDataStructureWriter(RedisServer redis) throws Throwable {
		List<DataStructure> list = new ArrayList<>();
		long count = 100;
		for (int index = 0; index < count; index++) {
			DataStructure keyValue = new DataStructure();
			keyValue.setKey("hash:" + index);
			keyValue.setType(DataStructure.HASH);
			Map<String, String> map = new HashMap<>();
			map.put("field1", "value1");
			map.put("field2", "value2");
			keyValue.setValue(map);
			list.add(keyValue);
		}
		ListItemReader<DataStructure> reader = new ListItemReader<>(list);
		DataStructureItemWriter writer = dataStructureWriter(redis);
		jobFactory.run(name(redis, "value-writer"), reader, writer);
		RedisKeyCommands<String, String> sync = sync(redis);
		List<String> keys = sync.keys("hash:*");
		Assertions.assertEquals(count, keys.size());
	}

	@ParameterizedTest
	@MethodSource("servers")
	public void testLiveReader(RedisServer redis) throws Throwable {
		LiveKeyValueItemReader<KeyValue<byte[]>> reader = liveKeyDumpReader(redis).build();
		ListItemWriter<KeyValue<byte[]>> writer = new ListItemWriter<>();
		JobFactory.JobExecutionWrapper execution = jobFactory.runFlushing(name(redis, "live-reader"), reader, writer);
		dataGenerator(redis).end(123).maxExpire(Duration.ofMillis(0))
				.dataTypes(DataStructure.STRING, DataStructure.HASH).build().call();
		execution.awaitTermination();
		RedisServerCommands<String, String> sync = sync(redis);
		Assertions.assertEquals(sync.dbsize(), writer.getWrittenItems().size());
	}

	@ParameterizedTest
	@MethodSource("servers")
	public void testKeyValueItemReaderFaultTolerance(RedisServer redis) throws Throwable {
		dataGenerator(redis).end(1000).dataTypes(DataStructure.STRING).build().call();
		List<String> keys = IntStream.range(0, 100).boxed().map(DataGenerator::stringKey).collect(Collectors.toList());
		DelegatingPollableItemReader<String> keyReader = DelegatingPollableItemReader.<String>builder()
				.delegate(new ListItemReader<>(keys)).exceptionSupplier(TimeoutException::new).interval(2).build();
		DataStructureValueReader valueReader = dataStructureValueReader(redis);
		KeyValueItemReader<DataStructure> reader = new KeyValueItemReader<>(keyReader, valueReader, 1, 1, 1000,
				Duration.ofMillis(100), new AlwaysSkipItemSkipPolicy());
		ListItemWriter<DataStructure> writer = new ListItemWriter<>();
		jobFactory.run(name(redis, "reader-ft"), reader, writer);
		Assertions.assertEquals(50, writer.getWrittenItems().size());
	}

}
