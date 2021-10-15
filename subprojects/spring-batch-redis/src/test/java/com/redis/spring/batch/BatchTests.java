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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
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
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.spring.batch.support.AbstractKeyspaceNotificationItemReader;
import com.redis.spring.batch.support.ClusterKeyspaceNotificationItemReader;
import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.DataStructureValueReader;
import com.redis.spring.batch.support.JobFactory;
import com.redis.spring.batch.support.KeyValue;
import com.redis.spring.batch.support.KeyspaceNotificationItemReader;
import com.redis.spring.batch.support.LiveRedisItemReader;
import com.redis.spring.batch.support.LiveRedisItemReaderBuilder;
import com.redis.spring.batch.support.Timer;
import com.redis.spring.batch.support.convert.GeoValueConverter;
import com.redis.spring.batch.support.convert.KeyMaker;
import com.redis.spring.batch.support.convert.MapFlattener;
import com.redis.spring.batch.support.operation.Geoadd;
import com.redis.spring.batch.support.operation.Hset;
import com.redis.spring.batch.support.operation.Xadd;
import com.redis.spring.batch.support.operation.Zadd;
import com.redis.testcontainers.RedisServer;

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
		int queueCapacity = LiveRedisItemReaderBuilder.DEFAULT_QUEUE_CAPACITY;
		if (redis.isCluster()) {
			RedisModulesClusterClient client = (RedisModulesClusterClient) client(redis);
			return new ClusterKeyspaceNotificationItemReader(client::connectPubSub,
					LiveRedisItemReaderBuilder.DEFAULT_PUBSUB_PATTERNS, queueCapacity);
		}
		RedisClient client = (RedisClient) client(redis);
		return new KeyspaceNotificationItemReader(client::connectPubSub,
				LiveRedisItemReaderBuilder.DEFAULT_PUBSUB_PATTERNS, queueCapacity);
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
			RedisModulesClusterClient client = (RedisModulesClusterClient) client(server);
			return new ClusterKeyspaceNotificationItemReader(client::connectPubSub,
					LiveRedisItemReaderBuilder.DEFAULT_PUBSUB_PATTERNS, queue);
		}
		RedisClient client = (RedisClient) client(server);
		return new KeyspaceNotificationItemReader(client::connectPubSub,
				LiveRedisItemReaderBuilder.DEFAULT_PUBSUB_PATTERNS, queue);
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testDataStructureReader(RedisServer redis) throws Throwable {
		populateSource("scan-reader-populate", redis);
		RedisItemReader<String, DataStructure<String>> reader = dataStructureReader(redis);
		ListItemWriter<DataStructure<String>> writer = new ListItemWriter<>();
		jobFactory.run(name(redis, "scan-reader"), reader, null, writer);
		RedisServerCommands<String, String> sync = sync(redis);
		Assertions.assertEquals(sync.dbsize(), writer.getWrittenItems().size());
	}

	private void populateSource(String name, RedisServer server) throws Throwable {
		JsonItemReader<Map<String, Object>> reader = Beers.mapReader();
		RedisItemWriter<String, String, Map<String, String>> writer = RedisItemWriter
				.operation(Hset.<String, Map<String, String>>key(t -> t.get("id")).map(t -> t).build())
				.client(client(server)).build();
		jobFactory.run(name(server, name), reader, new MapFlattener(), writer);
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testMultiThreadedReader(RedisServer server) throws Throwable {
		populateSource("multithreaded-scan-reader-populate", server);
		SynchronizedItemStreamReader<DataStructure<String>> synchronizedReader = new SynchronizedItemStreamReader<>();
		synchronizedReader.setDelegate(dataStructureReader(server));
		synchronizedReader.afterPropertiesSet();
		SynchronizedListItemWriter<DataStructure<String>> writer = new SynchronizedListItemWriter<>();
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
		RedisItemWriter<String, String, Map<String, String>> writer = RedisItemWriter
				.operation(Xadd.<String, Map<String, String>>key(stream).body(t -> t).build()).client(client(redis))
				.build();
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
		RedisItemWriter<String, String, Map<String, String>> writer = RedisItemWriter
				.operation(Xadd.<String, Map<String, String>>key(stream).body(t -> t).build()).client(client(REDIS))
				.multiExec().build();
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
		RedisItemWriter<String, String, Map<String, String>> writer = RedisItemWriter
				.operation(Hset.key(keyConverter).map(m -> m).build()).client(client(server)).build();
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
		ListItemReader<Geo> reader = new ListItemReader<>(Arrays.asList(
				Geo.builder().longitude(-118.476056).latitude(33.985728).member("Venice Breakwater").build(),
				Geo.builder().longitude(-73.667022).latitude(40.582739).member("Long Beach National").build()));
		GeoValueConverter<String, Geo> values = new GeoValueConverter<>(Geo::getMember, Geo::getLongitude,
				Geo::getLatitude);
		RedisItemWriter<String, String, Geo> writer = RedisItemWriter
				.operation(Geoadd.<Geo>key("geoset").values(values).build()).client(client(redis)).build();
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
		RedisItemWriter<String, String, Map.Entry<String, Map<String, String>>> writer = RedisItemWriter
				.operation(Hset.key(keyConverter).map(Map.Entry::getValue).build()).client(client(server)).build();
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
		RedisItemWriter<String, String, ScoredValue<String>> writer = RedisItemWriter
				.operation(Zadd.<ScoredValue<String>>key("zset").values(v -> new ScoredValue[] { v }).build())
				.client(client(server)).build();
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
		List<DataStructure<String>> list = new ArrayList<>();
		long count = 100;
		for (int index = 0; index < count; index++) {
			DataStructure<String> keyValue = new DataStructure<>();
			keyValue.setKey("hash:" + index);
			keyValue.setType(DataStructure.HASH);
			Map<String, String> map = new HashMap<>();
			map.put("field1", "value1");
			map.put("field2", "value2");
			keyValue.setValue(map);
			list.add(keyValue);
		}
		ListItemReader<DataStructure<String>> reader = new ListItemReader<>(list);
		RedisItemWriter<String, String, DataStructure<String>> writer = dataStructureWriter(redis);
		jobFactory.run(name(redis, "value-writer"), reader, writer);
		RedisKeyCommands<String, String> sync = sync(redis);
		List<String> keys = sync.keys("hash:*");
		Assertions.assertEquals(count, keys.size());
	}

	@ParameterizedTest
	@MethodSource("servers")
	public void testLiveReader(RedisServer redis) throws Throwable {
		LiveRedisItemReader<String, KeyValue<String, byte[]>> reader = liveKeyDumpReader(redis);
		ListItemWriter<KeyValue<String, byte[]>> writer = new ListItemWriter<>();
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
		DataStructureValueReader<String, String> valueReader = dataStructureValueReader(redis);
		RedisItemReader<String, DataStructure<String>> reader = new RedisItemReader<>(keyReader, valueReader, 1, 1,
				new LinkedBlockingQueue<>(1000), Duration.ofMillis(100), new AlwaysSkipItemSkipPolicy());
		ListItemWriter<DataStructure<String>> writer = new ListItemWriter<>();
		jobFactory.run(name(redis, "reader-ft"), reader, writer);
		Assertions.assertEquals(50, writer.getWrittenItems().size());
	}

}
