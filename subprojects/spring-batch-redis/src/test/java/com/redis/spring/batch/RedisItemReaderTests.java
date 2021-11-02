package com.redis.spring.batch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.Range;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.redis.spring.batch.builder.RedisStreamItemReaderBuilder;
import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.DataStructure.Type;
import com.redis.spring.batch.support.DataStructureValueReader;
import com.redis.spring.batch.support.KeyValue;
import com.redis.spring.batch.support.LiveKeyItemReader;
import com.redis.spring.batch.support.LiveRedisItemReader;
import com.redis.spring.batch.support.PollableItemReader;
import com.redis.spring.batch.support.RedisStreamItemReader;
import com.redis.spring.batch.support.RedisStreamItemReader.AckPolicy;
import com.redis.spring.batch.support.convert.MapFlattener;
import com.redis.spring.batch.support.generator.Generator;
import com.redis.spring.batch.support.generator.Generator.GeneratorBuilder;
import com.redis.spring.batch.support.operation.Hset;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.models.stream.PendingMessages;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.search.Search;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

class RedisItemReaderTests extends AbstractRedisTestBase {

	@ParameterizedTest
	@MethodSource("servers")
	void testFlushingStep(RedisServer redis) throws Exception {
		String name = "flushing-step";
		PollableItemReader<String> reader = keyspaceNotificationReader(redis);
		ListItemWriter<String> writer = new ListItemWriter<>();
		JobExecution execution = runFlushing(redis, name, reader, null, writer);
		execute(dataGenerator(redis, name).end(3).dataType(Type.STRING).dataType(Type.HASH));
		awaitTermination(execution);
		Assertions.assertEquals(dbsize(redis), writer.getWrittenItems().size());
	}

	private LiveKeyItemReader<String> keyspaceNotificationReader(RedisServer redis) {
		return LiveRedisItemReader.dataStructure(jobRepository, transactionManager, clients.get(redis)).live()
				.keyReader();
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testKeyspaceNotificationReader(RedisServer redis) throws Exception {
		String name = "keyspace-notification-reader";
		LiveKeyItemReader<String> keyReader = keyspaceNotificationReader(redis);
		keyReader.open(new ExecutionContext());
		execute(dataGenerator(redis, name).clearDataTypes().dataType(Type.HASH).dataType(Type.STRING)
				.dataType(Type.LIST).dataType(Type.SET).dataType(Type.ZSET).end(2));
		ListItemWriter<String> writer = new ListItemWriter<>();
		run(redis, name, keyReader, writer);
		Assertions.assertEquals(dbsize(redis), writer.getWrittenItems().size());
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testDataStructureReader(RedisServer redis) throws Exception {
		String name = "ds-reader";
		populateSource(redis, name);
		RedisItemReader<String, DataStructure<String>> reader = dataStructureReader(redis, name);
		ListItemWriter<DataStructure<String>> writer = new ListItemWriter<>();
		run(redis, name, reader, writer);
		Assertions.assertEquals(dbsize(redis), writer.getWrittenItems().size());
	}

	private void populateSource(RedisServer server, String name) throws Exception {
		JsonItemReader<Map<String, Object>> reader = Beers.mapReader();
		RedisItemWriter<String, String, Map<String, String>> writer = redisItemWriter(server,
				Hset.<Map<String, String>>key(t -> t.get("id")).map(t -> t).build()).build();
		run(server, name + "-populate", reader, new MapFlattener(), writer);
	}
	
	private static class SynchronizedListItemWriter<T> implements ItemWriter<T> {

		private List<T> writtenItems = new ArrayList<>();

		@Override
		public synchronized void write(List<? extends T> items) throws Exception {
			writtenItems.addAll(items);
		}

		public List<? extends T> getWrittenItems() {
			return this.writtenItems;
		}
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testMultiThreadedReader(RedisServer redis) throws Exception {
		String name = "multi-threaded-reader";
		populateSource(redis, name);
		RedisItemReader<String, DataStructure<String>> reader = dataStructureReader(redis, name);
		SynchronizedItemStreamReader<DataStructure<String>> synchronizedReader = new SynchronizedItemStreamReader<>();
		synchronizedReader.setDelegate(reader);
		synchronizedReader.afterPropertiesSet();
		SynchronizedListItemWriter<DataStructure<String>> writer = new SynchronizedListItemWriter<>();
		int threads = 4;
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setMaxPoolSize(threads);
		taskExecutor.setCorePoolSize(threads);
		taskExecutor.afterPropertiesSet();
		launch(job(redis, name, step(redis, name, synchronizedReader, null, writer).taskExecutor(taskExecutor)
				.throttleLimit(threads).build()).build());
		Assertions.assertEquals(dbsize(redis), writer.getWrittenItems().size());
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testLiveReader(RedisServer redis) throws Exception {
		String name = "live-reader";
		LiveRedisItemReader<String, KeyValue<String, byte[]>> reader = liveKeyDumpReader(redis, name, 10000);
		ListItemWriter<KeyValue<String, byte[]>> writer = new ListItemWriter<>();
		JobExecution execution = runFlushing(redis, name, reader, null, writer);
		execute(dataGenerator(redis, name).end(123).dataType(Type.HASH).dataType(Type.STRING));
		awaitTermination(execution);
		Assertions.assertEquals(dbsize(redis), writer.getWrittenItems().size());
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testKeyValueItemReaderFaultTolerance(RedisServer redis) throws Exception {
		String name = "reader-ft";
		execute(dataGenerator(redis, name).dataType(Type.STRING));
		List<String> keys = IntStream.range(0, 100).boxed().map(i -> "string:" + i).collect(Collectors.toList());
		DelegatingPollableItemReader<String> keyReader = DelegatingPollableItemReader.<String>builder()
				.delegate(new ListItemReader<>(keys)).exceptionSupplier(TimeoutException::new).interval(2).build();
		DataStructureValueReader<String, String> valueReader = dataStructureValueReader(redis);
		RedisItemReader<String, DataStructure<String>> reader = new RedisItemReader<>(jobRepository, transactionManager,
				keyReader, valueReader);
		reader.setChunkSize(1);
		reader.setQueueCapacity(1000);
		reader.setSkipPolicy(new AlwaysSkipItemSkipPolicy());
		reader.setName(name(redis, name + "-reader"));
		ListItemWriter<DataStructure<String>> writer = new ListItemWriter<>();
		run(redis, name, reader, writer);
		Assertions.assertEquals(50, writer.getWrittenItems().size());
	}

	@Test
	void testMetrics() throws Exception {
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
		Generator.builder("metrics", jobRepository, transactionManager, clients.get(REDIS)).build().call();
		RedisItemReader<String, DataStructure<String>> reader = RedisItemReader
				.dataStructure(jobRepository, transactionManager, clients.get(REDIS)).build();
		reader.open(new ExecutionContext());
		Search search = registry.find("spring.batch.redis.reader.queue.size");
		Assertions.assertNotNull(search.gauge());
		reader.close();
	}

	private static final long COUNT = 100;

	private void assertMessageBody(List<? extends StreamMessage<String, String>> items) {
		for (StreamMessage<String, String> message : items) {
			Assertions.assertTrue(message.getBody().containsKey("field1"));
			Assertions.assertTrue(message.getBody().containsKey("field2"));
		}
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testStreamReader(RedisServer redis) throws Exception {
		String name = "stream-reader";
		execute(streamDataGenerator(redis, name));
		RedisStreamItemReader<String, String> reader = streamReader(redis, offset()).build();
		reader.open(new ExecutionContext());
		List<StreamMessage<String, String>> messages = reader.readMessages();
		Assertions.assertEquals(RedisStreamItemReaderBuilder.DEFAULT_COUNT, messages.size());
		assertMessageBody(messages);
	}

	private GeneratorBuilder streamDataGenerator(RedisServer redis, String name) {
		return dataGenerator(redis, name).dataType(Type.STREAM).end(1).collectionCardinality(Range.is(COUNT));
	}

	private StreamOffset<String> offset() {
		return StreamOffset.from("stream:1", "0-0");
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testStreamReaderJob(RedisServer redis) throws Exception {
		String name = "stream-reader-job";
		execute(streamDataGenerator(redis, name));
		RedisStreamItemReader<String, String> reader = streamReader(redis, offset()).build();
		ListItemWriter<StreamMessage<String, String>> writer = new ListItemWriter<>();
		awaitTermination(runFlushing(redis, name, reader, null, writer));
		Assertions.assertEquals(COUNT, writer.getWrittenItems().size());
		assertMessageBody(writer.getWrittenItems());
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testMultipleStreamReaders(RedisServer redis) throws Exception {
		String name = "multiple-stream-readers";
		String consumerGroup = "consumerGroup";
		execute(streamDataGenerator(redis, name));
		RedisStreamItemReader<String, String> reader1 = streamReader(redis, offset()).consumerGroup(consumerGroup)
				.consumer("consumer1").ackPolicy(AckPolicy.MANUAL).build();
		RedisStreamItemReader<String, String> reader2 = streamReader(redis, offset()).consumerGroup(consumerGroup)
				.consumer("consumer2").ackPolicy(AckPolicy.MANUAL).build();
		ListItemWriter<StreamMessage<String, String>> writer1 = new ListItemWriter<>();
		JobExecution execution1 = runFlushing(redis, "stream-reader-1", reader1, null, writer1);
		ListItemWriter<StreamMessage<String, String>> writer2 = new ListItemWriter<>();
		JobExecution execution2 = runFlushing(redis, "stream-reader-2", reader2, null, writer2);
		awaitTermination(execution1);
		awaitTermination(execution2);
		Assertions.assertEquals(COUNT, writer1.getWrittenItems().size() + writer2.getWrittenItems().size());
		assertMessageBody(writer1.getWrittenItems());
		assertMessageBody(writer2.getWrittenItems());
		RedisStreamCommands<String, String> sync = sync(redis);
		PendingMessages pendingMessages = sync.xpending(offset().getName(), consumerGroup);
		Assertions.assertEquals(COUNT, pendingMessages.getCount());
		reader1.open(new ExecutionContext());
		reader1.ack(writer1.getWrittenItems());
		reader2.open(new ExecutionContext());
		reader2.ack(writer2.getWrittenItems());
		pendingMessages = sync.xpending(offset().getName(), consumerGroup);
		Assertions.assertEquals(0, pendingMessages.getCount());
	}

	private RedisStreamItemReaderBuilder streamReader(RedisServer server, XReadArgs.StreamOffset<String> offset) {
		return RedisItemReader.stream(offset).client(clients.get(server));
	}

}
