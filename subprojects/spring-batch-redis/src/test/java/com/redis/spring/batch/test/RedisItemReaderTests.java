package com.redis.spring.batch.test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.Range;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.builder.StreamItemReaderBuilder;
import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.DataStructure.Type;
import com.redis.spring.batch.support.DataStructureValueReader;
import com.redis.spring.batch.support.KeyValue;
import com.redis.spring.batch.support.LiveKeyItemReader;
import com.redis.spring.batch.support.LiveRedisItemReader;
import com.redis.spring.batch.support.PollableItemReader;
import com.redis.spring.batch.support.StreamItemReader;
import com.redis.spring.batch.support.StreamItemReader.AckPolicy;
import com.redis.spring.batch.support.generator.Generator.GeneratorBuilder;
import com.redis.testcontainers.junit.jupiter.RedisTestContext;
import com.redis.testcontainers.junit.jupiter.RedisTestContextsSource;

import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.models.stream.PendingMessages;

class RedisItemReaderTests extends AbstractRedisTestBase {

	private static final String STREAM = "stream:1";

	@ParameterizedTest
	@RedisTestContextsSource
	void testFlushingStep(RedisTestContext context) throws Exception {
		String name = "flushing-step";
		PollableItemReader<String> reader = keyspaceNotificationReader(context);
		ListItemWriter<String> writer = new ListItemWriter<>();
		JobExecution execution = runFlushing(context, name, reader, null, writer);
		execute(dataGenerator(context, name).end(3).dataType(Type.STRING).dataType(Type.HASH));
		awaitTermination(execution);
		Assertions.assertEquals(context.sync().dbsize(), writer.getWrittenItems().size());
	}

	private LiveKeyItemReader<String> keyspaceNotificationReader(RedisTestContext context) {
		return reader(context).dataStructure().live().keyReader();
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testKeyspaceNotificationReader(RedisTestContext context) throws Exception {
		String name = "keyspace-notification-reader";
		LiveKeyItemReader<String> keyReader = keyspaceNotificationReader(context);
		keyReader.open(new ExecutionContext());
		execute(dataGenerator(context, name).dataType(Type.HASH).dataType(Type.STRING).dataType(Type.LIST)
				.dataType(Type.SET).dataType(Type.ZSET).end(2));
		ListItemWriter<String> writer = new ListItemWriter<>();
		run(context, name, keyReader, writer);
		Assertions.assertEquals(context.sync().dbsize(), writer.getWrittenItems().size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testDataStructureReader(RedisTestContext context) throws Exception {
		String name = "ds-reader";
		dataGenerator(context, name).build().call();
		RedisItemReader<String, DataStructure<String>> reader = dataStructureReader(context, name);
		ListItemWriter<DataStructure<String>> writer = new ListItemWriter<>();
		run(context, name, reader, writer);
		Assertions.assertEquals(context.sync().dbsize(), writer.getWrittenItems().size());
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
	@RedisTestContextsSource
	void testMultiThreadedReader(RedisTestContext context) throws Exception {
		String name = "multi-threaded-reader";
		dataGenerator(context, name).build().call();
		RedisItemReader<String, DataStructure<String>> reader = dataStructureReader(context, name);
		SynchronizedItemStreamReader<DataStructure<String>> synchronizedReader = new SynchronizedItemStreamReader<>();
		synchronizedReader.setDelegate(reader);
		synchronizedReader.afterPropertiesSet();
		SynchronizedListItemWriter<DataStructure<String>> writer = new SynchronizedListItemWriter<>();
		int threads = 4;
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setMaxPoolSize(threads);
		taskExecutor.setCorePoolSize(threads);
		taskExecutor.afterPropertiesSet();
		launch(job(context, name, step(context, name, synchronizedReader, null, writer).taskExecutor(taskExecutor)
				.throttleLimit(threads).build()).build());
		Assertions.assertEquals(context.sync().dbsize(), writer.getWrittenItems().size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testLiveReader(RedisTestContext context) throws Exception {
		String name = "live-reader";
		LiveRedisItemReader<String, KeyValue<String, byte[]>> reader = liveKeyDumpReader(context, name, 10000);
		ListItemWriter<KeyValue<String, byte[]>> writer = new ListItemWriter<>();
		JobExecution execution = runFlushing(context, name, reader, null, writer);
		execute(dataGenerator(context, name).end(123).dataType(Type.HASH).dataType(Type.STRING));
		awaitTermination(execution);
		Assertions.assertEquals(context.sync().dbsize(), writer.getWrittenItems().size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testKeyValueItemReaderFaultTolerance(RedisTestContext context) throws Exception {
		String name = "reader-ft";
		execute(dataGenerator(context, name).dataType(Type.STRING));
		List<String> keys = IntStream.range(0, 100).boxed().map(i -> "string:" + i).collect(Collectors.toList());
		DelegatingPollableItemReader<String> keyReader = new DelegatingPollableItemReader<>(new ListItemReader<>(keys));
		DataStructureValueReader<String, String> valueReader = dataStructureValueReader(context);
		RedisItemReader<String, DataStructure<String>> reader = new RedisItemReader<>(jobRepository, transactionManager,
				keyReader, valueReader);
		reader.setChunkSize(1);
		reader.setQueueCapacity(1000);
		reader.setSkipPolicy(new AlwaysSkipItemSkipPolicy());
		reader.setName(name(context, name + "-reader"));
		ListItemWriter<DataStructure<String>> writer = new ListItemWriter<>();
		run(context, name, reader, writer);
		Assertions.assertEquals(50, writer.getWrittenItems().size());
	}

	private static final long COUNT = 100;

	private void assertMessageBody(List<? extends StreamMessage<String, String>> items) {
		for (StreamMessage<String, String> message : items) {
			Assertions.assertTrue(message.getBody().containsKey("field1"));
			Assertions.assertTrue(message.getBody().containsKey("field2"));
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testStreamReader(RedisTestContext redis) throws Exception {
		String name = "stream-reader";
		execute(streamDataGenerator(redis, name));
		StreamItemReader<String, String> reader = streamReader(redis).build();
		reader.open(new ExecutionContext());
		List<StreamMessage<String, String>> messages = reader.readMessages();
		Assertions.assertEquals(StreamItemReader.DEFAULT_COUNT, messages.size());
		assertMessageBody(messages);
	}

	private GeneratorBuilder streamDataGenerator(RedisTestContext redis, String name) {
		return dataGenerator(redis, name).dataType(Type.STREAM).end(1).collectionCardinality(Range.is(COUNT));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testStreamReaderJob(RedisTestContext redis) throws Exception {
		String name = "stream-reader-job";
		execute(streamDataGenerator(redis, name));
		Assertions.assertEquals(COUNT, redis.sync().xlen(STREAM));
		StreamItemReader<String, String> reader = streamReader(redis).build();
		ListItemWriter<StreamMessage<String, String>> writer = new ListItemWriter<>();
		awaitTermination(runFlushing(redis, name, reader, null, writer), reader);
		Assertions.assertEquals(COUNT, writer.getWrittenItems().size());
		assertMessageBody(writer.getWrittenItems());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testMultipleStreamReaders(RedisTestContext redis) throws Exception {
		String name = "multiple-stream-readers";
		String consumerGroup = "consumerGroup";
		execute(streamDataGenerator(redis, name));
		StreamItemReader<String, String> reader1 = streamReader(redis).consumerGroup(consumerGroup)
				.consumer("consumer1").ackPolicy(AckPolicy.MANUAL).build();
		StreamItemReader<String, String> reader2 = streamReader(redis).consumerGroup(consumerGroup)
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
		RedisStreamCommands<String, String> sync = redis.sync();
		PendingMessages pendingMessages = sync.xpending(STREAM, consumerGroup);
		Assertions.assertEquals(COUNT, pendingMessages.getCount());
		reader1.open(new ExecutionContext());
		reader1.ack(writer1.getWrittenItems());
		reader2.open(new ExecutionContext());
		reader2.ack(writer2.getWrittenItems());
		pendingMessages = sync.xpending(STREAM, consumerGroup);
		Assertions.assertEquals(0, pendingMessages.getCount());
	}

	private StreamItemReaderBuilder streamReader(RedisTestContext server) {
		return reader(server).stream(STREAM);
	}

}
