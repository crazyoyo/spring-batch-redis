package com.redis.spring.batch;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.ListItemWriter;

import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.JobFactory;
import com.redis.spring.batch.support.RedisStreamItemReader;
import com.redis.spring.batch.support.StreamItemReaderBuilder;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.models.stream.PendingMessages;

public class StreamItemReaderTests extends AbstractRedisTestBase {

	private void assertMessageBody(List<? extends StreamMessage<String, String>> items) {
		for (StreamMessage<String, String> message : items) {
			Assertions.assertTrue(message.getBody().containsKey("field1"));
			Assertions.assertTrue(message.getBody().containsKey("field2"));
		}
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testStreamReader(RedisServer server) throws Exception {
		dataGenerator(server).dataTypes(DataStructure.STREAM).end(100).build().call();
		RedisStreamItemReader reader = streamReader(server, XReadArgs.StreamOffset.from("stream:0", "0-0")).build();
		reader.open(new ExecutionContext());
		List<StreamMessage<String, String>> messages = reader.readMessages();
		Assertions.assertEquals(10, messages.size());
		assertMessageBody(messages);
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testStreamReaderJob(RedisServer redis) throws Throwable {
		dataGenerator(redis).dataTypes(DataStructure.STREAM).end(100).build().call();
		RedisStreamItemReader reader = streamReader(redis, XReadArgs.StreamOffset.from("stream:0", "0-0")).build();
		ListItemWriter<StreamMessage<String, String>> writer = new ListItemWriter<>();
		jobFactory.runFlushing(name(redis, "stream-reader"), reader, writer).awaitTermination();
		Assertions.assertEquals(10, writer.getWrittenItems().size());
		List<? extends StreamMessage<String, String>> items = writer.getWrittenItems();
		assertMessageBody(items);
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testMultipleStreamReaders(RedisServer redis) throws Throwable {
		String stream = "stream:0";
		String consumerGroup = "consumerGroup";
		dataGenerator(redis).dataTypes(DataStructure.STREAM).end(100).build().call();
		RedisStreamItemReader reader1 = streamReader(redis, XReadArgs.StreamOffset.from(stream, "0-0"))
				.consumerGroup(consumerGroup).consumer("consumer1").ackPolicy(RedisStreamItemReader.AckPolicy.MANUAL)
				.build();
		RedisStreamItemReader reader2 = streamReader(redis, XReadArgs.StreamOffset.from(stream, "0-0"))
				.consumerGroup(consumerGroup).consumer("consumer2").ackPolicy(RedisStreamItemReader.AckPolicy.MANUAL)
				.build();
		ListItemWriter<StreamMessage<String, String>> writer1 = new ListItemWriter<>();
		JobFactory.JobExecutionWrapper execution1 = jobFactory.runFlushing(name(redis, "stream-reader-1"), reader1,
				writer1);
		ListItemWriter<StreamMessage<String, String>> writer2 = new ListItemWriter<>();
		JobFactory.JobExecutionWrapper execution2 = jobFactory.runFlushing(name(redis, "stream-reader-2"), reader2,
				writer2);
		execution1.awaitTermination();
		execution2.awaitTermination();
		Assertions.assertEquals(10, writer1.getWrittenItems().size() + writer2.getWrittenItems().size());
		assertMessageBody(writer1.getWrittenItems());
		assertMessageBody(writer2.getWrittenItems());
		RedisStreamCommands<String, String> sync = sync(redis);
		PendingMessages pendingMessages = sync.xpending(stream, consumerGroup);
		Assertions.assertEquals(10, pendingMessages.getCount());
		reader1.open(new ExecutionContext());
		reader1.ack(writer1.getWrittenItems());
		reader2.open(new ExecutionContext());
		reader2.ack(writer2.getWrittenItems());
		pendingMessages = sync.xpending(stream, consumerGroup);
		Assertions.assertEquals(0, pendingMessages.getCount());
	}

	private StreamItemReaderBuilder streamReader(RedisServer server, XReadArgs.StreamOffset<String> offset) {
		return RedisItemReader.scan(offset).client(client(server));
	}
}
