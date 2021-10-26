package com.redis.spring.batch;

import java.util.List;

import org.apache.commons.lang3.Range;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.ListItemWriter;

import com.redis.spring.batch.support.RedisStreamItemReader;
import com.redis.spring.batch.support.RedisStreamItemReader.AckPolicy;
import com.redis.spring.batch.support.RedisStreamItemReaderBuilder;
import com.redis.spring.batch.support.generator.CollectionGeneratorItemReader;
import com.redis.spring.batch.support.generator.Generator.DataType;
import com.redis.spring.batch.support.generator.Generator.GeneratorOptions;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.models.stream.PendingMessages;

public class StreamItemReaderTests extends AbstractRedisTestBase {

	private static final int COUNT = 100;

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
		dataGenerator(redis, name).options(options()).build().call();
		RedisStreamItemReader<String, String> reader = streamReader(redis, offset()).build();
		reader.open(new ExecutionContext());
		List<StreamMessage<String, String>> messages = reader.readMessages();
		Assertions.assertEquals(RedisStreamItemReaderBuilder.DEFAULT_COUNT, messages.size());
		assertMessageBody(messages);
	}

	private StreamOffset<String> offset() {
		return StreamOffset.from(DataType.STREAM + ":1", "0-0");
	}

	private GeneratorOptions options() {
		return GeneratorOptions.builder().dataType(DataType.STREAM)
				.streamOptions(CollectionGeneratorItemReader.CollectionOptions.builder().start(0).end(1)
						.cardinality(Range.is(COUNT)).build())
				.build();
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testStreamReaderJob(RedisServer redis) throws Exception {
		String name = "stream-reader-job";
		dataGenerator(redis, name).options(options()).build().call();
		RedisStreamItemReader<String, String> reader = streamReader(redis, offset()).build();
		ListItemWriter<StreamMessage<String, String>> writer = new ListItemWriter<>();
		JobExecution execution = runFlushing(redis, name, reader, null, writer);
		Awaitility.await().until(() -> !execution.isRunning());
		Assertions.assertEquals(COUNT, writer.getWrittenItems().size());
		assertMessageBody(writer.getWrittenItems());
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testMultipleStreamReaders(RedisServer redis) throws Exception {
		String name = "multiple-stream-readers";
		String consumerGroup = "consumerGroup";
		dataGenerator(redis, name).options(options()).build().call();
		RedisStreamItemReader<String, String> reader1 = streamReader(redis, offset()).consumerGroup(consumerGroup)
				.consumer("consumer1").ackPolicy(AckPolicy.MANUAL).build();
		RedisStreamItemReader<String, String> reader2 = streamReader(redis, offset()).consumerGroup(consumerGroup)
				.consumer("consumer2").ackPolicy(AckPolicy.MANUAL).build();
		ListItemWriter<StreamMessage<String, String>> writer1 = new ListItemWriter<>();
		JobExecution execution1 = runFlushing(redis, "stream-reader-1", reader1, null, writer1);
		ListItemWriter<StreamMessage<String, String>> writer2 = new ListItemWriter<>();
		JobExecution execution2 = runFlushing(redis, "stream-reader-2", reader2, null, writer2);
		Awaitility.await().until(() -> !execution1.isRunning());
		Awaitility.await().until(() -> !execution2.isRunning());
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
		return RedisItemReader.stream(offset).client(client(server));
	}
}
