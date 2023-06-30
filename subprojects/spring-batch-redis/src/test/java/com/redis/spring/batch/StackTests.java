package com.redis.spring.batch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.ListItemReader;

import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.spring.batch.RedisItemWriter.WriterBuilder;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.reader.GeneratorItemReader;
import com.redis.spring.batch.reader.StreamAckPolicy;
import com.redis.spring.batch.reader.StreamItemReader;
import com.redis.spring.batch.writer.operation.Xadd;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;

import io.lettuce.core.Consumer;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.Range;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.sync.RedisStreamCommands;

class StackTests extends AbstractTests {

	private static final RedisStackContainer SOURCE = RedisContainerFactory.stack();
	private static final RedisStackContainer TARGET = RedisContainerFactory.stack();

	@Override
	protected RedisServer getSourceServer() {
		return SOURCE;
	}

	@Override
	protected RedisServer getTargetServer() {
		return TARGET;
	}

	@Test
	void dataStructures(TestInfo testInfo) throws Exception {
		GeneratorItemReader gen = new GeneratorItemReader();
		gen.setMaxItemCount(100);
		generate(testInfo, gen);
		RedisItemReader<String, String, DataStructure<String>> reader = dataStructureSourceReader();
		run(testInfo, reader, dataStructureTargetWriter());
		Assertions.assertTrue(compare(testInfo));
	}

	@Test
	void dumpAndRestore(TestInfo testInfo) throws Exception {
		GeneratorItemReader gen = new GeneratorItemReader();
		gen.setMaxItemCount(100);
		generate(testInfo, gen);
		RedisItemReader<byte[], byte[], KeyDump<byte[]>> reader = keyDumpSourceReader();
		run(testInfo, reader, keyDumpWriter(targetClient));
		Assertions.assertTrue(compare(testInfo));
	}

	private static final String DEFAULT_CONSUMER_GROUP = "consumerGroup";

	@Test
	void readMultipleStreams(TestInfo testInfo) throws Exception {
		generateStreams(testInfo(testInfo, "streams"));
		final List<String> keys = ScanIterator
				.scan(sourceConnection.sync(), KeyScanArgs.Builder.type(DataStructure.STREAM)).stream()
				.collect(Collectors.toList());
		for (String key : keys) {
			StreamItemReader<String, String> reader1 = streamReader(key);
			reader1.setConsumer(Consumer.from(DEFAULT_CONSUMER_GROUP, "consumer1"));
			reader1.setAckPolicy(StreamAckPolicy.MANUAL);
			StreamItemReader<String, String> reader2 = streamReader(key);
			reader2.setConsumer(Consumer.from(DEFAULT_CONSUMER_GROUP, "consumer2"));
			reader2.setAckPolicy(StreamAckPolicy.MANUAL);
			SynchronizedListItemWriter<StreamMessage<String, String>> writer1 = new SynchronizedListItemWriter<>();
			JobExecution execution1 = runAsync(job(testInfo(testInfo, key, "1"), reader1, writer1));
			SynchronizedListItemWriter<StreamMessage<String, String>> writer2 = new SynchronizedListItemWriter<>();
			JobExecution execution2 = runAsync(job(testInfo(testInfo, key, "2"), reader2, writer2));
			awaitTermination(execution1);
			awaitClosed(reader1);
			awaitClosed(writer1);
			awaitTermination(execution2);
			awaitClosed(reader2);
			awaitClosed(writer2);
			Assertions.assertEquals(STREAM_MESSAGE_COUNT, writer1.getItems().size() + writer2.getItems().size());
			assertMessageBody(writer1.getItems());
			assertMessageBody(writer2.getItems());
			RedisModulesCommands<String, String> sync = sourceConnection.sync();
			Assertions.assertEquals(STREAM_MESSAGE_COUNT, sync.xpending(key, DEFAULT_CONSUMER_GROUP).getCount());
			reader1 = streamReader(key);
			reader1.setConsumer(Consumer.from(DEFAULT_CONSUMER_GROUP, "consumer1"));
			reader1.setAckPolicy(StreamAckPolicy.MANUAL);
			reader1.open(new ExecutionContext());
			reader1.ack(writer1.getItems());
			reader1.close();
			reader2 = streamReader(key);
			reader2.setConsumer(Consumer.from(DEFAULT_CONSUMER_GROUP, "consumer2"));
			reader2.setAckPolicy(StreamAckPolicy.MANUAL);
			reader2.open(new ExecutionContext());
			reader2.ack(writer2.getItems());
			reader2.close();
			Assertions.assertEquals(0, sync.xpending(key, DEFAULT_CONSUMER_GROUP).getCount());
		}
	}

	@Test
	void writeStreamMultiExec(TestInfo testInfo) throws Exception {
		String stream = "stream:1";
		List<Map<String, String>> messages = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			Map<String, String> body = new HashMap<>();
			body.put("field1", "value1");
			body.put("field2", "value2");
			messages.add(body);
		}
		ListItemReader<Map<String, String>> reader = new ListItemReader<>(messages);
		Xadd<String, String, Map<String, String>> xadd = new Xadd<>(t -> stream, Function.identity(), m -> null);
		RedisItemWriter<String, String, Map<String, String>> writer = new WriterBuilder(sourceClient).multiExec(true)
				.operation(xadd);
		run(testInfo, reader, writer);
		RedisStreamCommands<String, String> sync = sourceConnection.sync();
		Assertions.assertEquals(messages.size(), sync.xlen(stream));
		List<StreamMessage<String, String>> xrange = sync.xrange(stream, Range.create("-", "+"));
		for (int index = 0; index < xrange.size(); index++) {
			StreamMessage<String, String> message = xrange.get(index);
			Assertions.assertEquals(messages.get(index), message.getBody());
		}
	}
}
