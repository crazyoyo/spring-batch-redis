package com.redis.spring.batch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.util.unit.DataSize;

import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.reader.GeneratorItemReader;
import com.redis.spring.batch.reader.KeyValueReadOperation;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.MemoryUsageOptions;
import com.redis.spring.batch.reader.ReaderOptions;
import com.redis.spring.batch.reader.StreamAckPolicy;
import com.redis.spring.batch.reader.StreamItemReader;
import com.redis.spring.batch.writer.OperationItemWriter;
import com.redis.spring.batch.writer.operation.Xadd;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;

import io.lettuce.core.Consumer;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.Range;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;

class StackTests extends AbstractModulesTests {

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
	void structs(TestInfo testInfo) throws Exception {
		GeneratorItemReader gen = new GeneratorItemReader();
		gen.setMaxItemCount(100);
		generate(testInfo, gen);
		RedisItemReader<String, String> reader = structSourceReader();
		run(testInfo, reader, structTargetWriter());
		Assertions.assertTrue(isOk(compare(testInfo)));
	}

	private RedisItemWriter<String, String> structTargetWriter() {
		return writer(targetClient).struct();
	}

	private RedisItemReader<String, String> structSourceReader() {
		return reader(sourceClient).struct();
	}

	@Test
	void dumpAndRestore(TestInfo testInfo) throws Exception {
		GeneratorItemReader gen = new GeneratorItemReader();
		gen.setMaxItemCount(100);
		generate(testInfo, gen);
		RedisItemReader<byte[], byte[]> reader = reader(sourceClient, ByteArrayCodec.INSTANCE).dump();
		RedisItemWriter<byte[], byte[]> writer = writer(targetClient, ByteArrayCodec.INSTANCE).dump();
		run(testInfo, reader, writer);
		Assertions.assertTrue(isOk(compare(testInfo)));
	}

	@Test
	void liveTypeBasedReplication(TestInfo testInfo) throws Exception {
		enableKeyspaceNotifications(sourceClient);
		RedisItemReader<String, String> reader = structSourceReader();
		RedisItemWriter<String, String> writer = structTargetWriter();
		LiveRedisItemReader<String, String> liveReader = reader(sourceClient).live().struct();
		RedisItemWriter<String, String> liveWriter = structTargetWriter();
		liveReplication(testInfo, reader, writer, liveReader, liveWriter);
	}

	private static final String DEFAULT_CONSUMER_GROUP = "consumerGroup";

	@Test
	void readMultipleStreams(TestInfo testInfo) throws Exception {
		generateStreams(testInfo(testInfo, "streams"));
		final List<String> keys = ScanIterator.scan(sourceConnection.sync(), KeyScanArgs.Builder.type(KeyValue.STREAM))
				.stream().collect(Collectors.toList());
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
		OperationItemWriter<String, String, Map<String, String>> writer = new OperationItemWriter<>(sourceClient,
				StringCodec.UTF8, xadd);
		writer.getOptions().setMultiExec(true);
		run(testInfo, reader, writer);
		RedisStreamCommands<String, String> sync = sourceConnection.sync();
		Assertions.assertEquals(messages.size(), sync.xlen(stream));
		List<StreamMessage<String, String>> xrange = sync.xrange(stream, Range.create("-", "+"));
		for (int index = 0; index < xrange.size(); index++) {
			StreamMessage<String, String> message = xrange.get(index);
			Assertions.assertEquals(messages.get(index), message.getBody());
		}
	}

	@Test
	void luaHashMem() throws InterruptedException, ExecutionException {
		String key = "myhash";
		Map<String, String> hash = new HashMap<>();
		hash.put("field1", "value1");
		hash.put("field2", "value2");
		sourceConnection.sync().hset(key, hash);
		long ttl = System.currentTimeMillis() + 123456;
		sourceConnection.sync().pexpireat(key, ttl);
		KeyValueReadOperation<String, String> operation = KeyValueReadOperation.builder(sourceClient).struct();
		operation.setMemoryUsageOptions(MemoryUsageOptions.builder().limit(DataSize.ofBytes(-1)).build());
		Future<KeyValue<String>> future = operation.execute(sourceConnection.async(), key);
		KeyValue<String> ds = future.get();
		Assertions.assertEquals(key, ds.getKey());
		Assertions.assertEquals(ttl, ds.getTtl());
		Assertions.assertEquals(KeyValue.HASH, ds.getType());
		Assertions.assertTrue(ds.getMemoryUsage() > 0);
	}

	@Test
	void structsMemUsage(TestInfo testInfo) throws Exception {
		generate(testInfo);
		long memLimit = 200;
		RedisItemReader<String, String> reader = reader(sourceClient).options(readerOptions(DataSize.ofBytes(memLimit)))
				.struct();
		List<KeyValue<String>> keyValues = readAll(testInfo, reader);
		Assertions.assertFalse(keyValues.isEmpty());
		for (KeyValue<String> keyValue : keyValues) {
			Assertions.assertTrue(keyValue.getMemoryUsage() > 0);
			if (keyValue.getMemoryUsage() > memLimit) {
				Assertions.assertNull(keyValue.getValue());
			}
		}
	}

	@Test
	void replicateStructsMemLimit(TestInfo testInfo) throws Exception {
		generate(testInfo);
		RedisItemReader<String, String> reader = reader(sourceClient).options(readerOptions(DataSize.ofMegabytes(100)))
				.struct();
		RedisItemWriter<String, String> writer = structTargetWriter();
		run(testInfo, reader, writer);
		Assertions.assertTrue(isOk(compare(testInfo)));
	}

	@Test
	void replicateDumpsMemLimitHigh(TestInfo testInfo) throws Exception {
		generate(testInfo);
		RedisItemReader<byte[], byte[]> reader = reader(sourceClient, ByteArrayCodec.INSTANCE)
				.options(readerOptions(DataSize.ofMegabytes(100))).dump();
		RedisItemWriter<byte[], byte[]> writer = writer(targetClient, ByteArrayCodec.INSTANCE).dump();
		run(testInfo, reader, writer);
		Assertions.assertTrue(isOk(compare(testInfo)));
	}

	private ReaderOptions readerOptions(DataSize size) {
		return ReaderOptions.builder().memoryUsageOptions(MemoryUsageOptions.builder().limit(size).build()).build();
	}

	@Test
	void replicateDumpsMemLimitLow(TestInfo testInfo) throws Exception {
		generate(testInfo);
		Assertions.assertTrue(sourceConnection.sync().dbsize() > 10);
		long memLimit = 1500;
		RedisItemReader<byte[], byte[]> reader = reader(sourceClient, ByteArrayCodec.INSTANCE)
				.options(readerOptions(DataSize.ofBytes(memLimit))).dump();
		RedisItemWriter<byte[], byte[]> writer = writer(targetClient, ByteArrayCodec.INSTANCE).dump();
		run(testInfo, reader, writer);
		RedisItemReader<String, String> fullReader = RedisItemReader.client(sourceClient, StringCodec.UTF8)
				.jobRepository(jobRepository).options(readerOptions(DataSize.ofBytes(-1))).struct();
		List<KeyValue<String>> items = readAll(testInfo, fullReader);
		List<KeyValue<String>> bigkeys = items.stream().filter(ds -> ds.getMemoryUsage() > memLimit)
				.collect(Collectors.toList());
		Assertions.assertEquals(sourceConnection.sync().dbsize(), bigkeys.size() + targetConnection.sync().dbsize());
	}

	@Test
	void replicateMemLimit(TestInfo testInfo) throws Exception {
		DataSize limit = DataSize.ofBytes(500);
		String key1 = "key:1";
		sourceConnection.sync().set(key1, "bar");
		String key2 = "key:2";
		sourceConnection.sync().set(key2, GeneratorItemReader.randomString(Math.toIntExact(limit.toBytes() * 2)));
		RedisItemReader<String, String> reader = reader(sourceClient).options(readerOptions(limit)).struct();
		List<KeyValue<String>> keyValues = readAll(testInfo, reader);
		Map<String, KeyValue<String>> map = keyValues.stream().collect(Collectors.toMap(s -> s.getKey(), s -> s));
		Assertions.assertNull(map.get(key2).getValue());
	}

	@Test
	void blockBigKeys() throws Exception {
		enableKeyspaceNotifications(sourceClient);
		LiveRedisItemReader<String, String> reader = reader(sourceClient).live()
				.options(readerOptions(DataSize.ofBytes(300))).struct();
		reader.setName("blockBigKeys");
		reader.open(new ExecutionContext());
		RedisModulesCommands<String, String> commands = sourceConnection.sync();
		String key = "key:1";
		for (int index = 0; index < 30; index++) {
			commands.sadd(key, "value:" + index);
			Thread.sleep(10);
		}
		Assertions.assertTrue(reader.getKeyReader().getBlockedKeys().contains(key));
		reader.close();
	}
}
