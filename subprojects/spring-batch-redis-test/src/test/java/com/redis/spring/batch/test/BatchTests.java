package com.redis.spring.batch.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.IteratorItemReader;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.Beers;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.search.IndexInfo;
import com.redis.lettucemod.search.Suggestion;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.DuplicatePolicy;
import com.redis.lettucemod.timeseries.RangeOptions;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.lettucemod.timeseries.TimeRange;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.Range;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemReader.ReaderMode;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.DataType;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.gen.GeneratorItemReader;
import com.redis.spring.batch.item.redis.gen.TimeSeriesOptions;
import com.redis.spring.batch.item.redis.reader.DefaultKeyComparator;
import com.redis.spring.batch.item.redis.reader.Evalsha;
import com.redis.spring.batch.item.redis.reader.KeyComparison;
import com.redis.spring.batch.item.redis.reader.KeyComparison.Status;
import com.redis.spring.batch.item.redis.reader.KeyComparisonItemReader;
import com.redis.spring.batch.item.redis.reader.KeyNotificationItemReader;
import com.redis.spring.batch.item.redis.reader.KeyScanNotificationItemReader;
import com.redis.spring.batch.item.redis.reader.KeyValueRead;
import com.redis.spring.batch.item.redis.reader.StreamItemReader;
import com.redis.spring.batch.item.redis.reader.StreamItemReader.AckPolicy;
import com.redis.spring.batch.item.redis.writer.KeyValueWrite;
import com.redis.spring.batch.item.redis.writer.operation.Geoadd;
import com.redis.spring.batch.item.redis.writer.operation.Hset;
import com.redis.spring.batch.item.redis.writer.operation.JsonDel;
import com.redis.spring.batch.item.redis.writer.operation.JsonSet;
import com.redis.spring.batch.item.redis.writer.operation.Sugadd;
import com.redis.spring.batch.item.redis.writer.operation.TsAdd;
import com.redis.spring.batch.step.FlushingStepBuilder;

import io.lettuce.core.Consumer;
import io.lettuce.core.GeoArgs;
import io.lettuce.core.GeoValue;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.models.stream.PendingMessages;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
abstract class BatchTests extends AbstractTargetTestBase {

	@Test
	void compareSet(TestInfo info) throws Exception {
		redisCommands.sadd("set:1", "value1", "value2");
		targetRedisCommands.sadd("set:1", "value2", "value1");
		KeyComparisonItemReader<String, String> reader = comparisonReader(info);
		reader.open(new ExecutionContext());
		List<KeyComparison<String>> comparisons = readAll(reader);
		reader.close();
		Assertions.assertEquals(Status.OK, comparisons.get(0).getStatus());
	}

	private <T> Evalsha<String, String, T> evalsha(Function<T, String> keyFunction) throws IOException {
		Evalsha<String, String, T> evalsha = new Evalsha<>(StringCodec.UTF8, keyFunction);
		String lua = BatchUtils.readFile("keyvalue.lua");
		String digest = redisConnection.sync().scriptLoad(lua);
		evalsha.setDigest(digest);
		return evalsha;
	}

	private static final KeyValueRead<String, String, Object> read = KeyValueRead.struct(StringCodec.UTF8);

	private KeyValue<String, Object> convert(List<Object> list) {
		return read.convert(list);
	}

	@Test
	void evalsha() throws Exception {
		String key = "key:1";
		String value = "value1";
		redisCommands.set(key, value);
		Evalsha<String, String, String> evalsha = evalsha(Function.identity());
		evalsha.setArgs("STRUCT", 0, 0);
		List<Object> result = evalsha.execute(redisAsyncCommands, key).get();
		KeyValue<String, Object> kv = convert(result);
		Assertions.assertEquals(key, kv.getKey());
		Assertions.assertEquals(-1, kv.getTtl());
		Assertions.assertEquals(DataType.STRING.getString(), kv.getType());
		Assertions.assertEquals(100, kv.getMemoryUsage(), 50);
		Assertions.assertEquals(value, kv.getValue());
		evalsha.setArgs("NONE", Long.MAX_VALUE, 5);
		result = evalsha.execute(redisAsyncCommands, key).get();
		kv = convert(result);
		Assertions.assertEquals(key, kv.getKey());
		Assertions.assertEquals(-1, kv.getTtl());
		Assertions.assertEquals(DataType.STRING.getString(), kv.getType());
		Assertions.assertEquals(100, kv.getMemoryUsage(), 50);
		Assertions.assertNull(kv.getValue());
		evalsha.setArgs("STRUCT", Long.MAX_VALUE, 5);
		result = evalsha.execute(redisAsyncCommands, key).get();
		kv = convert(result);
		Assertions.assertEquals(key, kv.getKey());
		Assertions.assertEquals(-1, kv.getTtl());
		Assertions.assertEquals(DataType.STRING.getString(), kv.getType());
		Assertions.assertEquals(100, kv.getMemoryUsage(), 50);
		Assertions.assertEquals(value, kv.getValue());
		Instant expireAt = Instant.now().plusSeconds(4321);
		redisCommands.expireat(key, expireAt);
		evalsha.setArgs("STRUCT", Long.MAX_VALUE, 5);
		result = evalsha.execute(redisAsyncCommands, key).get();
		kv = convert(result);
		Assertions.assertEquals(key, kv.getKey());
		Assertions.assertEquals(expireAt.toEpochMilli(), kv.getTime() + kv.getTtl(), 100);
		Assertions.assertEquals(DataType.STRING.getString(), kv.getType());
		Assertions.assertEquals(100, kv.getMemoryUsage(), 50);
		Assertions.assertEquals(value, kv.getValue());
	}

	@Test
	void compareQuick(TestInfo info) throws Exception {
		int sourceCount = 2;
		for (int index = 0; index < sourceCount; index++) {
			redisCommands.set("key:" + index, "value:" + index);
		}
		int targetCount = 1;
		for (int index = 0; index < targetCount; index++) {
			targetRedisCommands.set("key:" + index, "value:" + index);
		}
		RedisItemReader<String, String, Object> source = RedisItemReader.type();
		source.setClient(redisClient);
		RedisItemReader<String, String, Object> target = RedisItemReader.type();
		target.setClient(targetRedisClient);
		KeyComparisonItemReader<String, String> reader = comparisonReader(info);
		reader.open(new ExecutionContext());
		List<KeyComparison<String>> comparisons = readAll(reader);
		reader.close();
		KeyspaceComparison<String> comparison = new KeyspaceComparison<>(comparisons);
		Assertions.assertEquals(sourceCount - targetCount, comparison.get(Status.MISSING).size());
	}

	@Test
	void readStruct(TestInfo info) throws Exception {
		generate(info, generator(73));
		RedisItemReader<String, String, Object> reader = structReader(info);
		reader.open(new ExecutionContext());
		List<KeyValue<String, Object>> list = readAll(reader);
		reader.close();
		assertEquals(redisCommands.dbsize(), list.size());
	}

	@Test
	void readStructThreads(TestInfo info) throws Exception {
		generate(info, generator(73));
		RedisItemReader<String, String, Object> reader = structReader(info);
		reader.setThreads(8);
		reader.open(new ExecutionContext());
		List<KeyValue<String, Object>> list = readAll(reader);
		reader.close();
		assertEquals(redisCommands.dbsize(), list.size());
	}

	@Test
	void readStreamAutoAck(TestInfo info) throws Exception {
		String stream = "stream1";
		String consumerGroup = "batchtests-readStreamAutoAck";
		Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
		final StreamItemReader<String, String> reader = streamReader(info, stream, consumer);
		reader.setAckPolicy(AckPolicy.AUTO);
		reader.open(new ExecutionContext());
		String field1 = "field1";
		String value1 = "value1";
		String field2 = "field2";
		String value2 = "value2";
		Map<String, String> body = map(field1, value1, field2, value2);
		String id1 = redisCommands.xadd(stream, body);
		String id2 = redisCommands.xadd(stream, body);
		String id3 = redisCommands.xadd(stream, body);
		List<StreamMessage<String, String>> messages = new ArrayList<>();
		awaitUntil(() -> messages.addAll(reader.readMessages()));
		Assertions.assertEquals(3, messages.size());
		assertStreamEquals(id1, body, stream, messages.get(0));
		assertStreamEquals(id2, body, stream, messages.get(1));
		assertStreamEquals(id3, body, stream, messages.get(2));
		reader.close();
		Assertions.assertEquals(0, redisCommands.xpending(stream, consumerGroup).getCount(), "pending messages");
	}

	@Test
	void readStreamManualAck(TestInfo info) throws Exception {
		String stream = "stream1";
		String consumerGroup = "batchtests-readStreamManualAck";
		Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
		final StreamItemReader<String, String> reader = streamReader(info, stream, consumer);
		reader.setAckPolicy(AckPolicy.MANUAL);
		reader.open(new ExecutionContext());
		String field1 = "field1";
		String value1 = "value1";
		String field2 = "field2";
		String value2 = "value2";
		Map<String, String> body = map(field1, value1, field2, value2);
		String id1 = redisCommands.xadd(stream, body);
		String id2 = redisCommands.xadd(stream, body);
		String id3 = redisCommands.xadd(stream, body);
		List<StreamMessage<String, String>> messages = new ArrayList<>();
		awaitUntil(() -> messages.addAll(reader.readMessages()));
		Assertions.assertEquals(3, messages.size());

		assertStreamEquals(id1, body, stream, messages.get(0));
		assertStreamEquals(id2, body, stream, messages.get(1));
		assertStreamEquals(id3, body, stream, messages.get(2));
		PendingMessages pendingMsgsBeforeCommit = redisCommands.xpending(stream, consumerGroup);
		Assertions.assertEquals(3, pendingMsgsBeforeCommit.getCount(), "pending messages before commit");
		redisCommands.xack(stream, consumerGroup, messages.get(0).getId(), messages.get(1).getId());
		PendingMessages pendingMsgsAfterCommit = redisCommands.xpending(stream, consumerGroup);
		Assertions.assertEquals(1, pendingMsgsAfterCommit.getCount(), "pending messages after commit");
		reader.close();
	}

	@Test
	void readStreamManualAckRecover(TestInfo info) throws Exception {
		String stream = "stream1";
		Consumer<String> consumer = Consumer.from("batchtests-readStreamManualAckRecover", "consumer1");
		final StreamItemReader<String, String> reader = streamReader(info, stream, consumer);
		reader.setAckPolicy(AckPolicy.MANUAL);
		reader.open(new ExecutionContext());
		String field1 = "field1";
		String value1 = "value1";
		String field2 = "field2";
		String value2 = "value2";
		Map<String, String> body = map(field1, value1, field2, value2);
		redisCommands.xadd(stream, body);
		redisCommands.xadd(stream, body);
		redisCommands.xadd(stream, body);
		List<StreamMessage<String, String>> messages = new ArrayList<>();
		awaitUntil(() -> messages.addAll(reader.readMessages()));
		Assertions.assertEquals(3, messages.size());

		List<StreamMessage<String, String>> recoveredMessages = new ArrayList<>();
		redisCommands.xadd(stream, body);
		redisCommands.xadd(stream, body);
		redisCommands.xadd(stream, body);

		reader.close();

		final StreamItemReader<String, String> reader2 = streamReader(info, stream, consumer);
		reader2.setAckPolicy(AckPolicy.MANUAL);
		reader2.open(new ExecutionContext());

		awaitUntil(() -> recoveredMessages.addAll(reader2.readMessages()));
		awaitUntil(() -> !recoveredMessages.addAll(reader2.readMessages()));

		Assertions.assertEquals(6, recoveredMessages.size());
	}

	@Test
	void readStreamManualAckRecoverUncommitted(TestInfo info) throws Exception {
		String stream = "stream1";
		String consumerGroup = "batchtests-readStreamManualAckRecoverUncommitted";
		Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
		final StreamItemReader<String, String> reader = streamReader(info, stream, consumer);
		reader.setAckPolicy(AckPolicy.MANUAL);
		reader.open(new ExecutionContext());
		String field1 = "field1";
		String value1 = "value1";
		String field2 = "field2";
		String value2 = "value2";
		Map<String, String> body = map(field1, value1, field2, value2);
		redisCommands.xadd(stream, body);
		redisCommands.xadd(stream, body);
		String id3 = redisCommands.xadd(stream, body);
		List<StreamMessage<String, String>> messages = new ArrayList<>();
		awaitUntil(() -> messages.addAll(reader.readMessages()));
		Assertions.assertEquals(3, messages.size());
		redisCommands.xack(stream, consumerGroup, messages.get(0).getId(), messages.get(1).getId());

		List<StreamMessage<String, String>> recoveredMessages = new ArrayList<>();
		String id4 = redisCommands.xadd(stream, body);
		String id5 = redisCommands.xadd(stream, body);
		String id6 = redisCommands.xadd(stream, body);
		reader.close();

		final StreamItemReader<String, String> reader2 = streamReader(info, stream, consumer);
		reader2.setAckPolicy(AckPolicy.MANUAL);
		reader2.setOffset(messages.get(1).getId());
		reader2.open(new ExecutionContext());

		// Wait until task.poll() doesn't return any more records
		awaitUntil(() -> recoveredMessages.addAll(reader2.readMessages()));
		awaitUntil(() -> !recoveredMessages.addAll(reader2.readMessages()));
		List<String> recoveredIds = recoveredMessages.stream().map(StreamMessage::getId).collect(Collectors.toList());
		Assertions.assertEquals(Arrays.<String>asList(id3, id4, id5, id6), recoveredIds, "recoveredIds");
		reader2.close();
	}

	@Test
	void readStreamManualAckRecoverFromOffset(TestInfo info) throws Exception {
		String stream = "stream1";
		String consumerGroup = "batchtests-readStreamManualAckRecoverFromOffset";
		Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
		final StreamItemReader<String, String> reader = streamReader(info, stream, consumer);
		reader.setAckPolicy(AckPolicy.MANUAL);
		reader.open(new ExecutionContext());
		String field1 = "field1";
		String value1 = "value1";
		String field2 = "field2";
		String value2 = "value2";
		Map<String, String> body = map(field1, value1, field2, value2);
		redisCommands.xadd(stream, body);
		redisCommands.xadd(stream, body);
		String id3 = redisCommands.xadd(stream, body);
		List<StreamMessage<String, String>> sourceRecords = new ArrayList<>();
		awaitUntil(() -> sourceRecords.addAll(reader.readMessages()));
		Assertions.assertEquals(3, sourceRecords.size());

		List<StreamMessage<String, String>> recoveredRecords = new ArrayList<>();
		String id4 = redisCommands.xadd(stream, body);
		String id5 = redisCommands.xadd(stream, body);
		String id6 = redisCommands.xadd(stream, body);

		reader.close();

		final StreamItemReader<String, String> reader2 = streamReader(info, stream, consumer);
		reader2.setAckPolicy(AckPolicy.MANUAL);
		reader2.setOffset(id3);
		reader2.open(new ExecutionContext());

		// Wait until task.poll() doesn't return any more records
		awaitUntil(() -> recoveredRecords.addAll(reader2.readMessages()));
		awaitUntil(() -> !recoveredRecords.addAll(reader2.readMessages()));
		List<String> recoveredIds = recoveredRecords.stream().map(StreamMessage::getId).collect(Collectors.toList());
		Assertions.assertEquals(Arrays.<String>asList(id4, id5, id6), recoveredIds, "recoveredIds");
		reader2.close();
	}

	@Test
	void readStreamRecoverManualAckToAutoAck(TestInfo info) throws Exception {
		String stream = "stream1";
		String consumerGroup = "readStreamRecoverManualAckToAutoAck";
		Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
		final StreamItemReader<String, String> reader = streamReader(info, stream, consumer);
		reader.setAckPolicy(AckPolicy.MANUAL);
		reader.open(new ExecutionContext());
		String field1 = "field1";
		String value1 = "value1";
		String field2 = "field2";
		String value2 = "value2";
		Map<String, String> body = map(field1, value1, field2, value2);
		redisCommands.xadd(stream, body);
		redisCommands.xadd(stream, body);
		redisCommands.xadd(stream, body);
		List<StreamMessage<String, String>> sourceRecords = new ArrayList<>();
		awaitUntil(() -> sourceRecords.addAll(reader.readMessages()));
		Assertions.assertEquals(3, sourceRecords.size());

		List<StreamMessage<String, String>> recoveredRecords = new ArrayList<>();
		String id4 = redisCommands.xadd(stream, body);
		String id5 = redisCommands.xadd(stream, body);
		String id6 = redisCommands.xadd(stream, body);
		reader.close();

		final StreamItemReader<String, String> reader2 = streamReader(info, stream, consumer);
		reader2.setAckPolicy(AckPolicy.AUTO);
		reader2.open(new ExecutionContext());

		// Wait until task.poll() doesn't return any more records
		awaitUntil(() -> recoveredRecords.addAll(reader2.readMessages()));
		awaitUntil(() -> !recoveredRecords.addAll(reader2.readMessages()));
		List<String> recoveredIds = recoveredRecords.stream().map(StreamMessage::getId).collect(Collectors.toList());
		Assertions.assertEquals(Arrays.asList(id4, id5, id6), recoveredIds, "recoveredIds");

		PendingMessages pending = redisCommands.xpending(stream, consumerGroup);
		Assertions.assertEquals(0, pending.getCount(), "pending message count");
		reader2.close();
	}

	@Test
	void readStreamAck(TestInfo info) throws Exception {
		generateStreams(info, 57);
		List<String> keys = ScanIterator.scan(redisCommands, KeyScanArgs.Builder.type(DataType.STREAM.getString()))
				.stream().collect(Collectors.toList());
		Consumer<String> consumer = Consumer.from("batchtests-readmessages", "consumer1");
		for (String key : keys) {
			long count = redisCommands.xlen(key);
			StreamItemReader<String, String> reader = streamReader(info, key, consumer);
			reader.open(new ExecutionContext());
			List<StreamMessage<String, String>> messages = readAll(reader);
			assertEquals(count, messages.size());
			assertMessageBody(messages);
			awaitUntil(() -> reader.ack(reader.readMessages()) == 0);
			reader.close();
		}
	}

	@Test
	void readStream(TestInfo info) throws Exception {
		generateStreams(info, 73);
		List<String> keys = ScanIterator.scan(redisCommands, KeyScanArgs.Builder.type(DataType.STREAM.getString()))
				.stream().collect(Collectors.toList());
		Consumer<String> consumer = Consumer.from("batchtests-readstreamjob", "consumer1");
		for (String key : keys) {
			long count = redisCommands.xlen(key);
			StreamItemReader<String, String> reader = streamReader(info, key, consumer);
			reader.open(new ExecutionContext());
			List<StreamMessage<String, String>> messages = readAll(reader);
			reader.close();
			Assertions.assertEquals(count, messages.size());
			assertMessageBody(messages);
		}
	}

	@Test
	void readStructHash(TestInfo info) throws Exception {
		String key = "myhash";
		Map<String, String> hash = new HashMap<>();
		hash.put("field1", "value1");
		hash.put("field2", "value2");
		redisCommands.hset(key, hash);
		long ttl = System.currentTimeMillis() + 123456;
		redisCommands.pexpireat(key, ttl);
		RedisItemReader<String, String, Object> reader = structReader(info);
		reader.open(new ExecutionContext());
		KeyValue<String, Object> keyValue = reader.read();
		Assertions.assertEquals(key, keyValue.getKey());
		Assertions.assertEquals(ttl, KeyValue.absoluteTTL(keyValue), 1000);
		Assertions.assertEquals(DataType.HASH, KeyValue.type(keyValue));
		Assertions.assertEquals(hash, keyValue.getValue());
		reader.close();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void readStructZset(TestInfo info) throws Exception {
		String key = "myzset";
		ScoredValue[] values = { ScoredValue.just(123.456, "value1"), ScoredValue.just(654.321, "value2") };
		redisCommands.zadd(key, values);
		RedisItemReader<String, String, Object> reader = structReader(info);
		reader.open(new ExecutionContext());
		KeyValue<String, Object> ds = reader.read();
		Assertions.assertEquals(key, ds.getKey());
		Assertions.assertEquals(DataType.ZSET.getString(), ds.getType());
		Assertions.assertEquals(new HashSet<>(Arrays.asList(values)), ds.getValue());
		reader.close();
	}

	@Test
	void readStructList(TestInfo info) throws Exception {
		String key = "mylist";
		List<String> values = Arrays.asList("value1", "value2");
		redisCommands.rpush(key, values.toArray(new String[0]));
		RedisItemReader<String, String, Object> reader = structReader(info);
		reader.open(new ExecutionContext());
		KeyValue<String, Object> ds = reader.read();
		Assertions.assertEquals(key, ds.getKey());
		Assertions.assertEquals(DataType.LIST.getString(), ds.getType());
		Assertions.assertEquals(values, ds.getValue());
		reader.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	void readStructStream(TestInfo info) throws Exception {
		String key = "mystream";
		Map<String, String> body = new HashMap<>();
		body.put("field1", "value1");
		body.put("field2", "value2");
		redisCommands.xadd(key, body);
		redisCommands.xadd(key, body);
		RedisItemReader<String, String, Object> reader = structReader(info);
		reader.open(new ExecutionContext());
		KeyValue<String, Object> ds = reader.read();
		Assertions.assertEquals(key, ds.getKey());
		Assertions.assertEquals(DataType.STREAM.getString(), ds.getType());
		List<StreamMessage<String, String>> messages = (List<StreamMessage<String, String>>) ds.getValue();
		Assertions.assertEquals(2, messages.size());
		for (StreamMessage<String, String> message : messages) {
			Assertions.assertEquals(body, message.getBody());
			Assertions.assertNotNull(message.getId());
		}
		reader.close();
	}

	@Test
	void readDumpStream(TestInfo info) throws Exception {
		String key = "mystream";
		Map<String, String> body = new HashMap<>();
		body.put("field1", "value1");
		body.put("field2", "value2");
		redisCommands.xadd(key, body);
		redisCommands.xadd(key, body);
		long ttl = System.currentTimeMillis() + 123456;
		redisCommands.pexpireat(key, ttl);
		RedisItemReader<byte[], byte[], byte[]> reader = dumpReader(info);
		reader.open(new ExecutionContext());
		KeyValue<byte[], byte[]> dump = reader.read();
		Assertions.assertArrayEquals(toByteArray(key), dump.getKey());
		Assertions.assertEquals(ttl, KeyValue.absoluteTTL(dump), 1000);
		redisCommands.del(key);
		redisCommands.restore(key, (byte[]) dump.getValue(), RestoreArgs.Builder.ttl(ttl).absttl());
		Assertions.assertEquals(DataType.STREAM.getString(), redisCommands.type(key));
		reader.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	void readStructStreamByteArray(TestInfo info) throws Exception {
		String key = "mystream";
		Map<String, String> body = new HashMap<>();
		body.put("field1", "value1");
		body.put("field2", "value2");
		redisCommands.xadd(key, body);
		redisCommands.xadd(key, body);
		RedisItemReader<byte[], byte[], Object> reader = structReader(info, ByteArrayCodec.INSTANCE);
		reader.open(new ExecutionContext());
		KeyValue<byte[], Object> ds = reader.read();
		Assertions.assertArrayEquals(toByteArray(key), ds.getKey());
		Assertions.assertEquals(DataType.STREAM.getString(), ds.getType());
		List<StreamMessage<byte[], byte[]>> messages = (List<StreamMessage<byte[], byte[]>>) ds.getValue();
		Assertions.assertEquals(2, messages.size());
		for (StreamMessage<byte[], byte[]> message : messages) {
			Map<byte[], byte[]> actual = message.getBody();
			Assertions.assertEquals(2, actual.size());
			Map<String, String> actualString = new HashMap<>();
			actual.forEach((k, v) -> actualString.put(toString(k), toString(v)));
			Assertions.assertEquals(body, actualString);
		}
		reader.close();
	}

	@Test
	void readStructHLL(TestInfo info) throws Exception {
		String key1 = "hll:1";
		redisCommands.pfadd(key1, "member:1", "member:2");
		String key2 = "hll:2";
		redisCommands.pfadd(key2, "member:1", "member:2", "member:3");
		RedisItemReader<String, String, Object> reader = structReader(info);
		reader.open(new ExecutionContext());
		List<KeyValue<String, Object>> items = readAll(reader);
		Assertions.assertEquals(2, items.size());
		Optional<KeyValue<String, Object>> result = items.stream().filter(ds -> ds.getKey().equals(key1)).findFirst();
		Assertions.assertTrue(result.isPresent());
		Assertions.assertEquals(key1, result.get().getKey());
		Assertions.assertEquals(DataType.STRING.getString(), result.get().getType());
		Assertions.assertEquals(redisCommands.get(key1), result.get().getValue());
		reader.close();
	}

	@Test
	void replicateDump(TestInfo info) throws Exception {
		GeneratorItemReader gen = generator(100);
		generate(info, gen);
		RedisItemWriter<byte[], byte[], KeyValue<byte[], byte[]>> writer = RedisItemWriter.dump();
		writer.setClient(targetRedisClient);
		replicate(info, dumpReader(info), writer);
	}

	protected <K, V, T> void replicate(TestInfo info, RedisItemReader<K, V, T> reader,
			RedisItemWriter<K, V, KeyValue<K, T>> writer) throws Exception {
		run(testInfo(info, "replicate"), reader, writer);
		awaitUntil(reader::isComplete);
		KeyspaceComparison<String> comparison = compare(testInfo(info, "replicate"));
		Assertions.assertEquals(Collections.emptyList(), comparison.mismatches());
	}

	@Test
	void replicateStructEmptyCollections(TestInfo info) throws Exception {
		GeneratorItemReader gen = generator(123);
		Range cardinality = Range.of(0);
		gen.getOptions().getHashOptions().setFieldCount(cardinality);
		gen.getOptions().getSetOptions().setMemberCount(cardinality);
		gen.getOptions().getStreamOptions().setMessageCount(cardinality);
		gen.getOptions().getTimeSeriesOptions().setSampleCount(cardinality);
		gen.getOptions().getZsetOptions().setMemberCount(cardinality);
		generate(info, gen);
		RedisItemReader<String, String, Object> reader = structReader(info);
		RedisItemWriter<String, String, KeyValue<String, Object>> writer = RedisItemWriter.struct();
		writer.setClient(targetRedisClient);
		replicate(info, reader, writer);
	}

	@Test
	void replicateEmptyStream(TestInfo info) throws Exception {
		String key = "mystream";
		Map<String, String> body = new HashMap<>();
		body.put("test", "test");
		redisCommands.xadd(key, body);
		redisCommands.xtrim(key, 0);
		RedisItemReader<String, String, Object> reader = structReader(info);
		RedisItemWriter<String, String, KeyValue<String, Object>> writer = RedisItemWriter.struct();
		writer.setClient(targetRedisClient);
		replicate(info, reader, writer);
	}

	@Test
	void replicateIgnoreEmptyStream(TestInfo info) throws Exception {
		String key = "mystream";
		Map<String, String> body = new HashMap<>();
		body.put("test", "test");
		redisCommands.xadd(key, body);
		redisCommands.xtrim(key, 0);
		RedisItemReader<String, String, Object> reader = structReader(info);
		RedisItemWriter<String, String, KeyValue<String, Object>> writer = RedisItemWriter.struct();
		((KeyValueWrite<String, String>) writer.getOperation()).getXadd().setIgnoreEmptyStreams(true);
		writer.setClient(targetRedisClient);
		run(testInfo(info, "replicate"), reader, writer);
		awaitUntil(reader::isComplete);
		KeyspaceComparison<String> comparison = compare(testInfo(info, "replicate"));
		Assertions.assertEquals(key, comparison.get(Status.MISSING).get(0).getSource().getKey());
	}

	public static <T> Function<T, String> keyFunction(String key) {
		return t -> key;
	}

	@Test
	void writeGeo(TestInfo info) throws Exception {
		ListItemReader<Geo> reader = new ListItemReader<>(
				Arrays.asList(new Geo("Venice Breakwater", -118.476056, 33.985728),
						new Geo("Long Beach National", -73.667022, 40.582739)));
		Geoadd<String, String, Geo> geoadd = new Geoadd<>(keyFunction("geoset"), this::geoValue);
		RedisItemWriter<String, String, Geo> writer = writer(geoadd);
		run(info, reader, writer);
		Set<String> radius1 = redisCommands.georadius("geoset", -118, 34, 100, GeoArgs.Unit.mi);
		assertEquals(1, radius1.size());
		assertTrue(radius1.contains("Venice Breakwater"));
	}

	private GeoValue<String> geoValue(Geo geo) {
		return GeoValue.just(geo.getLongitude(), geo.getLatitude(), geo.getMember());
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

	@Test
	void writeWait(TestInfo info) throws Exception {
		List<KeyValue<String, Map<String, String>>> items = IntStream.range(0, 5).mapToObj(index -> {
			KeyValue<String, Map<String, String>> item = new KeyValue<>();
			item.setKey("key:" + index);
			item.setValue(map("field1", "value1", "field2", "value2"));
			return item;
		}).collect(Collectors.toList());
		ListItemReader<KeyValue<String, Map<String, String>>> reader = new ListItemReader<>(items);
		Hset<String, String, KeyValue<String, Map<String, String>>> hset = new Hset<>(KeyValue::getKey,
				KeyValue::getValue);
		RedisItemWriter<String, String, KeyValue<String, Map<String, String>>> writer = writer(hset);
		writer.setWaitReplicas(1);
		writer.setWaitTimeout(Duration.ofMillis(300));
		SimpleStepBuilder<KeyValue<String, Map<String, String>>, KeyValue<String, Map<String, String>>> step = step(
				info, reader, null, writer);
		JobExecution execution = run(job(info).start(step.build()).build());
		List<Throwable> exceptions = execution.getAllFailureExceptions();
		assertEquals("Insufficient replication level (0/1)", exceptions.get(0).getCause().getMessage());
	}

	private <K, V, T> void replicateLive(TestInfo info, RedisItemReader<K, V, T> reader,
			RedisItemWriter<K, V, KeyValue<K, T>> writer) throws Exception {
		live(reader);
		DataType[] types = new DataType[] { DataType.HASH, DataType.STRING };
		generate(info, generator(300, types));
		TaskletStep step = faultTolerant(flushingStep(new SimpleTestInfo(info, "step"), reader, writer)).build();
		GeneratorItemReader liveGen = generator(700, types);
		liveGen.getOptions().setExpiration(Range.of(100));
		liveGen.getOptions().setKeyRange(Range.from(300));
		generateAsync(testInfo(info, "genasync"), liveGen);
		run(job(info).start(step).build());
		awaitUntilNoSubscribers();
		KeyspaceComparison<String> comparison = compare(info);
		Assertions.assertEquals(Collections.emptyList(), comparison.mismatches());
	}

	@Test
	void keyScanNotificationReader(TestInfo info) throws Exception {
		enableKeyspaceNotifications();
		int scanCount = 10;
		IntStream.range(0, scanCount).forEach(i -> redisCommands.set("scan:" + i, "value" + i));
		KeyScanNotificationItemReader<String, String> reader = new KeyScanNotificationItemReader<>(redisClient,
				StringCodec.UTF8, new IteratorItemReader<>(ScanIterator.scan(redisCommands)));
		setName(info, reader);
		reader.setName(JSON_BEER_1);
		reader.open(new ExecutionContext());
		int notificationCount = 10;
		IntStream.range(0, notificationCount).forEach(i -> redisCommands.set("notification:" + i, "value" + i));
		Set<String> keys = new HashSet<>();
		String key;
		while ((key = reader.poll(10, TimeUnit.MILLISECONDS)) != null) {
			keys.add(key);
		}
		reader.close();
		Assertions.assertEquals(scanCount + notificationCount, keys.size());
	}

	@Test
	void readKeyNotificationsDedupe() throws Exception {
		enableKeyspaceNotifications();
		KeyNotificationItemReader<String, String> keyReader = new KeyNotificationItemReader<>(redisClient,
				StringCodec.UTF8);
		keyReader.open(new ExecutionContext());
		try {
			String key = "key1";
			redisCommands.zadd(key, 1, "member1");
			redisCommands.zadd(key, 2, "member2");
			redisCommands.zadd(key, 3, "member3");
			awaitUntil(() -> keyReader.getQueue().size() == 1);
			Assertions.assertEquals(key, keyReader.getQueue().take());
		} finally {
			keyReader.close();
		}
	}

	@Test
	void replicateDumpLive(TestInfo info) throws Exception {
		enableKeyspaceNotifications();
		RedisItemReader<byte[], byte[], byte[]> reader = dumpReader(info);
		RedisItemWriter<byte[], byte[], KeyValue<byte[], byte[]>> writer = RedisItemWriter.dump();
		writer.setClient(targetRedisClient);
		replicateLive(info, reader, writer);
	}

	@Test
	void replicateStructLive(TestInfo info) throws Exception {
		enableKeyspaceNotifications();
		RedisItemReader<String, String, Object> reader = structReader(info);
		RedisItemWriter<String, String, KeyValue<String, Object>> writer = RedisItemWriter.struct();
		writer.setClient(targetRedisClient);
		replicateLive(info, reader, writer);
	}

	@Test
	void replicateDumpLiveOnly(TestInfo info) throws Exception {
		enableKeyspaceNotifications();
		RedisItemReader<byte[], byte[], byte[]> reader = dumpReader(info);
		live(reader);
		reader.setMode(ReaderMode.LIVEONLY);
		RedisItemWriter<byte[], byte[], KeyValue<byte[], byte[]>> writer = RedisItemWriter.dump();
		writer.setClient(targetRedisClient);
		FlushingStepBuilder<KeyValue<byte[], byte[]>, KeyValue<byte[], byte[]>> step = flushingStep(info, reader,
				writer);
		GeneratorItemReader gen = generator(100, DataType.HASH, DataType.LIST, DataType.SET, DataType.STRING,
				DataType.ZSET);
		generateAsync(testInfo(info, "genasync"), gen);
		run(info, step);
		awaitUntilNoSubscribers();
		Assertions.assertEquals(Collections.emptyList(), compare(info).mismatches());
	}

	@Test
	void replicateSetLiveOnly(TestInfo info) throws Exception {
		enableKeyspaceNotifications();
		String key = "myset";
		redisCommands.sadd(key, "1", "2", "3", "4", "5");
		RedisItemReader<String, String, Object> reader = structReader(info);
		live(reader);
		reader.setNotificationQueueCapacity(100);
		RedisItemWriter<String, String, KeyValue<String, Object>> writer = RedisItemWriter.struct();
		writer.setClient(targetRedisClient);
		FlushingStepBuilder<KeyValue<String, Object>, KeyValue<String, Object>> step = flushingStep(info, reader,
				writer);
		Executors.newSingleThreadExecutor().execute(() -> {
			awaitUntilSubscribers();
			redisCommands.srem(key, "5");
		});
		run(info, step);
		awaitUntilNoSubscribers();
		assertEquals(redisCommands.smembers(key), targetRedisCommands.smembers(key));
	}

	@SuppressWarnings("unchecked")
	@Test
	void compareBinaryKeyValue(TestInfo info) throws Exception {
		byte[] zsetKey = randomBytes();
		Collection<ScoredValue<byte[]>> zsetValue = new ArrayList<>();
		for (int index = 0; index < 10; index++) {
			zsetValue.add(ScoredValue.just(index, randomBytes()));
		}
		byte[] listKey = randomBytes();
		List<byte[]> listValue = new ArrayList<>();
		for (int index = 0; index < 10; index++) {
			listValue.add(randomBytes());
		}
		byte[] setKey = randomBytes();
		Set<byte[]> setValue = new HashSet<>();
		for (int index = 0; index < 10; index++) {
			setValue.add(randomBytes());
		}
		byte[] hashKey = randomBytes();
		Map<byte[], byte[]> hashValue = byteArrayMap();
		StatefulRedisModulesConnection<byte[], byte[]> connection = RedisModulesUtils.connection(redisClient,
				ByteArrayCodec.INSTANCE);
		RedisModulesCommands<byte[], byte[]> source = connection.sync();
		StatefulRedisModulesConnection<byte[], byte[]> targetConnection = RedisModulesUtils
				.connection(targetRedisClient, ByteArrayCodec.INSTANCE);
		RedisModulesCommands<byte[], byte[]> target = targetConnection.sync();
		source.sadd(setKey, setValue.toArray(new byte[0][]));
		target.sadd(setKey, setValue.toArray(new byte[0][]));
		source.hset(hashKey, hashValue);
		target.hset(hashKey, hashValue);
		source.lpush(listKey, listValue.toArray(new byte[0][]));
		target.lpush(listKey, listValue.toArray(new byte[0][]));
		source.zadd(zsetKey, zsetValue.toArray(new ScoredValue[0]));
		target.zadd(zsetKey, zsetValue.toArray(new ScoredValue[0]));
		byte[] streamKey = randomBytes();
		for (int index = 0; index < 10; index++) {
			Map<byte[], byte[]> body = byteArrayMap();
			String id = source.xadd(streamKey, body);
			XAddArgs args = new XAddArgs();
			args.id(id);
			target.xadd(streamKey, args, body);
		}
		KeyComparisonItemReader<byte[], byte[]> comparisonReader = comparisonReader(info, ByteArrayCodec.INSTANCE);
		((DefaultKeyComparator<byte[], byte[]>) comparisonReader.getComparator())
				.setTtlTolerance(Duration.ofMillis(100));
		comparisonReader.open(new ExecutionContext());
		KeyspaceComparison<byte[]> comparison = new KeyspaceComparison<>(readAll(comparisonReader));
		comparisonReader.close();
		Assertions.assertFalse(comparison.getAll().isEmpty());
		List<KeyComparison<byte[]>> mismatches = comparison.mismatches();
		Assertions.assertTrue(mismatches.isEmpty());
	}

	private Map<byte[], byte[]> byteArrayMap() {
		Map<byte[], byte[]> hash = new HashMap<>();
		int fieldCount = 10;
		for (int index = 0; index < fieldCount; index++) {
			hash.put(randomBytes(), randomBytes());
		}
		return hash;

	}

	private final Random random = new Random();

	private byte[] randomBytes() {
		byte[] bytes = new byte[10];
		random.nextBytes(bytes);
		return bytes;
	}

	private static final String JSON_BEER_1 = "[{\"id\":\"1\",\"brewery_id\":\"812\",\"name\":\"Hocus Pocus\",\"abv\":\"4.5\",\"ibu\":\"0\",\"srm\":\"0\",\"upc\":\"0\",\"filepath\":\"\",\"descript\":\"Our take on a classic summer ale.  A toast to weeds, rays, and summer haze.  A light, crisp ale for mowing lawns, hitting lazy fly balls, and communing with nature, Hocus Pocus is offered up as a summer sacrifice to clodless days.\\n\\nIts malty sweetness finishes tart and crisp and is best apprediated with a wedge of orange.\",\"add_user\":\"0\",\"last_mod\":\"2010-07-22 20:00:20 UTC\",\"style_name\":\"Light American Wheat Ale or Lager\",\"cat_name\":\"Other Style\"}]";
	private static final int BEER_COUNT = 1019;

	@Test
	void beerIndex() throws Exception {
		Beers.populateIndex(redisConnection);
		IndexInfo indexInfo = RedisModulesUtils.indexInfo(redisCommands.ftInfo(Beers.INDEX));
		Assertions.assertEquals(BEER_COUNT, indexInfo.getNumDocs());
	}

	@Test
	void readTimeseries(TestInfo info) throws Exception {
		String key = "myts";
		Sample[] samples = { Sample.of(System.currentTimeMillis(), 1.1),
				Sample.of(System.currentTimeMillis() + 10, 2.2) };
		for (Sample sample : samples) {
			redisCommands.tsAdd(key, sample);
		}
		RedisItemReader<String, String, Object> reader = structReader(info);
		reader.open(new ExecutionContext());
		KeyValue<String, Object> ds = reader.read();
		Assertions.assertEquals(key, ds.getKey());
		Assertions.assertEquals(DataType.TIMESERIES.getString(), ds.getType());
		Assertions.assertEquals(Arrays.asList(samples), ds.getValue());
		reader.close();
	}

	@Test
	void readTimeseriesByteArray(TestInfo info) throws Exception {
		String key = "myts";
		Sample[] samples = { Sample.of(System.currentTimeMillis(), 1.1),
				Sample.of(System.currentTimeMillis() + 10, 2.2) };
		for (Sample sample : samples) {
			redisCommands.tsAdd(key, sample);
		}
		RedisItemReader<byte[], byte[], Object> reader = structReader(info, ByteArrayCodec.INSTANCE);
		reader.open(new ExecutionContext());
		Function<String, byte[]> toByteArrayKeyFunction = BatchUtils.toByteArrayKeyFunction(StringCodec.UTF8);
		KeyValue<byte[], Object> ds = reader.read();
		Assertions.assertArrayEquals(toByteArrayKeyFunction.apply(key), ds.getKey());
		Assertions.assertEquals(DataType.TIMESERIES.getString(), ds.getType());
		Assertions.assertEquals(Arrays.asList(samples), ds.getValue());
		reader.close();
	}

	@Test
	void writeSug(TestInfo info) throws Exception {
		String key = "sugadd";
		List<Suggestion<String>> values = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			values.add(Suggestion.string("word" + index).score(index + 1).payload("payload" + index).build());
		}
		ListItemReader<Suggestion<String>> reader = new ListItemReader<>(values);

		Sugadd<String, String, Suggestion<String>> sugadd = new Sugadd<>(keyFunction(key), Function.identity());
		RedisItemWriter<String, String, Suggestion<String>> writer = writer(sugadd);
		run(info, reader, writer);
		assertEquals(1, redisCommands.dbsize());
		assertEquals(values.size(), redisCommands.ftSuglen(key));
	}

	@Test
	void writeSugIncr(TestInfo info) throws Exception {
		String key = "sugaddIncr";
		List<Suggestion<String>> values = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			values.add(Suggestion.string("word" + index).score(index + 1).payload("payload" + index).build());
		}
		ListItemReader<Suggestion<String>> reader = new ListItemReader<>(values);
		Sugadd<String, String, Suggestion<String>> sugadd = new Sugadd<>(keyFunction(key), Function.identity());
		sugadd.setIncr(true);
		RedisItemWriter<String, String, Suggestion<String>> writer = writer(sugadd);
		run(info, reader, writer);
		assertEquals(1, redisCommands.dbsize());
		assertEquals(values.size(), redisCommands.ftSuglen(key));
	}

	@Test
	void writeTimeseries(TestInfo info) throws Exception {
		String key = "ts";
		long now = Instant.now().toEpochMilli();
		List<Sample> samples = IntStream.range(0, 100).mapToObj(index -> Sample.of(now + index, index))
				.collect(Collectors.toList());
		ListItemReader<Collection<Sample>> reader = new ListItemReader<>(Arrays.asList(samples));
		TsAdd<String, String, Collection<Sample>> tsAdd = new TsAdd<>(keyFunction(key), Function.identity());
		RedisItemWriter<String, String, Collection<Sample>> writer = writer(tsAdd);
		run(info, reader, writer);
		assertEquals(1, redisCommands.dbsize());
	}

	@Test
	void writeJsonSet(TestInfo info) throws Exception {
		JsonSet<String, String, JsonNode> jsonSet = new JsonSet<>(n -> "beer:" + n.get("id").asText(),
				JsonNode::toString);
		jsonSet.setPath(".");
		RedisItemWriter<String, String, JsonNode> writer = writer(jsonSet);
		IteratorItemReader<JsonNode> reader = new IteratorItemReader<>(Beers.jsonNodeIterator());
		run(info, reader, writer);
		Assertions.assertEquals(BEER_COUNT, keyCount("beer:*"));
		Assertions.assertEquals(new ObjectMapper().readTree(JSON_BEER_1),
				new ObjectMapper().readTree(redisCommands.jsonGet("beer:1", "$")));
	}

	@Test
	void writeJsonDel(TestInfo info) throws Exception {
		GeneratorItemReader gen = generator(73, DataType.JSON);
		generate(info, gen);
		JsonDel<String, String, KeyValue<String, Object>> jsonDel = new JsonDel<>(KeyValue::getKey);
		RedisItemWriter<String, String, KeyValue<String, Object>> writer = writer(jsonDel);
		run(info, gen, writer);
		Assertions.assertEquals(0, redisCommands.dbsize());
	}

	@Test
	void writeTsAdd(TestInfo info) throws Exception {
		String key = "ts:1";
		Random random = new Random();
		int count = 100;
		List<Sample> samples = new ArrayList<>(count);
		for (int index = 0; index < count; index++) {
			long timestamp = System.currentTimeMillis() - count + (index % (count / 2));
			samples.add(Sample.of(timestamp, random.nextDouble()));
		}
		ListItemReader<Collection<Sample>> reader = new ListItemReader<>(Arrays.asList(samples));
		AddOptions<String, String> addOptions = AddOptions.<String, String>builder().policy(DuplicatePolicy.LAST)
				.build();
		TsAdd<String, String, Collection<Sample>> tsadd = new TsAdd<>(keyFunction(key), Function.identity());
		tsadd.setOptions(addOptions);
		RedisItemWriter<String, String, Collection<Sample>> writer = writer(tsadd);
		run(info, reader, writer);
		Assertions.assertEquals(count / 2,
				redisCommands.tsRange(key, TimeRange.unbounded(), RangeOptions.builder().build()).size(), 2);
	}

	@SuppressWarnings("unchecked")
	@Test
	void writeTsAddAll(TestInfo info) throws Exception {
		int count = 10;
		GeneratorItemReader reader = generator(count, DataType.TIMESERIES);
		AddOptions<String, String> addOptions = AddOptions.<String, String>builder().policy(DuplicatePolicy.LAST)
				.build();
		TsAdd<String, String, KeyValue<String, Object>> tsadd = new TsAdd<>(KeyValue::getKey,
				t -> (Collection<Sample>) t.getValue());
		tsadd.setOptions(addOptions);
		RedisItemWriter<String, String, KeyValue<String, Object>> writer = new RedisItemWriter<>(StringCodec.UTF8,
				tsadd);
		writer.setClient(redisClient);
		run(info, reader, writer);
		for (int index = 1; index <= count; index++) {
			Assertions.assertEquals(
					TimeSeriesOptions.DEFAULT_SAMPLE_COUNT.getMin(), redisCommands
							.tsRange(reader.key(index), TimeRange.unbounded(), RangeOptions.builder().build()).size(),
					2);
		}
	}

}
