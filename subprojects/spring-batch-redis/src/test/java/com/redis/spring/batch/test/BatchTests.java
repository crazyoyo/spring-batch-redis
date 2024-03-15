package com.redis.spring.batch.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.IteratorItemReader;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.test.context.junit4.SpringRunner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.Beers;
import com.redis.lettucemod.search.IndexInfo;
import com.redis.lettucemod.search.Suggestion;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.DuplicatePolicy;
import com.redis.lettucemod.timeseries.RangeOptions;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.lettucemod.timeseries.TimeRange;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyComparison;
import com.redis.spring.batch.common.KeyComparison.Status;
import com.redis.spring.batch.common.KeyComparisonItemReader;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Range;
import com.redis.spring.batch.common.ToGeoValueFunction;
import com.redis.spring.batch.common.ToSampleFunction;
import com.redis.spring.batch.common.ToSuggestionFunction;
import com.redis.spring.batch.common.ValueReader;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.gen.TimeSeriesOptions;
import com.redis.spring.batch.reader.DumpItemReader;
import com.redis.spring.batch.reader.KeyTypeItemReader;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader;
import com.redis.spring.batch.reader.ScanSizeEstimator;
import com.redis.spring.batch.reader.StreamItemReader;
import com.redis.spring.batch.reader.StreamItemReader.AckPolicy;
import com.redis.spring.batch.reader.StructItemReader;
import com.redis.spring.batch.step.FlushingStepBuilder;
import com.redis.spring.batch.util.CodecUtils;
import com.redis.spring.batch.writer.DumpItemWriter;
import com.redis.spring.batch.writer.OperationItemWriter;
import com.redis.spring.batch.writer.StructItemWriter;
import com.redis.spring.batch.writer.operation.Geoadd;
import com.redis.spring.batch.writer.operation.Hset;
import com.redis.spring.batch.writer.operation.JsonDel;
import com.redis.spring.batch.writer.operation.JsonSet;
import com.redis.spring.batch.writer.operation.Sugadd;
import com.redis.spring.batch.writer.operation.TsAdd;
import com.redis.spring.batch.writer.operation.TsAddAll;
import com.redis.spring.batch.writer.operation.Xadd;

import io.lettuce.core.Consumer;
import io.lettuce.core.GeoArgs;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.models.stream.PendingMessages;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
abstract class BatchTests extends AbstractTargetTestBase {

	@Test
	void compareSet(TestInfo info) throws Exception {
		commands.sadd("set:1", "value1", "value2");
		targetCommands.sadd("set:1", "value2", "value1");
		KeyComparisonItemReader reader = comparisonReader(info);
		reader.open(new ExecutionContext());
		List<KeyComparison> comparisons = readAll(reader);
		reader.close();
		Assertions.assertEquals(KeyComparison.Status.OK, comparisons.get(0).getStatus());
	}

	@Test
	void compareQuick(TestInfo info) throws Exception {
		int sourceCount = 100;
		for (int index = 1; index <= sourceCount; index++) {
			commands.set("key:" + index, "value:" + index);
		}
		int targetCount = 90;
		for (int index = 1; index <= targetCount; index++) {
			targetCommands.set("key:" + index, "value:" + index);
		}
		KeyTypeItemReader<String, String> source = RedisItemReader.type(client);
		KeyTypeItemReader<String, String> target = RedisItemReader.type(targetClient);
		KeyComparisonItemReader reader = new KeyComparisonItemReader(source, target);
		reader.setName(name(info));
		reader.open(new ExecutionContext());
		List<KeyComparison> comparisons = readAll(reader);
		reader.close();
		List<KeyComparison> missing = comparisons.stream().filter(c -> c.getStatus() == Status.MISSING)
				.collect(Collectors.toList());
		Assertions.assertEquals(sourceCount - targetCount, missing.size());
	}

	@Test
	void compareStreams(TestInfo info) throws Exception {
		GeneratorItemReader gen = generator(10);
		gen.setTypes(DataType.STREAM);
		generate(info, gen);
		StructItemReader<String, String> reader = structReader(info);
		StructItemWriter<String, String> writer = RedisItemWriter.struct(targetClient);
		replicate(info, reader, writer);
		KeyspaceComparison comparison = compare(info);
		Assertions.assertEquals(Collections.emptyList(), comparison.mismatches());
	}

	@Test
	void compareStatus(TestInfo info) throws Exception {
		GeneratorItemReader gen = generator(120);
		generate(info, gen);
		assertDbNotEmpty(commands);
		DumpItemReader reader = dumpReader(info);
		DumpItemWriter writer = RedisItemWriter.dump(targetClient);
		replicate(info, reader, writer);
		assertDbNotEmpty(targetCommands);
		long deleted = 0;
		for (int index = 0; index < 13; index++) {
			deleted += targetCommands.del(targetCommands.randomkey());
		}
		Set<String> ttlChanges = new HashSet<>();
		for (int index = 0; index < 23; index++) {
			String key = targetCommands.randomkey();
			if (key == null) {
				continue;
			}
			long ttl = targetCommands.ttl(key) + 12345;
			if (targetCommands.expire(key, ttl)) {
				ttlChanges.add(key);
			}
		}
		Set<String> typeChanges = new HashSet<>();
		Set<String> valueChanges = new HashSet<>();
		for (int index = 0; index < 17; index++) {
			assertDbNotEmpty(targetCommands);
			String key;
			do {
				key = targetCommands.randomkey();
			} while (key == null);
			DataType type = DataType.of(targetCommands.type(key));
			if (type == DataType.STRING) {
				if (!typeChanges.contains(key)) {
					valueChanges.add(key);
				}
				ttlChanges.remove(key);
			} else {
				typeChanges.add(key);
				valueChanges.remove(key);
				ttlChanges.remove(key);
			}
			targetCommands.set(key, "blah");
		}
		KeyComparisonItemReader comparator = comparisonReader(info);
		comparator.open(new ExecutionContext());
		List<KeyComparison> comparisons = readAll(comparator);
		comparator.close();
		long sourceCount = commands.dbsize();
		assertEquals(sourceCount, comparisons.size());
		assertEquals(sourceCount, targetCommands.dbsize() + deleted);
		List<KeyComparison> actualTypeChanges = comparisons.stream().filter(c -> c.getStatus() == Status.TYPE)
				.collect(Collectors.toList());
		assertEquals(typeChanges.size(), actualTypeChanges.size());
		assertEquals(valueChanges.size(), comparisons.stream().filter(c -> c.getStatus() == Status.VALUE).count());
		assertEquals(ttlChanges.size(), comparisons.stream().filter(c -> c.getStatus() == Status.TTL).count());
		assertEquals(deleted, comparisons.stream().filter(c -> c.getStatus() == Status.MISSING).count());
	}

	@Test
	void estimateScanSize(TestInfo info) throws Exception {
		GeneratorItemReader gen = generator(3000, DataType.HASH, DataType.STRING);
		generate(info, gen);
		long expectedCount = commands.dbsize();
		ScanSizeEstimator estimator = new ScanSizeEstimator(client);
		estimator.setScanMatch(GeneratorItemReader.DEFAULT_KEYSPACE + ":*");
		estimator.setSamples(300);
		assertEquals(expectedCount, estimator.call(), expectedCount / 10);
		estimator.setScanType(DataType.HASH.getString());
		assertEquals(expectedCount / 2, estimator.call(), expectedCount / 10);
	}

	@Test
	void readStruct(TestInfo info) throws Exception {
		generate(info, generator(73));
		StructItemReader<String, String> reader = structReader(info);
		reader.open(new ExecutionContext());
		List<KeyValue<String>> list = readAll(reader);
		reader.close();
		assertEquals(commands.dbsize(), list.size());
	}

	@Test
	void readStreamAutoAck(TestInfo info) throws InterruptedException {
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
		String id1 = commands.xadd(stream, body);
		String id2 = commands.xadd(stream, body);
		String id3 = commands.xadd(stream, body);
		List<StreamMessage<String, String>> messages = new ArrayList<>();
		awaitUntil(() -> messages.addAll(reader.readMessages()));
		Assertions.assertEquals(3, messages.size());
		assertStreamEquals(id1, body, stream, messages.get(0));
		assertStreamEquals(id2, body, stream, messages.get(1));
		assertStreamEquals(id3, body, stream, messages.get(2));
		reader.close();
		Assertions.assertEquals(0, commands.xpending(stream, consumerGroup).getCount(), "pending messages");
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
		String id1 = commands.xadd(stream, body);
		String id2 = commands.xadd(stream, body);
		String id3 = commands.xadd(stream, body);
		List<StreamMessage<String, String>> messages = new ArrayList<>();
		awaitUntil(() -> messages.addAll(reader.readMessages()));
		Assertions.assertEquals(3, messages.size());

		assertStreamEquals(id1, body, stream, messages.get(0));
		assertStreamEquals(id2, body, stream, messages.get(1));
		assertStreamEquals(id3, body, stream, messages.get(2));
		PendingMessages pendingMsgsBeforeCommit = commands.xpending(stream, consumerGroup);
		Assertions.assertEquals(3, pendingMsgsBeforeCommit.getCount(), "pending messages before commit");
		commands.xack(stream, consumerGroup, messages.get(0).getId(), messages.get(1).getId());
		PendingMessages pendingMsgsAfterCommit = commands.xpending(stream, consumerGroup);
		Assertions.assertEquals(1, pendingMsgsAfterCommit.getCount(), "pending messages after commit");
		reader.close();
	}

	@Test
	void readStreamManualAckRecover(TestInfo info) throws InterruptedException {
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
		commands.xadd(stream, body);
		commands.xadd(stream, body);
		commands.xadd(stream, body);
		List<StreamMessage<String, String>> messages = new ArrayList<>();
		awaitUntil(() -> messages.addAll(reader.readMessages()));
		Assertions.assertEquals(3, messages.size());

		List<StreamMessage<String, String>> recoveredMessages = new ArrayList<>();
		commands.xadd(stream, body);
		commands.xadd(stream, body);
		commands.xadd(stream, body);

		reader.close();

		final StreamItemReader<String, String> reader2 = streamReader(info, stream, consumer);
		reader2.setAckPolicy(AckPolicy.MANUAL);
		reader2.open(new ExecutionContext());

		awaitUntil(() -> recoveredMessages.addAll(reader2.readMessages()));
		awaitUntil(() -> !recoveredMessages.addAll(reader2.readMessages()));

		Assertions.assertEquals(6, recoveredMessages.size());
	}

	@Test
	void readStreamManualAckRecoverUncommitted(TestInfo info) throws InterruptedException {
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
		commands.xadd(stream, body);
		commands.xadd(stream, body);
		String id3 = commands.xadd(stream, body);
		List<StreamMessage<String, String>> messages = new ArrayList<>();
		awaitUntil(() -> messages.addAll(reader.readMessages()));
		Assertions.assertEquals(3, messages.size());
		commands.xack(stream, consumerGroup, messages.get(0).getId(), messages.get(1).getId());

		List<StreamMessage<String, String>> recoveredMessages = new ArrayList<>();
		String id4 = commands.xadd(stream, body);
		String id5 = commands.xadd(stream, body);
		String id6 = commands.xadd(stream, body);
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
		commands.xadd(stream, body);
		commands.xadd(stream, body);
		String id3 = commands.xadd(stream, body);
		List<StreamMessage<String, String>> sourceRecords = new ArrayList<>();
		awaitUntil(() -> sourceRecords.addAll(reader.readMessages()));
		Assertions.assertEquals(3, sourceRecords.size());

		List<StreamMessage<String, String>> recoveredRecords = new ArrayList<>();
		String id4 = commands.xadd(stream, body);
		String id5 = commands.xadd(stream, body);
		String id6 = commands.xadd(stream, body);

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
	void readStreamRecoverManualAckToAutoAck(TestInfo info) throws InterruptedException {
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
		commands.xadd(stream, body);
		commands.xadd(stream, body);
		commands.xadd(stream, body);
		List<StreamMessage<String, String>> sourceRecords = new ArrayList<>();
		awaitUntil(() -> sourceRecords.addAll(reader.readMessages()));
		Assertions.assertEquals(3, sourceRecords.size());

		List<StreamMessage<String, String>> recoveredRecords = new ArrayList<>();
		String id4 = commands.xadd(stream, body);
		String id5 = commands.xadd(stream, body);
		String id6 = commands.xadd(stream, body);
		reader.close();

		final StreamItemReader<String, String> reader2 = streamReader(info, stream, consumer);
		reader2.setAckPolicy(AckPolicy.AUTO);
		reader2.open(new ExecutionContext());

		// Wait until task.poll() doesn't return any more records
		awaitUntil(() -> recoveredRecords.addAll(reader2.readMessages()));
		awaitUntil(() -> !recoveredRecords.addAll(reader2.readMessages()));
		List<String> recoveredIds = recoveredRecords.stream().map(StreamMessage::getId).collect(Collectors.toList());
		Assertions.assertEquals(Arrays.asList(id4, id5, id6), recoveredIds, "recoveredIds");

		PendingMessages pending = commands.xpending(stream, consumerGroup);
		Assertions.assertEquals(0, pending.getCount(), "pending message count");
		reader2.close();
	}

	@Test
	void readStreamAck(TestInfo info) throws Exception {
		generateStreams(info, 57);
		List<String> keys = ScanIterator.scan(commands, KeyScanArgs.Builder.type(DataType.STREAM.getString())).stream()
				.collect(Collectors.toList());
		Consumer<String> consumer = Consumer.from("batchtests-readmessages", "consumer1");
		for (String key : keys) {
			long count = commands.xlen(key);
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
		List<String> keys = ScanIterator.scan(commands, KeyScanArgs.Builder.type(DataType.STREAM.getString())).stream()
				.collect(Collectors.toList());
		Consumer<String> consumer = Consumer.from("batchtests-readstreamjob", "consumer1");
		for (String key : keys) {
			long count = commands.xlen(key);
			StreamItemReader<String, String> reader = streamReader(info, key, consumer);
			reader.open(new ExecutionContext());
			List<StreamMessage<String, String>> messages = readAll(reader);
			reader.close();
			Assertions.assertEquals(count, messages.size());
			assertMessageBody(messages);
		}
	}

	@Test
	void readStructHash() throws Exception {
		String key = "myhash";
		Map<String, String> hash = new HashMap<>();
		hash.put("field1", "value1");
		hash.put("field2", "value2");
		commands.hset(key, hash);
		long ttl = System.currentTimeMillis() + 123456;
		commands.pexpireat(key, ttl);
		ValueReader<String, String, String, KeyValue<String>> reader = structValueReader();
		KeyValue<String> ds = reader.execute(key);
		Assertions.assertEquals(key, ds.getKey());
		Assertions.assertEquals(ttl, ds.getTtl());
		Assertions.assertEquals(DataType.HASH, ds.getType());
		Assertions.assertEquals(hash, ds.getValue());
		reader.close();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void readStructZset() throws Exception {
		String key = "myzset";
		ScoredValue[] values = { ScoredValue.just(123.456, "value1"), ScoredValue.just(654.321, "value2") };
		commands.zadd(key, values);
		ValueReader<String, String, String, KeyValue<String>> executor = structValueReader();
		KeyValue<String> ds = executor.execute(key);
		Assertions.assertEquals(key, ds.getKey());
		Assertions.assertEquals(DataType.ZSET, ds.getType());
		Assertions.assertEquals(new HashSet<>(Arrays.asList(values)), ds.getValue());
		executor.close();
	}

	@Test
	void readStructList() throws Exception {
		String key = "mylist";
		List<String> values = Arrays.asList("value1", "value2");
		commands.rpush(key, values.toArray(new String[0]));
		ValueReader<String, String, String, KeyValue<String>> executor = structValueReader();
		KeyValue<String> ds = executor.execute(key);
		Assertions.assertEquals(key, ds.getKey());
		Assertions.assertEquals(DataType.LIST, ds.getType());
		Assertions.assertEquals(values, ds.getValue());
		executor.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	void readStructStream() throws Exception {
		String key = "mystream";
		Map<String, String> body = new HashMap<>();
		body.put("field1", "value1");
		body.put("field2", "value2");
		commands.xadd(key, body);
		commands.xadd(key, body);
		ValueReader<String, String, String, KeyValue<String>> executor = structValueReader();
		KeyValue<String> ds = executor.execute(key);
		Assertions.assertEquals(key, ds.getKey());
		Assertions.assertEquals(DataType.STREAM, ds.getType());
		List<StreamMessage<String, String>> messages = (List<StreamMessage<String, String>>) ds.getValue();
		Assertions.assertEquals(2, messages.size());
		for (StreamMessage<String, String> message : messages) {
			Assertions.assertEquals(body, message.getBody());
			Assertions.assertNotNull(message.getId());
		}
		executor.close();
	}

	@Test
	void readDumpStream() throws Exception {
		String key = "mystream";
		Map<String, String> body = new HashMap<>();
		body.put("field1", "value1");
		body.put("field2", "value2");
		commands.xadd(key, body);
		commands.xadd(key, body);
		long ttl = System.currentTimeMillis() + 123456;
		commands.pexpireat(key, ttl);
		ValueReader<byte[], byte[], byte[], KeyValue<byte[]>> executor = dumpValueReader();
		KeyValue<byte[]> dump = executor.execute(toByteArray(key));
		Assertions.assertArrayEquals(toByteArray(key), dump.getKey());
		Assertions.assertTrue(Math.abs(ttl - dump.getTtl()) <= 3);
		commands.del(key);
		commands.restore(key, (byte[]) dump.getValue(), RestoreArgs.Builder.ttl(ttl).absttl());
		Assertions.assertEquals(DataType.STREAM.getString(), commands.type(key));
		executor.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	void readStructStreamByteArray() throws Exception {
		String key = "mystream";
		Map<String, String> body = new HashMap<>();
		body.put("field1", "value1");
		body.put("field2", "value2");
		commands.xadd(key, body);
		commands.xadd(key, body);
		ValueReader<byte[], byte[], byte[], KeyValue<byte[]>> executor = structValueReader(ByteArrayCodec.INSTANCE);
		KeyValue<byte[]> ds = executor.execute(toByteArray(key));
		Assertions.assertArrayEquals(toByteArray(key), ds.getKey());
		Assertions.assertEquals(DataType.STREAM, ds.getType());
		List<StreamMessage<byte[], byte[]>> messages = (List<StreamMessage<byte[], byte[]>>) ds.getValue();
		Assertions.assertEquals(2, messages.size());
		for (StreamMessage<byte[], byte[]> message : messages) {
			Map<byte[], byte[]> actual = message.getBody();
			Assertions.assertEquals(2, actual.size());
			Map<String, String> actualString = new HashMap<>();
			actual.forEach((k, v) -> actualString.put(toString(k), toString(v)));
			Assertions.assertEquals(body, actualString);
		}
		executor.close();
	}

	@Test
	void readStructHLL() throws Exception {
		String key1 = "hll:1";
		commands.pfadd(key1, "member:1", "member:2");
		String key2 = "hll:2";
		commands.pfadd(key2, "member:1", "member:2", "member:3");
		ValueReader<String, String, String, KeyValue<String>> executor = structValueReader();
		KeyValue<String> ds1 = executor.execute(key1);
		Assertions.assertEquals(key1, ds1.getKey());
		Assertions.assertEquals(DataType.STRING, ds1.getType());
		Assertions.assertEquals(commands.get(key1), ds1.getValue());
		executor.close();
	}

	@Test
	void replicateDump(TestInfo info) throws Exception {
		GeneratorItemReader gen = generator(100);
		generate(info, gen);
		replicate(info, dumpReader(info), RedisItemWriter.dump(targetClient));
	}

	@Test
	void replicateStructBinaryStrings(TestInfo info) throws Exception {
		StatefulRedisConnection<byte[], byte[]> rawConnection = RedisModulesUtils.connection(client,
				ByteArrayCodec.INSTANCE);
		rawConnection.setAutoFlushCommands(false);
		RedisAsyncCommands<byte[], byte[]> async = rawConnection.async();
		List<RedisFuture<?>> futures = new ArrayList<>();
		Random random = new Random();
		for (int index = 0; index < 100; index++) {
			String key = "binary:" + index;
			byte[] value = new byte[1000];
			random.nextBytes(value);
			futures.add(async.set(key.getBytes(), value));
		}
		rawConnection.flushCommands();
		LettuceFutures.awaitAll(rawConnection.getTimeout(), futures.toArray(new RedisFuture[0]));
		rawConnection.setAutoFlushCommands(true);
		StructItemReader<byte[], byte[]> reader = configure(info,
				RedisItemReader.struct(client, ByteArrayCodec.INSTANCE));
		StructItemWriter<byte[], byte[]> writer = RedisItemWriter.struct(targetClient, ByteArrayCodec.INSTANCE);
		replicate(info, reader, writer);
		rawConnection.close();
	}

	protected <K, V> void replicate(TestInfo info, RedisItemReader<K, V, KeyValue<K>> reader,
			RedisItemWriter<K, V, KeyValue<K>> writer) throws Exception {
		run(testInfo(info, "replicate"), reader, writer);
		awaitUntil(() -> reader.getJobExecution() == null || !reader.getJobExecution().isRunning());
		KeyspaceComparison comparison = compare(testInfo(info, "replicate"));
		Assertions.assertEquals(Collections.emptyList(), comparison.mismatches());
	}

	@Test
	void replicateStructEmptyCollections(TestInfo info) throws Exception {
		GeneratorItemReader gen = generator(123);
		Range cardinality = Range.of(0);
		gen.getHashOptions().setFieldCount(cardinality);
		gen.getSetOptions().setMemberCount(cardinality);
		gen.getStreamOptions().setMessageCount(cardinality);
		gen.getTimeSeriesOptions().setSampleCount(cardinality);
		gen.getZsetOptions().setMemberCount(cardinality);
		generate(info, gen);
		StructItemReader<String, String> reader = structReader(info);
		StructItemWriter<String, String> writer = RedisItemWriter.struct(targetClient);
		replicate(info, reader, writer);
	}

	public static <T> Function<T, String> keyFunction(String key) {
		return t -> key;
	}

	@Test
	void writeGeo(TestInfo info) throws Exception {
		ListItemReader<Geo> reader = new ListItemReader<>(
				Arrays.asList(new Geo("Venice Breakwater", -118.476056, 33.985728),
						new Geo("Long Beach National", -73.667022, 40.582739)));
		Geoadd<String, String, Geo> geoadd = new Geoadd<>(keyFunction("geoset"),
				new ToGeoValueFunction<>(Geo::getMember, Geo::getLongitude, Geo::getLatitude));
		OperationItemWriter<String, String, Geo> writer = writer(geoadd);
		run(info, reader, writer);
		Set<String> radius1 = commands.georadius("geoset", -118, 34, 100, GeoArgs.Unit.mi);
		assertEquals(1, radius1.size());
		assertTrue(radius1.contains("Venice Breakwater"));
	}

	@Test
	void writeWait(TestInfo info) throws Exception {
		List<Map<String, String>> maps = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			Map<String, String> body = new HashMap<>();
			body.put("id", String.valueOf(index));
			body.put("field1", "value1");
			body.put("field2", "value2");
			maps.add(body);
		}
		ListItemReader<Map<String, String>> reader = new ListItemReader<>(maps);
		Hset<String, String, Map<String, String>> hset = new Hset<>(m -> "hash:" + m.remove("id"), Function.identity());
		OperationItemWriter<String, String, Map<String, String>> writer = writer(hset);
		writer.setWaitReplicas(1);
		writer.setWaitTimeout(Duration.ofMillis(300));
		JobExecution execution = run(info, reader, writer);
		List<Throwable> exceptions = execution.getAllFailureExceptions();
		assertEquals("Insufficient replication level (0/1)", exceptions.get(0).getCause().getCause().getMessage());
	}

	@Test
	void writeStream(TestInfo info) throws Exception {
		String stream = "stream:0";
		List<Map<String, String>> messages = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			Map<String, String> body = new HashMap<>();
			body.put("field1", "value1");
			body.put("field2", "value2");
			messages.add(body);
		}
		ListItemReader<Map<String, String>> reader = new ListItemReader<>(messages);
		Xadd<String, String, Map<String, String>> xadd = new Xadd<>(keyFunction(stream), Function.identity());
		OperationItemWriter<String, String, Map<String, String>> writer = writer(xadd);
		run(info, reader, writer);
		Assertions.assertEquals(messages.size(), commands.xlen(stream));
		List<StreamMessage<String, String>> xrange = commands.xrange(stream, io.lettuce.core.Range.create("-", "+"));
		for (int index = 0; index < xrange.size(); index++) {
			StreamMessage<String, String> message = xrange.get(index);
			Assertions.assertEquals(messages.get(index), message.getBody());
		}
	}

	private <K, V, T extends KeyValue<K>> void replicateLive(TestInfo info, RedisItemReader<K, V, T> reader,
			RedisItemWriter<K, V, T> writer, RedisItemReader<K, V, T> liveReader, RedisItemWriter<K, V, T> liveWriter)
			throws Exception {
		live(liveReader);
		DataType[] types = new DataType[] { DataType.HASH, DataType.STRING };
		generate(info, generator(300, types));
		TaskletStep step = faultTolerant(step(new SimpleTestInfo(info, "step"), reader, writer)).build();
		SimpleFlow flow = new FlowBuilder<SimpleFlow>(name(new SimpleTestInfo(info, "snapshotFlow"))).start(step)
				.build();
		FlushingStepBuilder<T, T> flushingStepBuilder = flushingStep(new SimpleTestInfo(info, "liveStep"), liveReader,
				liveWriter);
		GeneratorItemReader liveGen = generator(700, types);
		liveGen.setExpiration(Range.of(100));
		liveGen.setKeyRange(Range.from(300));
		generateAsync(testInfo(info, "genasync"), liveGen);
		TaskletStep liveStep = faultTolerant(flushingStepBuilder).build();
		SimpleFlow liveFlow = new FlowBuilder<SimpleFlow>(name(new SimpleTestInfo(info, "liveFlow"))).start(liveStep)
				.build();
		Job job = job(info).start(new FlowBuilder<SimpleFlow>(name(new SimpleTestInfo(info, "flow")))
				.split(new SimpleAsyncTaskExecutor()).add(liveFlow, flow).build()).build().build();
		run(job);
		awaitUntil(() -> liveReader.getJobExecution() == null || !liveReader.getJobExecution().isRunning());
		awaitUntil(() -> reader.getJobExecution() == null || !reader.getJobExecution().isRunning());
		KeyspaceComparison comparison = compare(info);
		Assertions.assertEquals(Collections.emptyList(), comparison.mismatches());
	}

	@Test
	void readKeyspaceNotificationsDedupe(TestInfo info) throws Exception {
		enableKeyspaceNotifications(client);
		StructItemReader<String, String> reader = live(structReader(info));
		KeyspaceNotificationItemReader<String> keyReader = (KeyspaceNotificationItemReader<String>) reader.keyReader();
		keyReader.open(new ExecutionContext());
		try {
			String key = "key1";
			commands.zadd(key, 1, "member1");
			commands.zadd(key, 2, "member2");
			commands.zadd(key, 3, "member3");
			awaitUntil(() -> keyReader.getQueue().size() == 1);
			Assertions.assertEquals(key, keyReader.getQueue().take());
		} finally {
			keyReader.close();
		}
	}

	@Test
	void replicateDumpLive(TestInfo info) throws Exception {
		enableKeyspaceNotifications(client);
		DumpItemReader reader = dumpReader(info);
		DumpItemWriter writer = RedisItemWriter.dump(targetClient);
		DumpItemReader liveReader = dumpReader(info, "live-reader");
		DumpItemWriter liveWriter = RedisItemWriter.dump(targetClient);
		replicateLive(info, reader, writer, liveReader, liveWriter);
	}

	@Test
	void replicateStructLive(TestInfo info) throws Exception {
		enableKeyspaceNotifications(client);
		StructItemReader<String, String> reader = structReader(info);
		StructItemWriter<String, String> writer = RedisItemWriter.struct(targetClient);
		StructItemReader<String, String> liveReader = structReader(info, "live-reader");
		StructItemWriter<String, String> liveWriter = RedisItemWriter.struct(targetClient);
		replicateLive(info, reader, writer, liveReader, liveWriter);
	}

	@Test
	void replicateDumpLiveOnly(TestInfo info) throws Exception {
		enableKeyspaceNotifications(client);
		DumpItemReader reader = live(dumpReader(info));
		DumpItemWriter writer = RedisItemWriter.dump(targetClient);
		FlushingStepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> step = flushingStep(info, reader, writer);
		GeneratorItemReader gen = generator(100, DataType.HASH, DataType.LIST, DataType.SET, DataType.STRING,
				DataType.ZSET);
		generateAsync(testInfo(info, "genasync"), gen);
		run(info, step);
		Assertions.assertEquals(Collections.emptyList(), compare(info).mismatches());
	}

	@Test
	void replicateSetLiveOnly(TestInfo info) throws Exception {
		enableKeyspaceNotifications(client);
		String key = "myset";
		commands.sadd(key, "1", "2", "3", "4", "5");
		StructItemReader<String, String> reader = live(structReader(info));
		reader.setKeyspaceNotificationQueueCapacity(100);
		StructItemWriter<String, String> writer = RedisItemWriter.struct(targetClient);
		FlushingStepBuilder<KeyValue<String>, KeyValue<String>> step = flushingStep(info, reader, writer);
		Executors.newSingleThreadExecutor().execute(() -> {
			awaitPubSub();
			commands.srem(key, "5");
		});
		run(info, step);
		assertEquals(commands.smembers(key), targetCommands.smembers(key));
	}

	private static final String JSON_BEER_1 = "[{\"id\":\"1\",\"brewery_id\":\"812\",\"name\":\"Hocus Pocus\",\"abv\":\"4.5\",\"ibu\":\"0\",\"srm\":\"0\",\"upc\":\"0\",\"filepath\":\"\",\"descript\":\"Our take on a classic summer ale.  A toast to weeds, rays, and summer haze.  A light, crisp ale for mowing lawns, hitting lazy fly balls, and communing with nature, Hocus Pocus is offered up as a summer sacrifice to clodless days.\\n\\nIts malty sweetness finishes tart and crisp and is best apprediated with a wedge of orange.\",\"add_user\":\"0\",\"last_mod\":\"2010-07-22 20:00:20 UTC\",\"style_name\":\"Light American Wheat Ale or Lager\",\"cat_name\":\"Other Style\"}]";

	private static final int BEER_COUNT = 1019;

	@Test
	void beerIndex() throws Exception {
		Beers.populateIndex(connection);
		IndexInfo indexInfo = RedisModulesUtils.indexInfo(commands.ftInfo(Beers.INDEX));
		Assertions.assertEquals(BEER_COUNT, indexInfo.getNumDocs());
	}

	@Test
	void compareTimeseries(TestInfo info) throws Exception {
		int count = 123;
		for (int index = 0; index < count; index++) {
			commands.tsAdd("ts:" + index, Sample.of(123));
		}
		KeyspaceComparison comparisons = compare(info);
		Assertions.assertEquals(count, comparisons.get(Status.MISSING).size());
	}

	@Test
	void readTimeseries() throws Exception {
		String key = "myts";
		Sample[] samples = { Sample.of(System.currentTimeMillis(), 1.1),
				Sample.of(System.currentTimeMillis() + 10, 2.2) };
		for (Sample sample : samples) {
			commands.tsAdd(key, sample);
		}
		ValueReader<String, String, String, KeyValue<String>> executor = structValueReader();
		KeyValue<String> ds = executor.execute(key);
		Assertions.assertEquals(key, ds.getKey());
		Assertions.assertEquals(DataType.TIMESERIES, ds.getType());
		Assertions.assertEquals(Arrays.asList(samples), ds.getValue());
		executor.close();
	}

	@Test
	void readTimeseriesByteArray() throws Exception {
		String key = "myts";
		Sample[] samples = { Sample.of(System.currentTimeMillis(), 1.1),
				Sample.of(System.currentTimeMillis() + 10, 2.2) };
		for (Sample sample : samples) {
			commands.tsAdd(key, sample);
		}
		ValueReader<byte[], byte[], byte[], KeyValue<byte[]>> executor = structValueReader(ByteArrayCodec.INSTANCE);
		Function<String, byte[]> toByteArrayKeyFunction = CodecUtils.toByteArrayKeyFunction(CodecUtils.STRING_CODEC);
		KeyValue<byte[]> ds = executor.execute(toByteArrayKeyFunction.apply(key));
		Assertions.assertArrayEquals(toByteArrayKeyFunction.apply(key), ds.getKey());
		Assertions.assertEquals(DataType.TIMESERIES, ds.getType());
		Assertions.assertEquals(Arrays.asList(samples), ds.getValue());
		executor.close();
	}

	@Test
	void writeSug(TestInfo info) throws Exception {
		String key = "sugadd";
		List<Suggestion<String>> values = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			values.add(Suggestion.string("word" + index).score(index + 1).payload("payload" + index).build());
		}
		ListItemReader<Suggestion<String>> reader = new ListItemReader<>(values);
		Sugadd<String, String, Suggestion<String>> sugadd = new Sugadd<>(keyFunction(key),
				new ToSuggestionFunction<>(Suggestion::getString, Suggestion::getScore, Suggestion::getPayload));
		OperationItemWriter<String, String, Suggestion<String>> writer = writer(sugadd);
		run(info, reader, writer);
		assertEquals(1, commands.dbsize());
		assertEquals(values.size(), commands.ftSuglen(key));
	}

	@Test
	void writeSugIncr(TestInfo info) throws Exception {
		String key = "sugaddIncr";
		List<Suggestion<String>> values = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			values.add(Suggestion.string("word" + index).score(index + 1).payload("payload" + index).build());
		}
		ListItemReader<Suggestion<String>> reader = new ListItemReader<>(values);
		ToSuggestionFunction<String, Suggestion<String>> converter = new ToSuggestionFunction<>(Suggestion::getString,
				Suggestion::getScore, Suggestion::getPayload);
		Sugadd<String, String, Suggestion<String>> sugadd = new Sugadd<>(keyFunction(key), converter);
		sugadd.setIncr(true);
		OperationItemWriter<String, String, Suggestion<String>> writer = writer(sugadd);
		run(info, reader, writer);
		assertEquals(1, commands.dbsize());
		assertEquals(values.size(), commands.ftSuglen(key));
	}

	@Test
	void writeTimeseries(TestInfo info) throws Exception {
		String key = "ts";
		Map<Long, Double> samples = new HashMap<>();
		for (int index = 0; index < 100; index++) {
			samples.put(Instant.now().toEpochMilli() + index, (double) index);
		}
		TsAdd<String, String, Entry<Long, Double>> tsAdd = new TsAdd<>(keyFunction(key),
				new ToSampleFunction<>(e -> e.getKey(), e -> e.getValue()));
		ListItemReader<Entry<Long, Double>> reader = new ListItemReader<>(new ArrayList<>(samples.entrySet()));
		OperationItemWriter<String, String, Entry<Long, Double>> writer = writer(tsAdd);
		run(info, reader, writer);
		assertEquals(1, commands.dbsize());
	}

	@Test
	void writeJsonSet(TestInfo info) throws Exception {
		JsonSet<String, String, JsonNode> jsonSet = new JsonSet<>(n -> "beer:" + n.get("id").asText(),
				JsonNode::toString);
		jsonSet.setPath(".");
		OperationItemWriter<String, String, JsonNode> writer = writer(jsonSet);
		IteratorItemReader<JsonNode> reader = new IteratorItemReader<>(Beers.jsonNodeIterator());
		run(info, reader, writer);
		Assertions.assertEquals(BEER_COUNT, keyCount("beer:*"));
		Assertions.assertEquals(new ObjectMapper().readTree(JSON_BEER_1),
				new ObjectMapper().readTree(commands.jsonGet("beer:1", "$")));
	}

	@Test
	void writeJsonDel(TestInfo info) throws Exception {
		GeneratorItemReader gen = generator(73, DataType.JSON);
		generate(info, gen);
		JsonDel<String, String, KeyValue<String>> jsonDel = new JsonDel<>(KeyValue::getKey);
		OperationItemWriter<String, String, KeyValue<String>> writer = writer(jsonDel);
		run(info, gen, writer);
		Assertions.assertEquals(0, commands.dbsize());
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
		ListItemReader<Sample> reader = new ListItemReader<>(samples);
		AddOptions<String, String> addOptions = AddOptions.<String, String>builder().policy(DuplicatePolicy.LAST)
				.build();
		TsAdd<String, String, Sample> tsadd = new TsAdd<>(keyFunction(key), Function.identity());
		tsadd.setOptions(addOptions);
		OperationItemWriter<String, String, Sample> writer = writer(tsadd);
		run(info, reader, writer);
		Assertions.assertEquals(count / 2,
				commands.tsRange(key, TimeRange.unbounded(), RangeOptions.builder().build()).size(), 2);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void writeTsAddAll(TestInfo info) throws Exception {
		int count = 10;
		GeneratorItemReader reader = generator(count, DataType.TIMESERIES);
		AddOptions<String, String> addOptions = AddOptions.<String, String>builder().policy(DuplicatePolicy.LAST)
				.build();
		TsAddAll<String, String, KeyValue<String>> tsadd = new TsAddAll<>();
		tsadd.setKeyFunction(KeyValue::getKey);
		tsadd.setSamplesFunction(t -> (Collection<Sample>) t.getValue());
		tsadd.setOptions(addOptions);
		OperationItemWriter<String, String, Sample> writer = new OperationItemWriter(client, CodecUtils.STRING_CODEC,
				tsadd);
		run(info, reader, writer);
		for (int index = 1; index <= count; index++) {
			Assertions.assertEquals(TimeSeriesOptions.DEFAULT_SAMPLE_COUNT.getMin(),
					commands.tsRange(reader.key(index), TimeRange.unbounded(), RangeOptions.builder().build()).size(),
					2);
		}
	}
}
