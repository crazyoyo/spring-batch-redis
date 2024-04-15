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
import java.util.stream.IntStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
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
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.search.IndexInfo;
import com.redis.lettucemod.search.Suggestion;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.DuplicatePolicy;
import com.redis.lettucemod.timeseries.RangeOptions;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.lettucemod.timeseries.TimeRange;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.KeyValue.Type;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.ValueType;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.FlushingStepBuilder;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.gen.Item;
import com.redis.spring.batch.gen.Range;
import com.redis.spring.batch.gen.TimeSeriesOptions;
import com.redis.spring.batch.operation.CompositeOperation;
import com.redis.spring.batch.operation.Evalsha;
import com.redis.spring.batch.operation.Geoadd;
import com.redis.spring.batch.operation.Hset;
import com.redis.spring.batch.operation.JsonDel;
import com.redis.spring.batch.operation.JsonSet;
import com.redis.spring.batch.operation.Operation;
import com.redis.spring.batch.operation.OperationExecutor;
import com.redis.spring.batch.operation.Sugadd;
import com.redis.spring.batch.operation.TsAdd;
import com.redis.spring.batch.operation.TsAddAll;
import com.redis.spring.batch.operation.Xadd;
import com.redis.spring.batch.reader.EvalFunction;
import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.spring.batch.reader.KeyComparisonItemReader;
import com.redis.spring.batch.reader.KeyNotificationItemReader;
import com.redis.spring.batch.reader.ScanSizeEstimator;
import com.redis.spring.batch.reader.StreamItemReader;
import com.redis.spring.batch.reader.StreamItemReader.AckPolicy;
import com.redis.spring.batch.util.BatchUtils;
import com.redis.spring.batch.util.ToGeoValueFunction;
import com.redis.spring.batch.util.ToSampleFunction;
import com.redis.spring.batch.util.ToSuggestionFunction;

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
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.models.stream.PendingMessages;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
abstract class BatchTests extends AbstractTargetTestBase {

	@Test
	void compareSet(TestInfo info) throws Exception {
		redisCommands.sadd("set:1", "value1", "value2");
		targetRedisCommands.sadd("set:1", "value2", "value1");
		KeyComparisonItemReader reader = comparisonReader(info);
		reader.open(new ExecutionContext());
		List<KeyComparison> comparisons = readAll(reader);
		reader.close();
		Assertions.assertEquals(KeyComparison.Status.OK, comparisons.get(0).getStatus());
	}

	@Test
	void evalsha() throws Exception {
		String key = "key:1";
		String value = "value1";
		redisCommands.set(key, value);
		String file = BatchUtils.readFile("keyvalue.lua");
		String digest = redisCommands.scriptLoad(file);
		Evalsha<String, String, String> evalsha = new Evalsha<>(digest, StringCodec.UTF8, Function.identity());
		evalsha.setArgs(ValueType.STRUCT.name().toLowerCase(), 0, 5);
		List<RedisFuture<List<Object>>> futures = new ArrayList<>();
		RedisModulesAsyncCommands<String, String> asyncCommands = BatchUtils.connection(redisClient).async();
		evalsha.execute(asyncCommands, Arrays.asList(key), futures);
		List<Object> result = OperationExecutor.getAll(redisConnection.getTimeout(), futures).get(0);
		Assertions.assertEquals(5, result.size());
		Assertions.assertEquals(key, result.get(0));
		Assertions.assertEquals(-1, (Long) result.get(1));
		Assertions.assertEquals(Type.STRING.getCode(), result.get(2));
		Assertions.assertEquals(0, (Long) result.get(3));
		Assertions.assertEquals(value, result.get(4));
		futures.clear();
		evalsha.setArgs(ValueType.TYPE.name().toLowerCase(), 0, 5);
		evalsha.execute(asyncCommands, Arrays.asList(key), futures);
		result = OperationExecutor.getAll(redisConnection.getTimeout(), futures).get(0);
		Assertions.assertEquals(4, result.size());
		Assertions.assertEquals(key, result.get(0));
		Assertions.assertEquals(-1, (Long) result.get(1));
		Assertions.assertEquals(Type.STRING.getCode(), result.get(2));
		Assertions.assertEquals(0, (Long) result.get(3));
		futures.clear();
		evalsha.setArgs(ValueType.STRUCT.name().toLowerCase(), -1, 5);
		evalsha.execute(asyncCommands, Arrays.asList(key), futures);
		result = OperationExecutor.getAll(redisConnection.getTimeout(), futures).get(0);
		Assertions.assertEquals(5, result.size());
		Assertions.assertEquals(key, result.get(0));
		Assertions.assertEquals(-1, (Long) result.get(1));
		Assertions.assertEquals(Type.STRING.getCode(), result.get(2));
		Assertions.assertEquals(88, (Long) result.get(3));
		Assertions.assertEquals(value, result.get(4));
		futures.clear();
		Instant expireAt = Instant.now().plusSeconds(3600);
		redisCommands.expireat(key, expireAt);
		evalsha.setArgs(ValueType.STRUCT.name().toLowerCase(), -1, 5);
		evalsha.execute(asyncCommands, Arrays.asList(key), futures);
		result = OperationExecutor.getAll(redisConnection.getTimeout(), futures).get(0);
		Assertions.assertEquals(5, result.size());
		Assertions.assertEquals(key, result.get(0));
		Assertions.assertEquals(expireAt.toEpochMilli(), (Long) result.get(1), 1000);
		Assertions.assertEquals(Type.STRING.getCode(), result.get(2));
		Assertions.assertEquals(88, (Long) result.get(3));
		Assertions.assertEquals(value, result.get(4));
	}

	@Test
	void evalFunction() throws Exception {
		String key = "key:1";
		String value = "value1";
		redisCommands.set(key, value);
		String file = BatchUtils.readFile("keyvalue.lua");
		String digest = redisCommands.scriptLoad(file);
		Evalsha<String, String, String> evalsha = new Evalsha<>(digest, StringCodec.UTF8, Function.identity());
		evalsha.setArgs(ValueType.STRUCT.name().toLowerCase(), 0, 5);
		List<RedisFuture<List<Object>>> futures = new ArrayList<>();
		RedisModulesAsyncCommands<String, String> asyncCommands = BatchUtils.connection(redisClient).async();
		evalsha.execute(asyncCommands, Arrays.asList(key), futures);
		EvalFunction<String, String> function = new EvalFunction<>(StringCodec.UTF8);
		KeyValue<String> keyValue = function
				.apply(OperationExecutor.getAll(redisConnection.getTimeout(), futures).get(0));
		Assertions.assertEquals(key, keyValue.getKey());
		Assertions.assertEquals(-1, keyValue.getTtl());
		Assertions.assertEquals(Type.STRING, keyValue.getType());
		Assertions.assertEquals(0, keyValue.getMemoryUsage());
		Assertions.assertEquals(value, keyValue.getValue());
		futures.clear();
		evalsha.setArgs(ValueType.TYPE.name().toLowerCase(), 0, 5);
		evalsha.execute(asyncCommands, Arrays.asList(key), futures);
		keyValue = function.apply(OperationExecutor.getAll(redisConnection.getTimeout(), futures).get(0));
		Assertions.assertEquals(key, keyValue.getKey());
		Assertions.assertEquals(-1, keyValue.getTtl());
		Assertions.assertEquals(Type.STRING, keyValue.getType());
		Assertions.assertEquals(0, keyValue.getMemoryUsage());
		futures.clear();
		evalsha.setArgs(ValueType.STRUCT.name().toLowerCase(), -1, 5);
		evalsha.execute(asyncCommands, Arrays.asList(key), futures);
		keyValue = function.apply(OperationExecutor.getAll(redisConnection.getTimeout(), futures).get(0));
		Assertions.assertEquals(key, keyValue.getKey());
		Assertions.assertEquals(-1, keyValue.getTtl());
		Assertions.assertEquals(Type.STRING, keyValue.getType());
		Assertions.assertEquals(100, keyValue.getMemoryUsage(), 50);
		Assertions.assertEquals(value, keyValue.getValue());
		futures.clear();
		Instant expireAt = Instant.now().plusSeconds(3600);
		redisCommands.expireat(key, expireAt);
		evalsha.setArgs(ValueType.STRUCT.name().toLowerCase(), -1, 5);
		evalsha.execute(asyncCommands, Arrays.asList(key), futures);
		keyValue = function.apply(OperationExecutor.getAll(redisConnection.getTimeout(), futures).get(0));
		Assertions.assertEquals(key, keyValue.getKey());
		Assertions.assertEquals(expireAt.toEpochMilli(), keyValue.getTtl(), 1000);
		Assertions.assertEquals(Type.STRING, keyValue.getType());
		Assertions.assertEquals(100, keyValue.getMemoryUsage(), 50);
		Assertions.assertEquals(value, keyValue.getValue());
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
		RedisItemReader<String, String> source = RedisItemReader.type();
		source.setClient(redisClient);
		RedisItemReader<String, String> target = RedisItemReader.type();
		target.setClient(targetRedisClient);
		KeyComparisonItemReader reader = comparisonReader(info, source, target);
		reader.open(new ExecutionContext());
		List<KeyComparison> comparisons = readAll(reader);
		reader.close();
		KeyspaceComparison keyspaceComparison = new KeyspaceComparison(comparisons);
		Assertions.assertEquals(sourceCount - targetCount, keyspaceComparison.get(Status.MISSING).size());
	}

	@Test
	void compareStreams(TestInfo info) throws Exception {
		GeneratorItemReader gen = generator(10);
		gen.setTypes(Item.Type.STREAM);
		generate(info, gen);
		RedisItemReader<String, String> reader = structReader(info);
		RedisItemWriter<String, String, KeyValue<String>> writer = RedisItemWriter.struct();
		writer.setClient(targetRedisClient);
		replicate(info, reader, writer);
		KeyspaceComparison comparison = compare(info);
		Assertions.assertEquals(Collections.emptyList(), comparison.mismatches());
	}

	@Test
	void compareStatus(TestInfo info) throws Exception {
		GeneratorItemReader gen = generator(120);
		generate(info, gen);
		assertDbNotEmpty(redisCommands);
		RedisItemReader<byte[], byte[]> reader = dumpReader(info);
		RedisItemWriter<byte[], byte[], KeyValue<byte[]>> writer = RedisItemWriter.dump();
		writer.setClient(targetRedisClient);
		replicate(info, reader, writer);
		assertDbNotEmpty(targetRedisCommands);
		long deleted = 0;
		for (int index = 0; index < 13; index++) {
			deleted += targetRedisCommands.del(targetRedisCommands.randomkey());
		}
		Set<String> ttlChanges = new HashSet<>();
		for (int index = 0; index < 23; index++) {
			String key = targetRedisCommands.randomkey();
			if (key == null) {
				continue;
			}
			long ttl = targetRedisCommands.ttl(key) + 12345;
			if (targetRedisCommands.expire(key, ttl)) {
				ttlChanges.add(key);
			}
		}
		Set<String> typeChanges = new HashSet<>();
		Set<String> valueChanges = new HashSet<>();
		for (int index = 0; index < 17; index++) {
			assertDbNotEmpty(targetRedisCommands);
			String key;
			do {
				key = targetRedisCommands.randomkey();
			} while (key == null);
			Type type = Type.of(targetRedisCommands.type(key));
			if (type == Type.STRING) {
				if (!typeChanges.contains(key)) {
					valueChanges.add(key);
				}
				ttlChanges.remove(key);
			} else {
				typeChanges.add(key);
				valueChanges.remove(key);
				ttlChanges.remove(key);
			}
			targetRedisCommands.set(key, "blah");
		}
		KeyComparisonItemReader comparator = comparisonReader(info);
		comparator.open(new ExecutionContext());
		List<KeyComparison> comparisons = readAll(comparator);
		comparator.close();
		long sourceCount = redisCommands.dbsize();
		assertEquals(sourceCount, comparisons.size());
		assertEquals(sourceCount, targetRedisCommands.dbsize() + deleted);
		List<KeyComparison> actualTypeChanges = comparisons.stream().filter(c -> c.getStatus() == Status.TYPE)
				.collect(Collectors.toList());
		assertEquals(typeChanges.size(), actualTypeChanges.size());
		assertEquals(valueChanges.size(), comparisons.stream().filter(c -> c.getStatus() == Status.VALUE).count());
		assertEquals(ttlChanges.size(), comparisons.stream().filter(c -> c.getStatus() == Status.TTL).count());
		assertEquals(deleted, comparisons.stream().filter(c -> c.getStatus() == Status.MISSING).count());
	}

	@Test
	void estimateScanSize(TestInfo info) throws Exception {
		GeneratorItemReader gen = generator(3000, Item.Type.HASH, Item.Type.STRING);
		generate(info, gen);
		long expectedCount = redisCommands.dbsize();
		ScanSizeEstimator estimator = new ScanSizeEstimator(redisClient);
		estimator.setKeyPattern(GeneratorItemReader.DEFAULT_KEYSPACE + ":*");
		estimator.setSamples(300);
		assertEquals(expectedCount, estimator.getAsLong(), expectedCount / 10);
		estimator.setKeyType(Type.HASH.getCode());
		assertEquals(expectedCount / 2, estimator.getAsLong(), expectedCount / 10);
	}

	@Test
	void readStruct(TestInfo info) throws Exception {
		generate(info, generator(73));
		RedisItemReader<String, String> reader = structReader(info);
		reader.open(new ExecutionContext());
		List<KeyValue<String>> list = readAll(reader);
		reader.close();
		assertEquals(redisCommands.dbsize(), list.size());
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
		List<String> keys = ScanIterator.scan(redisCommands, KeyScanArgs.Builder.type(Type.STREAM.getCode())).stream()
				.collect(Collectors.toList());
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
		List<String> keys = ScanIterator.scan(redisCommands, KeyScanArgs.Builder.type(Type.STREAM.getCode())).stream()
				.collect(Collectors.toList());
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
		OperationExecutor<String, String, String, KeyValue<String>> reader = structOperationExecutor(info);
		KeyValue<String> ds = reader.apply(Arrays.asList(key)).get(0);
		Assertions.assertEquals(key, ds.getKey());
		Assertions.assertEquals(ttl, ds.getTtl());
		Assertions.assertEquals(Type.HASH, ds.getType());
		Assertions.assertEquals(hash, ds.getValue());
		reader.close();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void readStructZset(TestInfo info) throws Exception {
		String key = "myzset";
		ScoredValue[] values = { ScoredValue.just(123.456, "value1"), ScoredValue.just(654.321, "value2") };
		redisCommands.zadd(key, values);
		OperationExecutor<String, String, String, KeyValue<String>> executor = structOperationExecutor(info);
		KeyValue<String> ds = executor.apply(Arrays.asList(key)).get(0);
		Assertions.assertEquals(key, ds.getKey());
		Assertions.assertEquals(Type.ZSET, ds.getType());
		Assertions.assertEquals(new HashSet<>(Arrays.asList(values)), ds.getValue());
		executor.close();
	}

	@Test
	void readStructList(TestInfo info) throws Exception {
		String key = "mylist";
		List<String> values = Arrays.asList("value1", "value2");
		redisCommands.rpush(key, values.toArray(new String[0]));
		OperationExecutor<String, String, String, KeyValue<String>> executor = structOperationExecutor(info);
		KeyValue<String> ds = executor.apply(Arrays.asList(key)).get(0);
		Assertions.assertEquals(key, ds.getKey());
		Assertions.assertEquals(Type.LIST, ds.getType());
		Assertions.assertEquals(values, ds.getValue());
		executor.close();
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
		OperationExecutor<String, String, String, KeyValue<String>> executor = structOperationExecutor(info);
		KeyValue<String> ds = executor.apply(Arrays.asList(key)).get(0);
		Assertions.assertEquals(key, ds.getKey());
		Assertions.assertEquals(Type.STREAM, ds.getType());
		List<StreamMessage<String, String>> messages = (List<StreamMessage<String, String>>) ds.getValue();
		Assertions.assertEquals(2, messages.size());
		for (StreamMessage<String, String> message : messages) {
			Assertions.assertEquals(body, message.getBody());
			Assertions.assertNotNull(message.getId());
		}
		executor.close();
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
		OperationExecutor<byte[], byte[], byte[], KeyValue<byte[]>> executor = dumpValueReader(info);
		KeyValue<byte[]> dump = executor.apply(Arrays.asList(toByteArray(key))).get(0);
		Assertions.assertArrayEquals(toByteArray(key), dump.getKey());
		Assertions.assertTrue(Math.abs(ttl - dump.getTtl()) <= 3);
		redisCommands.del(key);
		redisCommands.restore(key, (byte[]) dump.getValue(), RestoreArgs.Builder.ttl(ttl).absttl());
		Assertions.assertEquals(Type.STREAM.getCode(), redisCommands.type(key));
		executor.close();
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
		OperationExecutor<byte[], byte[], byte[], KeyValue<byte[]>> executor = structOperationExecutor(info,
				ByteArrayCodec.INSTANCE);
		KeyValue<byte[]> ds = executor.apply(Arrays.asList(toByteArray(key))).get(0);
		Assertions.assertArrayEquals(toByteArray(key), ds.getKey());
		Assertions.assertEquals(Type.STREAM, ds.getType());
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
	void readStructHLL(TestInfo info) throws Exception {
		String key1 = "hll:1";
		redisCommands.pfadd(key1, "member:1", "member:2");
		String key2 = "hll:2";
		redisCommands.pfadd(key2, "member:1", "member:2", "member:3");
		OperationExecutor<String, String, String, KeyValue<String>> executor = structOperationExecutor(info);
		KeyValue<String> ds1 = executor.apply(Arrays.asList(key1)).get(0);
		Assertions.assertEquals(key1, ds1.getKey());
		Assertions.assertEquals(Type.STRING, ds1.getType());
		Assertions.assertEquals(redisCommands.get(key1), ds1.getValue());
		executor.close();
	}

	@Test
	void replicateDump(TestInfo info) throws Exception {
		GeneratorItemReader gen = generator(100);
		generate(info, gen);
		RedisItemWriter<byte[], byte[], KeyValue<byte[]>> writer = RedisItemWriter.dump();
		writer.setClient(targetRedisClient);
		replicate(info, dumpReader(info), writer);
	}

	@Test
	void replicateStructBinaryStrings(TestInfo info) throws Exception {
		StatefulRedisConnection<byte[], byte[]> rawConnection = RedisModulesUtils.connection(redisClient,
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
		RedisItemReader<byte[], byte[]> reader = RedisItemReader.struct(ByteArrayCodec.INSTANCE);
		configure(info, reader);
		reader.setClient(redisClient);
		RedisItemWriter<byte[], byte[], KeyValue<byte[]>> writer = RedisItemWriter.struct(ByteArrayCodec.INSTANCE);
		writer.setClient(targetRedisClient);
		replicate(info, reader, writer);
		rawConnection.close();
	}

	protected <K, V> void replicate(TestInfo info, RedisItemReader<K, V> reader,
			RedisItemWriter<K, V, KeyValue<K>> writer) throws Exception {
		run(testInfo(info, "replicate"), reader, writer);
		awaitUntilFalse(reader::isRunning);
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
		RedisItemReader<String, String> reader = structReader(info);
		RedisItemWriter<String, String, KeyValue<String>> writer = RedisItemWriter.struct();
		writer.setClient(targetRedisClient);
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
		RedisItemWriter<String, String, Geo> writer = writer(geoadd);
		run(info, reader, writer);
		Set<String> radius1 = redisCommands.georadius("geoset", -118, 34, 100, GeoArgs.Unit.mi);
		assertEquals(1, radius1.size());
		assertTrue(radius1.contains("Venice Breakwater"));
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
		RedisItemWriter<String, String, Map<String, String>> writer = writer(hset);
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
		RedisItemWriter<String, String, Map<String, String>> writer = writer(xadd);
		run(info, reader, writer);
		Assertions.assertEquals(messages.size(), redisCommands.xlen(stream));
		List<StreamMessage<String, String>> xrange = redisCommands.xrange(stream,
				io.lettuce.core.Range.create("-", "+"));
		for (int index = 0; index < xrange.size(); index++) {
			StreamMessage<String, String> message = xrange.get(index);
			Assertions.assertEquals(messages.get(index), message.getBody());
		}
	}

	private <K, V> void replicateLive(TestInfo info, RedisItemReader<K, V> reader,
			RedisItemWriter<K, V, KeyValue<K>> writer, RedisItemReader<K, V> liveReader,
			RedisItemWriter<K, V, KeyValue<K>> liveWriter) throws Exception {
		live(liveReader);
		Item.Type[] types = new Item.Type[] { Item.Type.HASH, Item.Type.STRING };
		generate(info, generator(300, types));
		TaskletStep step = faultTolerant(step(new SimpleTestInfo(info, "step"), reader, writer)).build();
		SimpleFlow flow = new FlowBuilder<SimpleFlow>(name(new SimpleTestInfo(info, "snapshotFlow"))).start(step)
				.build();
		FlushingStepBuilder<KeyValue<K>, KeyValue<K>> flushingStepBuilder = flushingStep(
				new SimpleTestInfo(info, "liveStep"), liveReader, liveWriter);
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
		awaitUntilFalse(liveReader::isRunning);
		awaitUntilFalse(reader::isRunning);
		KeyspaceComparison comparison = compare(info);
		Assertions.assertEquals(Collections.emptyList(), comparison.mismatches());
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
			Assertions.assertEquals(key, keyReader.getQueue().take().getKey());
		} finally {
			keyReader.close();
		}
	}

	@Test
	void replicateDumpLive(TestInfo info) throws Exception {
		enableKeyspaceNotifications();
		RedisItemReader<byte[], byte[]> reader = dumpReader(info);
		RedisItemWriter<byte[], byte[], KeyValue<byte[]>> writer = RedisItemWriter.dump();
		writer.setClient(targetRedisClient);
		RedisItemReader<byte[], byte[]> liveReader = dumpReader(info, "live");
		RedisItemWriter<byte[], byte[], KeyValue<byte[]>> liveWriter = RedisItemWriter.dump();
		liveWriter.setClient(targetRedisClient);
		replicateLive(info, reader, writer, liveReader, liveWriter);
	}

	@Test
	void replicateStructLive(TestInfo info) throws Exception {
		enableKeyspaceNotifications();
		RedisItemReader<String, String> reader = structReader(info);
		RedisItemWriter<String, String, KeyValue<String>> writer = RedisItemWriter.struct();
		writer.setClient(targetRedisClient);
		RedisItemReader<String, String> liveReader = structReader(info, "live");
		RedisItemWriter<String, String, KeyValue<String>> liveWriter = RedisItemWriter.struct();
		liveWriter.setClient(targetRedisClient);
		replicateLive(info, reader, writer, liveReader, liveWriter);
	}

	@Test
	void replicateDumpLiveOnly(TestInfo info) throws Exception {
		enableKeyspaceNotifications();
		RedisItemReader<byte[], byte[]> reader = dumpReader(info);
		live(reader);
		RedisItemWriter<byte[], byte[], KeyValue<byte[]>> writer = RedisItemWriter.dump();
		writer.setClient(targetRedisClient);
		FlushingStepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> step = flushingStep(info, reader, writer);
		GeneratorItemReader gen = generator(100, Item.Type.HASH, Item.Type.LIST, Item.Type.SET, Item.Type.STRING,
				Item.Type.ZSET);
		generateAsync(testInfo(info, "genasync"), gen);
		run(info, step);
		Assertions.assertEquals(Collections.emptyList(), compare(info).mismatches());
	}

	@Test
	void replicateSetLiveOnly(TestInfo info) throws Exception {
		enableKeyspaceNotifications();
		String key = "myset";
		redisCommands.sadd(key, "1", "2", "3", "4", "5");
		RedisItemReader<String, String> reader = structReader(info);
		live(reader);
		reader.setNotificationQueueCapacity(100);
		RedisItemWriter<String, String, KeyValue<String>> writer = RedisItemWriter.struct();
		writer.setClient(targetRedisClient);
		FlushingStepBuilder<KeyValue<String>, KeyValue<String>> step = flushingStep(info, reader, writer);
		Executors.newSingleThreadExecutor().execute(() -> {
			awaitPubSub();
			redisCommands.srem(key, "5");
		});
		run(info, step);
		assertEquals(redisCommands.smembers(key), targetRedisCommands.smembers(key));
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
	void compareTimeseries(TestInfo info) throws Exception {
		int count = 123;
		for (int index = 0; index < count; index++) {
			redisCommands.tsAdd("ts:" + index, Sample.of(123));
		}
		KeyspaceComparison comparisons = compare(info);
		Assertions.assertEquals(count, comparisons.get(Status.MISSING).size());
	}

	@Test
	void readTimeseries(TestInfo info) throws Exception {
		String key = "myts";
		Sample[] samples = { Sample.of(System.currentTimeMillis(), 1.1),
				Sample.of(System.currentTimeMillis() + 10, 2.2) };
		for (Sample sample : samples) {
			redisCommands.tsAdd(key, sample);
		}
		OperationExecutor<String, String, String, KeyValue<String>> executor = structOperationExecutor(info);
		KeyValue<String> ds = executor.apply(Arrays.asList(key)).get(0);
		Assertions.assertEquals(key, ds.getKey());
		Assertions.assertEquals(Type.TIMESERIES, ds.getType());
		Assertions.assertEquals(Arrays.asList(samples), ds.getValue());
		executor.close();
	}

	@Test
	void readTimeseriesByteArray(TestInfo info) throws Exception {
		String key = "myts";
		Sample[] samples = { Sample.of(System.currentTimeMillis(), 1.1),
				Sample.of(System.currentTimeMillis() + 10, 2.2) };
		for (Sample sample : samples) {
			redisCommands.tsAdd(key, sample);
		}
		OperationExecutor<byte[], byte[], byte[], KeyValue<byte[]>> executor = structOperationExecutor(info,
				ByteArrayCodec.INSTANCE);
		Function<String, byte[]> toByteArrayKeyFunction = BatchUtils.toByteArrayKeyFunction(StringCodec.UTF8);
		KeyValue<byte[]> ds = executor.apply(Arrays.asList(toByteArrayKeyFunction.apply(key))).get(0);
		Assertions.assertArrayEquals(toByteArrayKeyFunction.apply(key), ds.getKey());
		Assertions.assertEquals(Type.TIMESERIES, ds.getType());
		Assertions.assertEquals(Arrays.asList(samples), ds.getValue());
		executor.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	void writeComposite(TestInfo info) throws JobExecutionException, InterruptedException {
		List<String> list = IntStream.range(0, 10).mapToObj(String::valueOf).collect(Collectors.toList());
		ListItemReader<String> reader = new ListItemReader<>(list);
		String saddKey = "sadd:1";
		CompositeOperation<String, String, String, Object> composite = new CompositeOperation<>(
				Operation.set(Function.identity(), Function.identity()), Operation.sadd(saddKey, Function.identity()));
		RedisItemWriter<String, String, String> writer = RedisItemWriter.operation(composite);
		writer.setClient(redisClient);
		run(info, reader, writer);
		assertEquals(list.size() + 1, redisCommands.dbsize());
		for (String id : list) {
			assertEquals(id, redisCommands.get(id));
		}
		assertEquals(new HashSet<>(list), redisCommands.smembers(saddKey));

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
		ToSuggestionFunction<String, Suggestion<String>> converter = new ToSuggestionFunction<>(Suggestion::getString,
				Suggestion::getScore, Suggestion::getPayload);
		Sugadd<String, String, Suggestion<String>> sugadd = new Sugadd<>(keyFunction(key), converter);
		sugadd.setIncr(true);
		RedisItemWriter<String, String, Suggestion<String>> writer = writer(sugadd);
		run(info, reader, writer);
		assertEquals(1, redisCommands.dbsize());
		assertEquals(values.size(), redisCommands.ftSuglen(key));
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
		RedisItemWriter<String, String, Entry<Long, Double>> writer = writer(tsAdd);
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
		GeneratorItemReader gen = generator(73, Item.Type.JSON);
		generate(info, gen);
		JsonDel<String, String, KeyValue<String>> jsonDel = new JsonDel<>(KeyValue::getKey);
		RedisItemWriter<String, String, KeyValue<String>> writer = writer(jsonDel);
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
		ListItemReader<Sample> reader = new ListItemReader<>(samples);
		AddOptions<String, String> addOptions = AddOptions.<String, String>builder().policy(DuplicatePolicy.LAST)
				.build();
		TsAdd<String, String, Sample> tsadd = new TsAdd<>(keyFunction(key), Function.identity());
		tsadd.setOptions(addOptions);
		RedisItemWriter<String, String, Sample> writer = writer(tsadd);
		run(info, reader, writer);
		Assertions.assertEquals(count / 2,
				redisCommands.tsRange(key, TimeRange.unbounded(), RangeOptions.builder().build()).size(), 2);
	}

	@SuppressWarnings("unchecked")
	@Test
	void writeTsAddAll(TestInfo info) throws Exception {
		int count = 10;
		GeneratorItemReader reader = generator(count, Item.Type.TIMESERIES);
		AddOptions<String, String> addOptions = AddOptions.<String, String>builder().policy(DuplicatePolicy.LAST)
				.build();
		TsAddAll<String, String, KeyValue<String>> tsadd = new TsAddAll<>();
		tsadd.setKeyFunction(KeyValue::getKey);
		tsadd.setSamplesFunction(t -> (Collection<Sample>) t.getValue());
		tsadd.setOptions(addOptions);
		RedisItemWriter<String, String, KeyValue<String>> writer = new RedisItemWriter<>(StringCodec.UTF8, tsadd);
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
