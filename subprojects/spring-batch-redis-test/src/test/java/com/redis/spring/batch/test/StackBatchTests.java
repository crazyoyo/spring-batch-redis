package com.redis.spring.batch.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.util.unit.DataSize;

import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.Range;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.DataType;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.common.MultiOperation;
import com.redis.spring.batch.item.redis.gen.GeneratorItemReader;
import com.redis.spring.batch.item.redis.gen.GeneratorOptions;
import com.redis.spring.batch.item.redis.gen.MapOptions;
import com.redis.spring.batch.item.redis.reader.KeyComparison;
import com.redis.spring.batch.item.redis.reader.KeyComparison.Status;
import com.redis.spring.batch.item.redis.reader.KeyComparisonItemReader;
import com.redis.spring.batch.item.redis.reader.KeyValueRead;
import com.redis.spring.batch.item.redis.reader.StreamItemReader;
import com.redis.spring.batch.item.redis.reader.StreamItemReader.AckPolicy;
import com.redis.spring.batch.item.redis.writer.KeyValueWrite.WriteMode;
import com.redis.spring.batch.item.redis.writer.operation.Del;
import com.redis.spring.batch.item.redis.writer.operation.Expire;
import com.redis.spring.batch.item.redis.writer.operation.ExpireAt;
import com.redis.spring.batch.item.redis.writer.operation.Hset;
import com.redis.spring.batch.item.redis.writer.operation.Lpush;
import com.redis.spring.batch.item.redis.writer.operation.Rpush;
import com.redis.spring.batch.item.redis.writer.operation.Sadd;
import com.redis.spring.batch.item.redis.writer.operation.Xadd;
import com.redis.spring.batch.item.redis.writer.operation.Zadd;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.Consumer;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.Range.Boundary;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.ByteArrayCodec;

class StackBatchTests extends BatchTests {

	private static final RedisServer source = RedisContainerFactory.stack();
	private static final RedisServer target = RedisContainerFactory.stack();

	@Override
	protected RedisServer getRedisServer() {
		return source;
	}

	@Override
	protected RedisServer getTargetRedisServer() {
		return target;
	}

	@Test
	void compareTimeseries(TestInfo info) throws Exception {
		int count = 123;
		for (int index = 0; index < count; index++) {
			redisCommands.tsAdd("ts:" + index, Sample.of(123));
		}
		KeyspaceComparison<String> comparisons = compare(info);
		Assertions.assertEquals(count, comparisons.get(Status.MISSING).size());
	}

	@Test
	void readStructLive(TestInfo info) throws Exception {
		enableKeyspaceNotifications();
		RedisItemReader<byte[], byte[], Object> reader = RedisItemReader.struct(ByteArrayCodec.INSTANCE);
		configure(info, reader);
		reader.setClient(redisClient);
		reader.setIdleTimeout(Duration.ofSeconds(3));
		live(reader);
		reader.open(new ExecutionContext());
		int count = 1234;
		generate(info, generator(count, DataType.HASH, DataType.STRING));
		awaitUntil(reader::isComplete);
		Collection<KeyValue<byte[], Object>> list = reader.getQueue();
		Function<byte[], String> toString = BatchUtils.toStringKeyFunction(ByteArrayCodec.INSTANCE);
		Set<String> keys = list.stream().map(KeyValue::getKey).map(toString).collect(Collectors.toSet());
		Assertions.assertEquals(count, keys.size());
		reader.close();
	}

	@Test
	void replicateHLL(TestInfo info) throws Exception {
		String key1 = "hll:1";
		redisCommands.pfadd(key1, "member:1", "member:2");
		String key2 = "hll:2";
		redisCommands.pfadd(key2, "member:1", "member:2", "member:3");
		RedisItemReader<byte[], byte[], Object> reader = RedisItemReader.struct(ByteArrayCodec.INSTANCE);
		configure(info, reader);
		reader.setClient(redisClient);
		RedisItemWriter<byte[], byte[], KeyValue<byte[], Object>> writer = RedisItemWriter
				.struct(ByteArrayCodec.INSTANCE);
		writer.setClient(targetRedisClient);
		replicate(info, reader, writer);
		assertEquals(redisCommands.pfcount(key1), targetRedisCommands.pfcount(key1));
	}

	@Test
	void readLiveType(TestInfo info) throws Exception {
		enableKeyspaceNotifications();
		RedisItemReader<String, String, Object> reader = structReader(info);
		live(reader);
		reader.setKeyType(DataType.HASH.getString());
		reader.open(new ExecutionContext());
		generate(info, generator(100));
		List<KeyValue<String, Object>> keyValues = readAll(reader);
		reader.close();
		keyValues.forEach(v -> Assertions.assertEquals(DataType.HASH.getString(), v.getType()));
	}

	@Test
	void readStructMemoryUsage(TestInfo info) throws Exception {
		generate(info, generator(73));
		DataSize memLimit = DataSize.ofBytes(200);
		RedisItemReader<String, String, Object> reader = structReader(info);
		((KeyValueRead<String, String, Object>) reader.getOperation()).setMemUsageLimit(memLimit);
		reader.open(new ExecutionContext());
		List<KeyValue<String, Object>> keyValues = readAll(reader);
		reader.close();
		Assertions.assertFalse(keyValues.isEmpty());
		for (KeyValue<String, Object> keyValue : keyValues) {
			Assertions.assertNotNull(keyValue.getMemoryUsage());
			if (keyValue.getMemoryUsage() > memLimit.toBytes()) {
				Assertions.assertNull(keyValue.getValue());
			}
		}
	}

	@Test
	void readStructMemoryUsageTTL(TestInfo info) throws Exception {
		String key = "myhash";
		Map<String, String> hash = new HashMap<>();
		hash.put("field1", "value1");
		hash.put("field2", "value2");
		redisCommands.hset(key, hash);
		long ttl = System.currentTimeMillis() + 123456;
		redisCommands.pexpireat(key, ttl);
		RedisItemReader<String, String, Object> reader = structReader(info);
		reader.open(new ExecutionContext());
		KeyValue<String, Object> ds = reader.read();
		Assertions.assertEquals(key, ds.getKey());
		Assertions.assertEquals(ttl, ds.getTtl(), 1000);
		Assertions.assertEquals(DataType.HASH.getString(), ds.getType());
		Assertions.assertTrue(ds.getMemoryUsage() > 0);
		reader.close();
	}

	@Test
	void readStructMemLimit(TestInfo info) throws Exception {
		DataSize limit = DataSize.ofBytes(500);
		String key1 = "key:1";
		redisCommands.set(key1, "bar");
		String key2 = "key:2";
		redisCommands.set(key2, GeneratorItemReader.string(Math.toIntExact(limit.toBytes() * 2)));
		RedisItemReader<String, String, Object> reader = structReader(info);
		((KeyValueRead<String, String, Object>) reader.getOperation()).setMemUsageLimit(limit);
		reader.open(new ExecutionContext());
		List<KeyValue<String, Object>> keyValues = readAll(reader);
		reader.close();
		Map<String, KeyValue<String, Object>> map = keyValues.stream()
				.collect(Collectors.toMap(s -> s.getKey(), Function.identity()));
		Assertions.assertNull(map.get(key2).getValue());
	}

	@Test
	void replicateStructByteArray(TestInfo info) throws Exception {
		GeneratorItemReader gen = generator(1000);
		generate(info, gen);
		RedisItemReader<byte[], byte[], Object> reader = RedisItemReader.struct(ByteArrayCodec.INSTANCE);
		configure(info, reader);
		reader.setClient(redisClient);
		RedisItemWriter<byte[], byte[], KeyValue<byte[], Object>> writer = RedisItemWriter
				.struct(ByteArrayCodec.INSTANCE);
		writer.setClient(targetRedisClient);
		replicate(info, reader, writer);
	}

	@Test
	void replicateStructMemLimit(TestInfo info) throws Exception {
		generate(info, generator(73));
		RedisItemReader<String, String, Object> reader = structReader(info);
		((KeyValueRead<String, String, Object>) reader.getOperation()).setMemUsageLimit(DataSize.ofMegabytes(100));
		RedisItemWriter<String, String, KeyValue<String, Object>> writer = RedisItemWriter.struct();
		writer.setClient(targetRedisClient);
		replicate(info, reader, writer);
	}

	@Test
	void replicateDumpMemLimitHigh(TestInfo info) throws Exception {
		generate(info, generator(73));
		RedisItemReader<byte[], byte[], byte[]> reader = dumpReader(info);
		((KeyValueRead<byte[], byte[], byte[]>) reader.getOperation()).setMemUsageLimit(DataSize.ofMegabytes(100));
		RedisItemWriter<byte[], byte[], KeyValue<byte[], byte[]>> writer = RedisItemWriter.dump();
		writer.setClient(targetRedisClient);
		replicate(info, reader, writer);
	}

	@Test
	void readDumpMemLimitLow(TestInfo info) throws Exception {
		generate(info, generator(73));
		Assertions.assertTrue(redisCommands.dbsize() > 10);
		RedisItemReader<byte[], byte[], byte[]> reader = dumpReader(info);
		DataSize memLimit = DataSize.ofBytes(1500);
		((KeyValueRead<byte[], byte[], byte[]>) reader.getOperation()).setMemUsageLimit(memLimit);
		reader.open(new ExecutionContext());
		List<KeyValue<byte[], byte[]>> items = readAll(reader);
		reader.close();
		Assertions.assertFalse(
				items.stream().anyMatch(v -> v.getMemoryUsage() > memLimit.toBytes() && v.getValue() != null));
	}

	@Test
	void writeStruct(TestInfo info) throws Exception {
		int count = 1000;
		GeneratorItemReader reader = generator(count);
		generate(info, reader);
		RedisItemWriter<String, String, KeyValue<String, Object>> writer = RedisItemWriter.struct();
		writer.setClient(redisClient);
		run(info, reader, writer);
		awaitUntil(() -> keyCount("gen:*") == count);
		assertEquals(count, keyCount("gen:*"));
	}

	@Test
	void writeStructMultiExec(TestInfo info) throws Exception {
		int count = 10;
		GeneratorItemReader reader = generator(count);
		RedisItemWriter<String, String, KeyValue<String, Object>> writer = RedisItemWriter.struct();
		writer.setClient(redisClient);
		writer.setMultiExec(true);
		run(info, step(info, 1, reader, null, writer));
		assertEquals(count, redisCommands.dbsize());
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
		Xadd<String, String, Map<String, String>> xadd = new Xadd<>(t -> stream,
				t -> Arrays.asList(new StreamMessage<>(null, null, t)));
		xadd.setArgs(null);
		RedisItemWriter<String, String, Map<String, String>> writer = writer(xadd);
		writer.setMultiExec(true);
		run(testInfo, reader, writer);
		Assertions.assertEquals(messages.size(), redisCommands.xlen(stream));
		List<StreamMessage<String, String>> xrange = redisCommands.xrange(stream,
				io.lettuce.core.Range.create("-", "+"));
		for (int index = 0; index < xrange.size(); index++) {
			StreamMessage<String, String> message = xrange.get(index);
			Assertions.assertEquals(messages.get(index), message.getBody());
		}
	}

	@Test
	void readMultipleStreams(TestInfo info) throws Exception {
		String consumerGroup = "consumerGroup";
		generateStreams(info, 277);
		KeyScanArgs args = KeyScanArgs.Builder.type(DataType.STREAM.getString());
		final List<String> keys = ScanIterator.scan(redisCommands, args).stream().collect(Collectors.toList());
		for (String key : keys) {
			long count = redisCommands.xlen(key);
			StreamItemReader<String, String> reader1 = streamReader(info, key,
					Consumer.from(consumerGroup, "consumer1"));
			reader1.setAckPolicy(AckPolicy.MANUAL);
			StreamItemReader<String, String> reader2 = streamReader(info, key,
					Consumer.from(consumerGroup, "consumer2"));
			reader2.setAckPolicy(AckPolicy.MANUAL);
			ListItemWriter<StreamMessage<String, String>> writer1 = new ListItemWriter<>();
			TestInfo testInfo1 = new SimpleTestInfo(info, key, "1");
			TaskletStep step1 = faultTolerant(flushingStep(testInfo1, reader1, writer1)).build();
			TestInfo testInfo2 = new SimpleTestInfo(info, key, "2");
			ListItemWriter<StreamMessage<String, String>> writer2 = new ListItemWriter<>();
			TaskletStep step2 = faultTolerant(flushingStep(testInfo2, reader2, writer2)).build();
			SimpleFlow flow1 = flow("flow1").start(step1).build();
			SimpleFlow flow2 = flow("flow2").start(step2).build();
			SimpleFlow flow = flow("replicate").split(new SimpleAsyncTaskExecutor()).add(flow1, flow2).build();
			run(job(testInfo1).start(flow).build().build());
			Assertions.assertEquals(count, writer1.getWrittenItems().size() + writer2.getWrittenItems().size());
			assertMessageBody(writer1.getWrittenItems());
			assertMessageBody(writer2.getWrittenItems());
			Assertions.assertEquals(count, redisCommands.xpending(key, consumerGroup).getCount());
			reader1 = streamReader(info, key, Consumer.from(consumerGroup, "consumer1"));
			reader1.setAckPolicy(AckPolicy.MANUAL);
			reader1.open(new ExecutionContext());
			reader1.ack(writer1.getWrittenItems());
			reader1.close();
			reader2 = streamReader(info, key, Consumer.from(consumerGroup, "consumer2"));
			reader2.setAckPolicy(AckPolicy.MANUAL);
			reader2.open(new ExecutionContext());
			reader2.ack(writer2.getWrittenItems());
			reader2.close();
			Assertions.assertEquals(0, redisCommands.xpending(key, consumerGroup).getCount());
		}
	}

	@Test
	void replicateStruct(TestInfo info) throws Exception {
		GeneratorItemReader gen = generator(100);
		generate(info, gen);
		RedisItemReader<String, String, Object> reader = structReader(info);
		RedisItemWriter<String, String, KeyValue<String, Object>> writer = RedisItemWriter.struct();
		writer.setClient(targetRedisClient);
		replicate(info, reader, writer);
	}

	@Test
	void replicateDumpLive(TestInfo info) throws Exception {
		enableKeyspaceNotifications();
		RedisItemReader<byte[], byte[], byte[]> reader = dumpReader(info);
		RedisItemWriter<byte[], byte[], KeyValue<byte[], byte[]>> writer = RedisItemWriter.dump();
		writer.setClient(targetRedisClient);
		replicateLive(info, reader, writer);
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
	void replicateStructLive(TestInfo info) throws Exception {
		enableKeyspaceNotifications();
		RedisItemReader<String, String, Object> reader = structReader(info);
		RedisItemWriter<String, String, KeyValue<String, Object>> writer = RedisItemWriter.struct();
		writer.setClient(targetRedisClient);
		replicateLive(info, reader, writer);
	}

	private static FlowBuilder<SimpleFlow> flow(String name) {
		return new FlowBuilder<>(name);
	}

	@Test
	void writeHash(TestInfo info) throws Exception {
		int count = 100;
		List<Map<String, String>> maps = new ArrayList<>();
		for (int index = 0; index < count; index++) {
			Map<String, String> body = new HashMap<>();
			body.put("id", String.valueOf(index));
			body.put("field1", "value1");
			body.put("field2", "value2");
			maps.add(body);
		}
		ListItemReader<Map<String, String>> reader = new ListItemReader<>(maps);
		Hset<String, String, Map<String, String>> hset = new Hset<>(m -> "hash:" + m.remove("id"), Function.identity());
		RedisItemWriter<String, String, Map<String, String>> writer = writer(hset);
		run(info, reader, writer);
		assertEquals(count, keyCount("hash:*"));
		for (int index = 0; index < maps.size(); index++) {
			Map<String, String> hash = redisCommands.hgetall("hash:" + index);
			assertEquals(maps.get(index), hash);
		}
	}

	@Test
	void writeHashDel(TestInfo info) throws Exception {
		List<Entry<String, Map<String, String>>> hashes = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			String key = String.valueOf(index);
			Map<String, String> value = new HashMap<>();
			value.put("field1", "value1");
			redisCommands.hset("hash:" + key, value);
			Map<String, String> body = new HashMap<>();
			body.put("field2", "value2");
			hashes.add(new AbstractMap.SimpleEntry<>(key, index < 50 ? null : body));
		}
		ListItemReader<Map.Entry<String, Map<String, String>>> reader = new ListItemReader<>(hashes);
		Hset<String, String, Entry<String, Map<String, String>>> hset = new Hset<>(e -> "hash:" + e.getKey(),
				Entry::getValue);
		RedisItemWriter<String, String, Entry<String, Map<String, String>>> writer = writer(hset);
		run(info, reader, writer);
		assertEquals(100, keyCount("hash:*"));
		assertEquals(2, redisCommands.hgetall("hash:50").size());
	}

	@Test
	void writeDel(TestInfo info) throws Exception {
		generate(info, generator(73));
		GeneratorItemReader gen = generator(73);
		Del<String, String, KeyValue<String, Object>> del = new Del<>(KeyValue::getKey);
		RedisItemWriter<String, String, KeyValue<String, Object>> writer = writer(del);
		run(info, gen, writer);
		assertEquals(0, keyCount(GeneratorOptions.DEFAULT_KEYSPACE + "*"));
	}

	@Test
	void writeLpush(TestInfo info) throws Exception {
		int count = 73;
		GeneratorItemReader gen = generator(count, DataType.STRING);
		Lpush<String, String, KeyValue<String, Object>> lpush = new Lpush<>(KeyValue::getKey,
				v -> Arrays.asList((String) v.getValue()));
		RedisItemWriter<String, String, KeyValue<String, Object>> writer = writer(lpush);
		run(info, gen, writer);
		assertEquals(count, redisCommands.dbsize());
		for (String key : redisCommands.keys("*")) {
			assertEquals(DataType.LIST.getString(), redisCommands.type(key));
		}
	}

	@Test
	void writeRpush(TestInfo info) throws Exception {
		int count = 73;
		GeneratorItemReader gen = generator(count, DataType.STRING);
		Rpush<String, String, KeyValue<String, Object>> rpush = new Rpush<>(KeyValue::getKey,
				v -> Arrays.asList((String) v.getValue()));
		RedisItemWriter<String, String, KeyValue<String, Object>> writer = writer(rpush);
		run(info, gen, writer);
		assertEquals(count, redisCommands.dbsize());
		for (String key : redisCommands.keys("*")) {
			assertEquals(DataType.LIST.getString(), redisCommands.type(key));
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	void writeLpushAll(TestInfo info) throws Exception {
		int count = 73;
		GeneratorItemReader gen = generator(count, DataType.LIST);
		Lpush<String, String, KeyValue<String, Object>> lpush = new Lpush<>(KeyValue::getKey,
				v -> (Collection<String>) v.getValue());
		RedisItemWriter<String, String, KeyValue<String, Object>> writer = writer(lpush);
		run(info, gen, writer);
		assertEquals(count, redisCommands.dbsize());
		for (String key : redisCommands.keys("*")) {
			assertEquals(DataType.LIST.getString(), redisCommands.type(key));
		}
	}

	@Test
	void writeExpire(TestInfo info) throws Exception {
		int count = 73;
		GeneratorItemReader gen = generator(count, DataType.STRING);
		Expire<String, String, KeyValue<String, Object>> expire = new Expire<>(KeyValue::getKey);
		expire.setTtl(1L);
		RedisItemWriter<String, String, KeyValue<String, Object>> writer = writer(expire);
		run(info, gen, writer);
		awaitUntil(() -> redisCommands.dbsize() == 0);
		assertEquals(0, redisCommands.dbsize());
	}

	@Test
	void writeExpireAt(TestInfo info) throws Exception {
		int count = 73;
		GeneratorItemReader gen = generator(count, DataType.STRING);
		ExpireAt<String, String, KeyValue<String, Object>> expire = new ExpireAt<>(KeyValue::getKey);
		expire.setTimestamp(System.currentTimeMillis());
		RedisItemWriter<String, String, KeyValue<String, Object>> writer = writer(expire);
		run(info, gen, writer);
		awaitUntil(() -> redisCommands.dbsize() == 0);
		assertEquals(0, redisCommands.dbsize());
	}

	@Test
	void writeZset(TestInfo info) throws Exception {
		String key = "zadd";
		List<ScoredValue<String>> values = IntStream.range(0, 100)
				.mapToObj(index -> ScoredValue.just(index % 10, String.valueOf(index))).collect(Collectors.toList());
		ListItemReader<ScoredValue<String>> reader = new ListItemReader<>(values);
		Zadd<String, String, ScoredValue<String>> zadd = new Zadd<>(t -> key, t -> Arrays.asList(t));
		RedisItemWriter<String, String, ScoredValue<String>> writer = writer(zadd);
		run(info, reader, writer);
		assertEquals(1, redisCommands.dbsize());
		assertEquals(values.size(), redisCommands.zcard(key));
		assertEquals(60, redisCommands
				.zrangebyscore(key, io.lettuce.core.Range.from(Boundary.including(0), Boundary.including(5))).size());
	}

	@Test
	void writeSet(TestInfo info) throws Exception {
		String key = "sadd";
		List<String> values = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			values.add(String.valueOf(index));
		}
		ListItemReader<String> reader = new ListItemReader<>(values);
		Sadd<String, String, String> sadd = new Sadd<>(t -> key, v -> Arrays.asList(v));
		RedisItemWriter<String, String, String> writer = writer(sadd);
		run(info, reader, writer);
		assertEquals(1, redisCommands.dbsize());
		assertEquals(values.size(), redisCommands.scard(key));
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
		Xadd<String, String, Map<String, String>> xadd = new Xadd<>(t -> stream,
				m -> Arrays.asList(new StreamMessage<>(null, null, m)));
		xadd.setArgs(null);
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

	private MapOptions hashOptions(Range fieldCount) {
		MapOptions options = new MapOptions();
		options.setFieldCount(fieldCount);
		return options;
	}

	@Test
	void writeStructOverwrite(TestInfo info) throws Exception {
		GeneratorItemReader gen1 = generator(100, DataType.HASH);
		gen1.getOptions().setHashOptions(hashOptions(Range.of(5)));
		generate(info, gen1);
		GeneratorItemReader gen2 = generator(100, DataType.HASH);
		gen2.getOptions().setHashOptions(hashOptions(Range.of(10)));
		generate(testInfo(info, "target"), targetRedisClient, gen2);
		RedisItemWriter<String, String, KeyValue<String, Object>> writer = RedisItemWriter.struct();
		writer.setClient(targetRedisClient);
		replicate(info, structReader(info), writer);
		assertEquals(redisCommands.hgetall("gen:1"), targetRedisCommands.hgetall("gen:1"));
	}

	@Test
	void writeStructMerge(TestInfo info) throws Exception {
		GeneratorItemReader gen1 = generator(100, DataType.HASH);
		gen1.getOptions().setHashOptions(hashOptions(Range.of(5)));
		generate(info, gen1);
		GeneratorItemReader gen2 = generator(100, DataType.HASH);
		gen2.getOptions().setHashOptions(hashOptions(Range.of(10)));
		generate(testInfo(info, "target"), targetRedisClient, gen2);
		RedisItemReader<String, String, Object> reader = structReader(info);
		RedisItemWriter<String, String, KeyValue<String, Object>> writer = RedisItemWriter.struct(WriteMode.MERGE);
		writer.setClient(targetRedisClient);
		run(testInfo(info, "replicate"), reader, writer);
		Map<String, String> actual = targetRedisCommands.hgetall("gen:1");
		assertEquals(10, actual.size());
	}

	@Test
	void writeMultiOperation(TestInfo info) throws Exception {
		int count = 100;
		List<Map<String, String>> maps = new ArrayList<>();
		for (int index = 0; index < count; index++) {
			Map<String, String> body = new HashMap<>();
			body.put("id", String.valueOf(index));
			body.put("field1", "value1");
			body.put("field2", "value2");
			maps.add(body);
		}
		ListItemReader<Map<String, String>> reader = new ListItemReader<>(maps);
		Hset<String, String, Map<String, String>> hset = new Hset<>(m -> "hash:" + m.get("id"), Function.identity());
		Hset<String, String, Map<String, String>> hset2 = new Hset<>(m -> "hash2:" + m.get("id"), Function.identity());
		RedisItemWriter<String, String, Map<String, String>> writer = writer(new MultiOperation<>(hset, hset2));
		run(info, reader, writer);
		assertEquals(count, keyCount("hash:*"));
		assertEquals(count, keyCount("hash2:*"));
		for (int index = 0; index < maps.size(); index++) {
			assertEquals(maps.get(index), redisCommands.hgetall("hash:" + index));
			assertEquals(maps.get(index), redisCommands.hgetall("hash2:" + index));
		}
	}

	@Test
	void compareStreams(TestInfo info) throws Exception {
		GeneratorItemReader gen = generator(10);
		gen.getOptions().setTypes(DataType.STREAM);
		generate(info, gen);
		RedisItemReader<String, String, Object> reader = structReader(info);
		RedisItemWriter<String, String, KeyValue<String, Object>> writer = RedisItemWriter.struct();
		writer.setClient(targetRedisClient);
		replicate(info, reader, writer);
		KeyspaceComparison<String> comparison = compare(info);
		Assertions.assertEquals(Collections.emptyList(), comparison.mismatches());
	}

	@Test
	void compareStatus(TestInfo info) throws Exception {
		GeneratorItemReader gen = generator(120);
		generate(info, gen);
		assertDbNotEmpty(redisCommands);
		RedisItemReader<byte[], byte[], byte[]> reader = dumpReader(info);
		RedisItemWriter<byte[], byte[], KeyValue<byte[], byte[]>> writer = RedisItemWriter.dump();
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
			String type = targetRedisCommands.type(key);
			if (DataType.STRING.getString().equalsIgnoreCase(type)) {
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
		KeyComparisonItemReader<String, String> comparator = comparisonReader(testInfo(info, "comparison"));
		comparator.open(new ExecutionContext());
		List<KeyComparison<String>> comparisons = readAll(comparator);
		comparator.close();
		long sourceCount = redisCommands.dbsize();
		assertEquals(sourceCount, comparisons.size());
		assertEquals(sourceCount, targetRedisCommands.dbsize() + deleted);
		List<KeyComparison<String>> actualTypeChanges = comparisons.stream().filter(c -> c.getStatus() == Status.TYPE)
				.collect(Collectors.toList());
		assertEquals(typeChanges.size(), actualTypeChanges.size());
		assertEquals(valueChanges.size(), comparisons.stream().filter(c -> c.getStatus() == Status.VALUE).count());
		assertEquals(ttlChanges.size(), comparisons.stream().filter(c -> c.getStatus() == Status.TTL).count());
		assertEquals(deleted, comparisons.stream().filter(c -> c.getStatus() == Status.MISSING).count());
	}
}