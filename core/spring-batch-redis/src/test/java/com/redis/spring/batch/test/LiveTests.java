package com.redis.spring.batch.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Range;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.reader.DumpItemReader;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader;
import com.redis.spring.batch.reader.StructItemReader;
import com.redis.spring.batch.step.FlushingStepBuilder;
import com.redis.spring.batch.util.Await;
import com.redis.spring.batch.util.CodecUtils;
import com.redis.spring.batch.writer.DumpItemWriter;
import com.redis.spring.batch.writer.StructItemWriter;

import io.lettuce.core.codec.ByteArrayCodec;

abstract class LiveTests extends BatchTests {

	private final Log log = LogFactory.getLog(getClass());

	private <K, V, T extends KeyValue<K>> KeyspaceComparison replicateLive(TestInfo info,
			RedisItemReader<K, V, T> reader, RedisItemWriter<K, V, T> writer, RedisItemReader<K, V, T> liveReader,
			RedisItemWriter<K, V, T> liveWriter) throws Exception {
		live(liveReader);
		generate(info, generator(300));
		TaskletStep step = faultTolerant(step(new SimpleTestInfo(info, "step"), reader, writer)).build();
		SimpleFlow flow = new FlowBuilder<SimpleFlow>(name(new SimpleTestInfo(info, "snapshotFlow"))).start(step)
				.build();
		FlushingStepBuilder<T, T> flushingStepBuilder = flushingStep(new SimpleTestInfo(info, "liveStep"), liveReader,
				liveWriter);
		GeneratorItemReader liveGen = generator(700, DataType.HASH, DataType.LIST, DataType.SET, DataType.STRING,
				DataType.ZSET);
		liveGen.setExpiration(Range.of(100));
		liveGen.setKeyRange(Range.from(300));
		generateAsync(testInfo(info, "genasync"), flushingStepBuilder, liveGen);
		TaskletStep liveStep = faultTolerant(flushingStepBuilder).build();
		SimpleFlow liveFlow = new FlowBuilder<SimpleFlow>(name(new SimpleTestInfo(info, "liveFlow"))).start(liveStep)
				.build();
		Job job = job(info).start(new FlowBuilder<SimpleFlow>(name(new SimpleTestInfo(info, "flow")))
				.split(new SimpleAsyncTaskExecutor()).add(liveFlow, flow).build()).build().build();
		run(job);
		return compare(info);
	}

	@Test
	void readKeyspaceNotificationsDedupe() throws Exception {
		enableKeyspaceNotifications(client);
		KeyspaceNotificationItemReader<String> reader = live(RedisItemReader.struct(client))
				.keyspaceNotificationReader();
		try {
			reader.open(new ExecutionContext());
			String key = "key1";
			commands.zadd(key, 1, "member1");
			commands.zadd(key, 2, "member2");
			commands.zadd(key, 3, "member3");
			awaitUntil(() -> reader.getQueue().size() == 1);
			Assertions.assertEquals(key, reader.getQueue().take());
		} finally {
			reader.close();
		}
	}

	@Test
	void readStructLive(TestInfo info) throws Exception {
		enableKeyspaceNotifications(client);
		StructItemReader<byte[], byte[]> reader = live(RedisItemReader.struct(client, ByteArrayCodec.INSTANCE));
		reader.setKeyspaceNotificationQueueCapacity(10000);
		reader.open(new ExecutionContext());
		int count = 1234;
		generate(info, generator(count, DataType.HASH, DataType.STRING));
		List<KeyValue<byte[]>> list = readAll(reader);
		Function<byte[], String> toString = CodecUtils.toStringKeyFunction(ByteArrayCodec.INSTANCE);
		Set<String> keys = list.stream().map(KeyValue::getKey).map(toString).collect(Collectors.toSet());
		Set<String> sourceKeys = new HashSet<>(commands.keys("*"));
		sourceKeys.removeAll(keys);
		log.info(sourceKeys);
		Assertions.assertEquals(count, keys.size());
		reader.close();
	}

	@Test
	void replicateDumpLive(TestInfo info) throws Exception {
		enableKeyspaceNotifications(client);
		DumpItemReader reader = RedisItemReader.dump(client);
		DumpItemWriter writer = RedisItemWriter.dump(targetClient);
		DumpItemReader liveReader = RedisItemReader.dump(client);
		DumpItemWriter liveWriter = RedisItemWriter.dump(targetClient);
		Assertions.assertTrue(replicateLive(info, reader, writer, liveReader, liveWriter).isOk());
	}

	@Test
	void replicateStructLive(TestInfo info) throws Exception {
		enableKeyspaceNotifications(client);
		StructItemReader<String, String> reader = RedisItemReader.struct(client);
		StructItemWriter<String, String> writer = RedisItemWriter.struct(targetClient);
		StructItemReader<String, String> liveReader = RedisItemReader.struct(client);
		StructItemWriter<String, String> liveWriter = RedisItemWriter.struct(targetClient);
		Assertions.assertTrue(replicateLive(info, reader, writer, liveReader, liveWriter).isOk());
	}

	@Test
	void replicateDumpLiveOnly(TestInfo info) throws Exception {
		enableKeyspaceNotifications(client);
		DumpItemReader reader = live(RedisItemReader.dump(client));
		reader.setKeyspaceNotificationQueueCapacity(100000);
		DumpItemWriter writer = RedisItemWriter.dump(targetClient);
		FlushingStepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> step = flushingStep(info, reader, writer);
		generateAsync(info, step, generator(100, DataType.HASH, DataType.LIST, DataType.SET, DataType.STRING, DataType.ZSET));
		run(info, step);
		Assertions.assertTrue(compare(info).isOk());
	}

	@Test
	void replicateSetLiveOnly(TestInfo info) throws Exception {
		enableKeyspaceNotifications(client);
		String key = "myset";
		commands.sadd(key, "1", "2", "3", "4", "5");
		StructItemReader<String, String> reader = live(RedisItemReader.struct(client));
		reader.setKeyspaceNotificationQueueCapacity(100);
		StructItemWriter<String, String> writer = RedisItemWriter.struct(targetClient);
		FlushingStepBuilder<KeyValue<String>, KeyValue<String>> step = flushingStep(info, reader, writer);
		Executors.newSingleThreadExecutor().execute(() -> {
			try {
				Await.await().until(() -> commands.pubsubNumpat() > 0);
				commands.srem(key, "5");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
		run(info, step);
		assertEquals(commands.smembers(key), targetCommands.smembers(key));
	}

}
