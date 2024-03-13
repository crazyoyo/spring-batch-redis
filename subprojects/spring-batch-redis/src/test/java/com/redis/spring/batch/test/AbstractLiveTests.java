package com.redis.spring.batch.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.concurrent.Executors;

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
import com.redis.spring.batch.writer.DumpItemWriter;
import com.redis.spring.batch.writer.StructItemWriter;

abstract class AbstractLiveTests extends AbstractBatchTests {

	private <K, V, T extends KeyValue<K>> void replicateLive(TestInfo info, RedisItemReader<K, V, T> reader,
			RedisItemWriter<K, V, T> writer, RedisItemReader<K, V, T> liveReader, RedisItemWriter<K, V, T> liveWriter)
			throws Exception {
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

}
