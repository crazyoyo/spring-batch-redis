package com.redis.spring.batch.reader;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.step.FlushingSimpleStepBuilder;

public class LiveRedisItemReader<K, T extends KeyValue<K>> extends RedisItemReader<K, T>
		implements PollableItemReader<T> {

	public LiveRedisItemReader(LiveKeyItemReader<K> keyReader, ItemProcessor<List<? extends K>, List<T>> valueReader,
			JobRunner jobRunner, LiveReaderOptions options) {
		super(keyReader, valueReader, jobRunner, options);
	}

	@Override
	protected void doOpen() {
		super.doOpen();
		Awaitility.await().timeout(JobRunner.DEFAULT_RUNNING_TIMEOUT).until(((LiveKeyItemReader<K>) keyReader)::isOpen);
	}

	@Override
	protected SimpleStepBuilder<K, K> createStep() {
		return new FlushingSimpleStepBuilder<>(super.createStep())
				.flushingInterval(((LiveReaderOptions) options).getFlushingInterval())
				.idleTimeout(((LiveReaderOptions) options).getIdleTimeout());
	}

	@Override
	public T poll(long timeout, TimeUnit unit) throws InterruptedException {
		return valueQueue.poll(timeout, unit);
	}

}
