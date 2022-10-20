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

	public LiveRedisItemReader(KeyspaceNotificationItemReader<K> keyReader,
			ItemProcessor<List<? extends K>, List<T>> valueReader, JobRunner jobRunner, LiveReaderOptions options) {
		super(keyReader, valueReader, jobRunner, options);
	}

	@Override
	public KeyspaceNotificationItemReader<K> getKeyReader() {
		return (KeyspaceNotificationItemReader<K>) super.getKeyReader();
	}

	@Override
	protected void doOpen() {
		super.doOpen();
		Awaitility.await().timeout(JobRunner.DEFAULT_RUNNING_TIMEOUT)
				.until(((KeyspaceNotificationItemReader<K>) keyReader)::isOpen);
	}

	@Override
	protected SimpleStepBuilder<K, K> createStep() {
		SimpleStepBuilder<K, K> step = super.createStep();
		return new FlushingSimpleStepBuilder<>(step).options(((LiveReaderOptions) options).getFlushingOptions());
	}

	@Override
	public T poll(long timeout, TimeUnit unit) throws InterruptedException {
		return queue.poll(timeout, unit);
	}

}
