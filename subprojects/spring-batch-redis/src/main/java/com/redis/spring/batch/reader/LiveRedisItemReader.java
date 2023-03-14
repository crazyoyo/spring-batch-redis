package com.redis.spring.batch.reader;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.springframework.batch.item.ItemProcessor;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.KeyValue;

public class LiveRedisItemReader<K, T extends KeyValue<K>> extends RedisItemReader<K, T>
		implements PollableItemReader<T> {

	public LiveRedisItemReader(JobRunner jobRunner, PollableItemReader<K> keyReader, ItemProcessor<K, K> keyProcessor,
			ItemProcessor<List<K>, List<T>> valueReader, ReaderOptions options) {
		super(jobRunner, keyReader, keyProcessor, valueReader, options);
	}

	@Override
	protected synchronized void doOpen() throws Exception {
		super.doOpen();
		Awaitility.await().timeout(JobRunner.DEFAULT_RUNNING_TIMEOUT).until(this::isOpen);
	}

	@Override
	public boolean isOpen() {
		return super.isOpen() && ((PollableItemReader<K>) keyReader).isOpen();
	}

	@Override
	public T poll(long timeout, TimeUnit unit) throws InterruptedException {
		return queue.poll(timeout, unit);
	}

}
