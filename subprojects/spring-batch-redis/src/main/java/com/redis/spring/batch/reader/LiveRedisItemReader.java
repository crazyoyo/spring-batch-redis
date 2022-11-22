package com.redis.spring.batch.reader;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamException;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.FlushingStepOptions;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.KeyValue;

public class LiveRedisItemReader<K, T extends KeyValue<K>> extends RedisItemReader<K, T>
		implements PollableItemReader<T> {

	public LiveRedisItemReader(JobRunner jobRunner, PollableItemReader<K> keyReader,
			ItemProcessor<List<? extends K>, List<T>> valueReader, FlushingStepOptions stepOptions,
			QueueOptions queueOptions) {
		super(jobRunner, keyReader, valueReader, stepOptions, queueOptions);
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		super.open(executionContext);
		Awaitility.await().timeout(JobRunner.DEFAULT_RUNNING_TIMEOUT)
				.until(((PollableItemReader<K>) keyReader)::isOpen);
	}

	@Override
	public T poll(long timeout, TimeUnit unit) throws InterruptedException {
		return queue.poll(timeout, unit);
	}

}
