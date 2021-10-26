package com.redis.spring.batch.support;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.transaction.PlatformTransactionManager;

import com.redis.spring.batch.RedisItemReader;

public class LiveRedisItemReader<K, T extends KeyValue<K, ?>> extends RedisItemReader<K, T>
		implements PollableItemReader<T> {

	private final Duration flushingInterval;
	private final Duration idleTimeout;
	private State state;

	public LiveRedisItemReader(JobRepository jobRepository, PlatformTransactionManager transactionManager,
			PollableItemReader<K> keyReader, ItemProcessor<List<? extends K>, List<T>> valueReader, int threads,
			int chunkSize, BlockingQueue<T> queue, Duration queuePollTimeout, SkipPolicy skipPolicy,
			Duration flushingInterval, Duration idleTimeout) {
		super(jobRepository, transactionManager, keyReader, valueReader, threads, chunkSize, queue, queuePollTimeout,
				skipPolicy);
		Utils.assertPositive(flushingInterval, "Flushing interval");
		this.flushingInterval = flushingInterval;
		this.idleTimeout = idleTimeout;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
		super.open(executionContext);
		this.state = State.OPEN;
	}

	@Override
	public synchronized void close() {
		super.close();
		this.state = State.CLOSED;
	}

	@Override
	public State getState() {
		return state;
	}

	@Override
	public T poll(long timeout, TimeUnit unit) throws InterruptedException {
		return queue.poll(timeout, unit);
	}

	@Override
	protected FaultTolerantStepBuilder<K, K> faultTolerantStepBuilder(SimpleStepBuilder<K, K> stepBuilder) {
		return new FlushingStepBuilder<>(stepBuilder).flushingInterval(flushingInterval).idleTimeout(idleTimeout);
	}

}
