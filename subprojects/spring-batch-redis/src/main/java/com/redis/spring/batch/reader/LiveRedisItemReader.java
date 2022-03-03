package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.transaction.PlatformTransactionManager;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.reader.LiveKeyItemReader.KeyListener;
import com.redis.spring.batch.step.FlushingSimpleStepBuilder;

public class LiveRedisItemReader<K, T extends KeyValue<K, ?>> extends RedisItemReader<K, T>
		implements PollableItemReader<T> {

	private final KeyDeduplicator deduplicator = new KeyDeduplicator();
	private final LiveKeyItemReader<K> keyReader;
	private Duration flushingInterval = FlushingSimpleStepBuilder.DEFAULT_FLUSHING_INTERVAL;
	private Optional<Duration> idleTimeout = Optional.empty();

	public LiveRedisItemReader(JobRepository jobRepository, PlatformTransactionManager transactionManager,
			LiveKeyItemReader<K> keyReader, ValueReader<K, T> valueReader) {
		super(jobRepository, transactionManager, keyReader, valueReader);
		this.keyReader = keyReader;
	}

	public void setFlushingInterval(Duration flushingInterval) {
		this.flushingInterval = flushingInterval;
	}

	public void setIdleTimeout(Optional<Duration> idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
		super.open(executionContext);
		keyReader.addListener(deduplicator);
	}

	@Override
	public T poll(long timeout, TimeUnit unit) throws InterruptedException {
		return valueQueue.poll(timeout, unit);
	}

	@Override
	protected FaultTolerantStepBuilder<K, K> faultTolerant(SimpleStepBuilder<K, K> stepBuilder) {
		return new FlushingSimpleStepBuilder<>(stepBuilder).flushingInterval(flushingInterval).idleTimeout(idleTimeout);
	}

	private class KeyDeduplicator implements KeyListener<K> {

		@Override
		public void key(K key) {
			enqueuer.filter(key);
		}

	}

}
