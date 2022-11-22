package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.step.FlushingChunkProvider;

public class BlockingQueueItemReader<T> extends AbstractItemStreamItemReader<T> implements PollableItemReader<T> {

	private final Log log = LogFactory.getLog(getClass());

	private final BlockingQueue<T> queue;
	private final long defaultPollTimeout;

	private boolean open;

	public BlockingQueueItemReader(BlockingQueue<T> queue, Duration defaultPollTimeout) {
		Assert.notNull(queue, "Queue must not be null");
		Assert.notNull(defaultPollTimeout, "Default poll timeout must not be null");
		Assert.isTrue(!defaultPollTimeout.isNegative() && !defaultPollTimeout.isZero(),
				"Default poll timeout must be strictly positive");
		setName(ClassUtils.getShortName(getClass()));
		this.queue = queue;
		this.defaultPollTimeout = defaultPollTimeout.toMillis();
	}

	@Override
	public T read() throws InterruptedException {
		return poll(defaultPollTimeout, TimeUnit.MILLISECONDS);
	}

	@Override
	public T poll(long timeout, TimeUnit unit) throws InterruptedException {
		if (!open) {
			return null;
		}
		return queue.poll(timeout, unit);
	}

	@Override
	public void open(ExecutionContext executionContext) {
		super.open(executionContext);
		this.open = true;
	}

	@Override
	public void close() {
		super.close();
		if (!queue.isEmpty()) {
			log.warn(String.format("Closing with %s items still in queue", queue.size()));
		}
		queue.clear();
		this.open = false;
	}

	@Override
	public boolean isOpen() {
		return open;
	}

	@SuppressWarnings("unchecked")
	public void stop() throws InterruptedException {
		queue.put((T) FlushingChunkProvider.POISON_PILL);
	}

}
