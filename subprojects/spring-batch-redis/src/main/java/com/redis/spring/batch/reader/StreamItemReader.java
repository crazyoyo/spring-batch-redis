package com.redis.spring.batch.reader;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.Assert;

import com.redis.spring.batch.common.Utils;
import com.redis.spring.batch.reader.StreamReaderOptions.AckPolicy;

import io.lettuce.core.Consumer;
import io.lettuce.core.RedisBusyException;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.RedisStreamCommands;

public class StreamItemReader<K, V> implements PollableItemReader<StreamMessage<K, V>> {

	private final GenericObjectPool<StatefulConnection<K, V>> pool;
	private final K stream;
	private final Consumer<K> consumer;
	private final StreamReaderOptions options;
	private Iterator<StreamMessage<K, V>> iterator = Collections.emptyIterator();
	private boolean open;

	public StreamItemReader(GenericObjectPool<StatefulConnection<K, V>> pool, K stream, Consumer<K> consumer,
			StreamReaderOptions options) {
		Assert.notNull(pool, "A connection pool is required");
		this.pool = pool;
		this.stream = stream;
		this.consumer = consumer;
		this.options = options;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		synchronized (pool) {
			try (StatefulConnection<K, V> connection = pool.borrowObject()) {
				RedisStreamCommands<K, V> commands = Utils.sync(connection);
				commands.xgroupCreate(StreamOffset.from(stream, options.getOffset()), consumer.getGroup(),
						XGroupCreateArgs.Builder.mkstream(true));
				open = true;
			} catch (RedisBusyException e) {
				// Consumer Group name already exists, ignore
			} catch (Exception e) {
				throw new ItemStreamException("Failed to initialize the reader", e);
			}
		}
	}

	public boolean isOpen() {
		return open;
	}

	@Override
	public void update(ExecutionContext executionContext) throws ItemStreamException {
		// Do nothing
	}

	@Override
	public StreamMessage<K, V> read() throws Exception {
		throw new IllegalAccessException("read() method is not supposed to be called");
	}

	@Override
	public StreamMessage<K, V> poll(long timeout, TimeUnit unit) throws Exception {
		if (!iterator.hasNext()) {
			List<StreamMessage<K, V>> messages = readMessages(unit.toMillis(timeout));
			if (messages == null || messages.isEmpty()) {
				return null;
			}
			iterator = messages.iterator();
		}
		return iterator.next();
	}

	public List<StreamMessage<K, V>> readMessages() throws Exception {
		return readMessages(options.getBlock().toMillis());
	}

	@SuppressWarnings("unchecked")
	private List<StreamMessage<K, V>> readMessages(long blockInMillis) throws Exception {
		XReadArgs args = XReadArgs.Builder.count(options.getCount()).block(blockInMillis);
		try (StatefulConnection<K, V> connection = pool.borrowObject()) {
			RedisStreamCommands<K, V> commands = Utils.sync(connection);
			List<StreamMessage<K, V>> messages = commands.xreadgroup(consumer, args, StreamOffset.lastConsumed(stream));
			if (options.getAckPolicy() == AckPolicy.AUTO) {
				ack(messages.stream().map(StreamMessage::getId).toArray(String[]::new));
			}
			return messages;
		}
	}

	/**
	 * Acks given message ids
	 * @param ids message ids to be acked
	 * @throws Exception if a connection could not be obtained from the pool 
	 */
	public void ack(String... ids) throws Exception {
		if (ids.length == 0) {
			return;
		}
		try (StatefulConnection<K, V> connection = pool.borrowObject()) {
			RedisStreamCommands<K, V> commands = Utils.sync(connection);
			commands.xack(stream, consumer.getGroup(), ids);
		}
	}

	@Override
	public void close() throws ItemStreamException {
		open = false;
	}

}
