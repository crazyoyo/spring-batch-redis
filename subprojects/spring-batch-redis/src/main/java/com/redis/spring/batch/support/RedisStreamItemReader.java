package com.redis.spring.batch.support;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.Assert;

import io.lettuce.core.Consumer;
import io.lettuce.core.RedisBusyException;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisStreamCommands;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RedisStreamItemReader<K, V> extends ConnectionPoolItemStream<K, V>
		implements PollableItemReader<StreamMessage<K, V>> {

	public enum AckPolicy {
		AUTO, MANUAL
	}

	private final Function<StatefulConnection<K, V>, BaseRedisCommands<K, V>> sync;
	private final Long count;
	private final Duration block;
	private final StreamOffset<K> offset;
	private final K consumerGroup;
	private final K consumer;
	private final AckPolicy ackPolicy;
	private Iterator<StreamMessage<K, V>> iterator = Collections.emptyIterator();
	private State state;

	public RedisStreamItemReader(Supplier<StatefulConnection<K, V>> connectionSupplier,
			GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig,
			Function<StatefulConnection<K, V>, BaseRedisCommands<K, V>> sync, Long count, Duration block,
			K consumerGroup, K consumer, StreamOffset<K> offset, AckPolicy ackPolicy) {
		super(connectionSupplier, poolConfig);
		Assert.notNull(sync, "A command provider is required");
		this.sync = sync;
		this.count = count;
		this.block = block;
		this.consumerGroup = consumerGroup;
		this.consumer = consumer;
		this.offset = offset;
		this.ackPolicy = ackPolicy;
	}

	@SuppressWarnings("unchecked")
	@Override
	public synchronized void open(ExecutionContext executionContext) {
		super.open(executionContext);
		try (StatefulConnection<K, V> connection = pool.borrowObject()) {
			RedisStreamCommands<K, V> commands = (RedisStreamCommands<K, V>) sync.apply(connection);
			XGroupCreateArgs args = XGroupCreateArgs.Builder.mkstream(true);
			try {
				commands.xgroupCreate(offset, consumerGroup, args);
			} catch (RedisBusyException e) {
				// Consumer Group name already exists, ignore
			}
		} catch (Exception e) {
			throw new ItemStreamException("Failed to initialize the reader", e);
		}
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
		return readMessages(block == null ? null : block.toMillis());
	}

	@SuppressWarnings("unchecked")
	private List<StreamMessage<K, V>> readMessages(Long blockInMillis) throws Exception {
		XReadArgs args = XReadArgs.Builder.count(count);
		if (block != null) {
			args.block(block);
		}
		try (StatefulConnection<K, V> connection = pool.borrowObject()) {
			RedisStreamCommands<K, V> commands = (RedisStreamCommands<K, V>) sync.apply(connection);
			List<StreamMessage<K, V>> messages = commands.xreadgroup(Consumer.from(consumerGroup, consumer), args,
					StreamOffset.lastConsumed(offset.getName()));
			if (ackPolicy == AckPolicy.AUTO) {
				ack(messages);
			}
			return messages;
		}
	}

	@SuppressWarnings("unchecked")
	public void ack(List<? extends StreamMessage<K, V>> messages) throws Exception {
		if (messages.isEmpty()) {
			return;
		}
		try (StatefulConnection<K, V> connection = pool.borrowObject()) {
			RedisStreamCommands<K, V> commands = (RedisStreamCommands<K, V>) sync.apply(connection);
			Map<K, List<StreamMessage<K, V>>> streams = messages.stream()
					.collect(Collectors.groupingBy(StreamMessage::getStream));
			for (Map.Entry<K, List<StreamMessage<K, V>>> entry : streams.entrySet()) {
				String[] messageIds = entry.getValue().stream().map(StreamMessage::getId).toArray(String[]::new);
				log.info("Ack'ing message ids: {}", Arrays.asList(messageIds));
				commands.xack(entry.getKey(), consumerGroup, messageIds);
			}
		}
	}

}
