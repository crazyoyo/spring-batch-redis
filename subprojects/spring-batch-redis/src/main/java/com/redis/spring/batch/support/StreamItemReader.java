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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

public class StreamItemReader<K, V> extends ConnectionPoolItemStream<K, V>
		implements PollableItemReader<StreamMessage<K, V>> {

	private static final Logger log = LoggerFactory.getLogger(StreamItemReader.class);

	public enum AckPolicy {
		AUTO, MANUAL
	}

	public static final Duration DEFAULT_BLOCK = Duration.ofMillis(100);
	public static final long DEFAULT_COUNT = 50;
	public static final AckPolicy DEFAULT_ACK_POLICY = AckPolicy.AUTO;

	private final Function<StatefulConnection<K, V>, BaseRedisCommands<K, V>> sync;
	private final StreamOffset<K> offset;
	private long count = DEFAULT_COUNT;
	private Duration block = DEFAULT_BLOCK;
	private K consumerGroup;
	private K consumer;
	private AckPolicy ackPolicy = DEFAULT_ACK_POLICY;
	private Iterator<StreamMessage<K, V>> iterator = Collections.emptyIterator();

	public StreamItemReader(Supplier<StatefulConnection<K, V>> connectionSupplier,
			GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig,
			Function<StatefulConnection<K, V>, BaseRedisCommands<K, V>> sync, StreamOffset<K> offset) {
		super(connectionSupplier, poolConfig);
		Assert.notNull(sync, "A command provider is required");
		Assert.notNull(offset, "A stream offset is required");
		this.sync = sync;
		this.offset = offset;
	}

	public void setConsumer(K consumer) {
		this.consumer = consumer;
	}

	public void setConsumerGroup(K consumerGroup) {
		this.consumerGroup = consumerGroup;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public void setAckPolicy(AckPolicy ackPolicy) {
		this.ackPolicy = ackPolicy;
	}

	public void setBlock(Duration block) {
		this.block = block;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void open(ExecutionContext executionContext) {
		super.open(executionContext);
		synchronized (sync) {
			try (StatefulConnection<K, V> connection = borrowConnection()) {
				RedisStreamCommands<K, V> commands = (RedisStreamCommands<K, V>) sync.apply(connection);
				commands.xgroupCreate(offset, consumerGroup, XGroupCreateArgs.Builder.mkstream(true));
			} catch (RedisBusyException e) {
				// Consumer Group name already exists, ignore
			} catch (Exception e) {
				throw new ItemStreamException("Failed to initialize the reader", e);
			}
		}
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
		if (blockInMillis != null) {
			args.block(blockInMillis);
		}
		try (StatefulConnection<K, V> connection = borrowConnection()) {
			RedisStreamCommands<K, V> commands = (RedisStreamCommands<K, V>) sync.apply(connection);
			List<StreamMessage<K, V>> messages = commands.xreadgroup(Consumer.from(consumerGroup, consumer), args,
					StreamOffset.lastConsumed(offset.getName()));
			if (ackPolicy == AckPolicy.AUTO) {
				ack(messages, commands);
			}
			return messages;
		}
	}

	@SuppressWarnings("unchecked")
	public void ack(List<? extends StreamMessage<K, V>> messages) throws Exception {
		if (messages.isEmpty()) {
			return;
		}
		try (StatefulConnection<K, V> connection = borrowConnection()) {
			RedisStreamCommands<K, V> commands = (RedisStreamCommands<K, V>) sync.apply(connection);
			ack(messages, commands);
		}
	}

	private void ack(List<? extends StreamMessage<K, V>> messages, RedisStreamCommands<K, V> commands) {
		Map<K, List<StreamMessage<K, V>>> streams = messages.stream()
				.collect(Collectors.groupingBy(StreamMessage::getStream));
		for (Map.Entry<K, List<StreamMessage<K, V>>> entry : streams.entrySet()) {
			String[] messageIds = entry.getValue().stream().map(StreamMessage::getId).toArray(String[]::new);
			log.debug("Ack'ing message ids: {}", Arrays.asList(messageIds));
			commands.xack(entry.getKey(), consumerGroup, messageIds);
		}
	}

}
