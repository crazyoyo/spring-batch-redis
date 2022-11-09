package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.ArrayList;
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

	public static final Duration DEFAULT_POLL_DURATION = Duration.ofSeconds(1);
	public static final String START_OFFSET = "0-0";

	private final GenericObjectPool<StatefulConnection<K, V>> pool;
	private final K stream;
	private final Consumer<K> consumer;
	private final StreamReaderOptions options;
	private Iterator<StreamMessage<K, V>> iterator = Collections.emptyIterator();
	private boolean open;
	private MessageReader<K, V> reader;
	private String lastId;

	public StreamItemReader(GenericObjectPool<StatefulConnection<K, V>> pool, K stream, Consumer<K> consumer,
			StreamReaderOptions options) {
		Assert.notNull(pool, "A connection pool is required");
		this.pool = pool;
		this.stream = stream;
		this.consumer = consumer;
		this.options = options;
	}

	public static class StreamId implements Comparable<StreamId> {

		public static final StreamId ZERO = StreamId.of(0, 0);

		private final long millis;
		private final long sequence;

		public StreamId(long millis, long sequence) {
			this.millis = millis;
			this.sequence = sequence;
		}

		private static void checkPositive(String id, long number) {
			if (number < 0) {
				throw new IllegalArgumentException(String.format("not an id: %s", id));
			}
		}

		public static StreamId parse(String id) {
			int off = id.indexOf("-");
			if (off == -1) {
				long millis = Long.parseLong(id);
				checkPositive(id, millis);
				return StreamId.of(millis, 0L);
			}
			long millis = Long.parseLong(id.substring(0, off));
			checkPositive(id, millis);
			long sequence = Long.parseLong(id.substring(off + 1));
			checkPositive(id, sequence);
			return StreamId.of(millis, sequence);
		}

		public static StreamId of(long millis, long sequence) {
			return new StreamId(millis, sequence);
		}

		public String toStreamId() {
			return millis + "-" + sequence;
		}

		@Override
		public String toString() {
			return toStreamId();
		}

		@Override
		public int compareTo(StreamId o) {
			long diff = millis - o.millis;
			if (diff != 0) {
				return Long.signum(diff);
			}
			return Long.signum(sequence - o.sequence);
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			}
			if (!(obj instanceof StreamId)) {
				return false;
			}
			StreamId o = (StreamId) obj;
			return o.millis == millis && o.sequence == sequence;
		}

		@Override
		public int hashCode() {
			long val = millis * 31 * sequence;
			return (int) (val ^ (val >> 32));
		}
	}

	private interface MessageReader<K, V> {

		/**
		 * Reads messages from a stream
		 * 
		 * @param commands Synchronous executed commands for Streams
		 * @param args     Stream read command args
		 * @return list of messages retrieved from the stream or empty list if no
		 *         messages available
		 * @throws Exception
		 */
		List<StreamMessage<K, V>> read(long blockMillis) throws Exception;

	}

	private XReadArgs args(long blockMillis) {
		return XReadArgs.Builder.count(options.getCount()).block(blockMillis);
	}

	private class ExplicitAckPendingMessageReader implements MessageReader<K, V> {

		@SuppressWarnings("unchecked")
		protected List<StreamMessage<K, V>> readMessages(RedisStreamCommands<K, V> commands, XReadArgs args) {
			return recover(commands, commands.xreadgroup(consumer, args, StreamOffset.from(stream, START_OFFSET)));
		}

		protected List<StreamMessage<K, V>> recover(RedisStreamCommands<K, V> commands,
				List<StreamMessage<K, V>> messages) {
			if (messages.isEmpty()) {
				return messages;
			}
			List<StreamMessage<K, V>> recoveredMessages = new ArrayList<>();
			List<StreamMessage<K, V>> messagesToAck = new ArrayList<>();
			StreamId recoveryId = StreamId.parse(lastId);
			for (StreamMessage<K, V> message : messages) {
				StreamId messageId = StreamId.parse(message.getId());
				if (messageId.compareTo(recoveryId) > 0) {
					recoveredMessages.add(message);
					lastId = message.getId();
				} else {
					messagesToAck.add(message);
				}
			}
			ack(commands, messagesToAck);
			return recoveredMessages;
		}

		protected MessageReader<K, V> messageReader() {
			return new ExplicitAckMessageReader();
		}

		@Override
		public List<StreamMessage<K, V>> read(long blockMillis) throws Exception {
			List<StreamMessage<K, V>> messages;
			try (StatefulConnection<K, V> connection = pool.borrowObject()) {
				messages = readMessages(commands(connection), args(blockMillis));
			}
			if (messages.isEmpty()) {
				reader = messageReader();
				return reader.read(blockMillis);
			}
			return messages;
		}

	}

	private class ExplicitAckMessageReader implements MessageReader<K, V> {

		@SuppressWarnings("unchecked")
		@Override
		public List<StreamMessage<K, V>> read(long blockMillis) throws Exception {
			try (StatefulConnection<K, V> connection = pool.borrowObject()) {
				return commands(connection).xreadgroup(consumer, args(blockMillis), StreamOffset.lastConsumed(stream));
			}
		}
	}

	private class AutoAckPendingMessageReader extends ExplicitAckPendingMessageReader {

		@Override
		protected StreamItemReader.MessageReader<K, V> messageReader() {
			return new AutoAckMessageReader();
		}

		@Override
		protected List<StreamMessage<K, V>> recover(RedisStreamCommands<K, V> commands,
				List<StreamMessage<K, V>> messages) {
			ack(commands, messages);
			return Collections.emptyList();
		}

	}

	private class AutoAckMessageReader extends ExplicitAckMessageReader {

		@Override
		public List<StreamMessage<K, V>> read(long blockMillis) throws Exception {
			List<StreamMessage<K, V>> messages = super.read(blockMillis);
			ack(messages);
			return messages;
		}

	}

	private RedisStreamCommands<K, V> commands(StatefulConnection<K, V> connection) {
		return Utils.sync(connection);
	}

	@Override
	public void open(ExecutionContext executionContext) {
		synchronized (pool) {
			try (StatefulConnection<K, V> connection = pool.borrowObject()) {
				RedisStreamCommands<K, V> commands = Utils.sync(connection);
				createConsumerGroup(commands);
				lastId = options.getOffset();
				reader = options.getAckPolicy() == AckPolicy.MANUAL ? new ExplicitAckPendingMessageReader()
						: new AutoAckPendingMessageReader();
				open = true;
			} catch (Exception e) {
				throw new ItemStreamException("Failed to initialize the reader", e);
			}
		}
	}

	private void createConsumerGroup(RedisStreamCommands<K, V> commands) {
		try {
			commands.xgroupCreate(StreamOffset.from(stream, options.getOffset()), consumer.getGroup(),
					XGroupCreateArgs.Builder.mkstream(true));
		} catch (RedisBusyException e) {
			// Consumer Group name already exists, ignore
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
		return poll(DEFAULT_POLL_DURATION.toMillis(), TimeUnit.MILLISECONDS);
	}

	@Override
	public StreamMessage<K, V> poll(long timeout, TimeUnit unit) throws Exception {
		if (!iterator.hasNext()) {
			List<StreamMessage<K, V>> messages = reader.read(unit.toMillis(timeout));
			if (messages == null || messages.isEmpty()) {
				return null;
			}
			iterator = messages.iterator();
		}
		return iterator.next();
	}

	public List<StreamMessage<K, V>> readMessages() throws Exception {
		return reader.read(options.getBlock().toMillis());
	}

	/**
	 * Acks given messages
	 * 
	 * @param messages to be acked
	 * @throws Exception if a connection could not be obtained from the pool
	 */
	public long ack(Iterable<? extends StreamMessage<K, V>> messages) throws Exception {
		if (messages == null) {
			return 0;
		}
		List<String> ids = new ArrayList<>();
		messages.forEach(m -> ids.add(m.getId()));
		return ack(ids.toArray(String[]::new));
	}

	/**
	 * Acks given message ids
	 * 
	 * @param ids message ids to be acked
	 * @return
	 * @throws Exception if a connection could not be obtained from the pool
	 */
	public long ack(String... ids) throws Exception {
		if (ids.length > 0) {
			synchronized (pool) {
				try (StatefulConnection<K, V> connection = pool.borrowObject()) {
					ack(Utils.sync(connection), ids);
				}
				lastId = ids[ids.length - 1];
			}
		}
		return ids.length;
	}

	private void ack(RedisStreamCommands<K, V> commands, Iterable<StreamMessage<K, V>> messages) {
		List<String> ids = new ArrayList<>();
		for (StreamMessage<K, V> message : messages) {
			ids.add(message.getId());
		}
		ack(commands, ids.toArray(String[]::new));
	}

	private void ack(RedisStreamCommands<K, V> commands, String... ids) {
		if (ids.length == 0) {
			return;
		}
		commands.xack(stream, consumer.getGroup(), ids);
	}

	@Override
	public void close() throws ItemStreamException {
		open = false;
	}

}
