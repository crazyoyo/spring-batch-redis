package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.spring.batch.common.PollableItemReader;
import com.redis.spring.batch.util.BatchUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.Consumer;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisBusyException;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.codec.RedisCodec;

public class StreamItemReader<K, V> extends AbstractItemCountingItemStreamItemReader<StreamMessage<K, V>>
		implements PollableItemReader<StreamMessage<K, V>> {

	public enum AckPolicy {
		AUTO, MANUAL
	}

	public static final Duration DEFAULT_POLL_DURATION = Duration.ofSeconds(1);
	public static final String DEFAULT_OFFSET = "0-0";
	public static final Duration DEFAULT_BLOCK = Duration.ofMillis(100);
	public static final long DEFAULT_COUNT = 50;
	public static final AckPolicy DEFAULT_ACK_POLICY = AckPolicy.AUTO;

	private final AbstractRedisClient client;
	private final RedisCodec<K, V> codec;
	private final K stream;
	private final Consumer<K> consumer;

	private String offset = DEFAULT_OFFSET;
	private Duration block = DEFAULT_BLOCK;
	private long count = DEFAULT_COUNT;
	private AckPolicy ackPolicy = DEFAULT_ACK_POLICY;
	private StatefulRedisModulesConnection<K, V> connection;
	private Iterator<StreamMessage<K, V>> iterator = Collections.emptyIterator();
	private MessageReader<K, V> messageReader;
	private String lastId;
	private RedisStreamCommands<K, V> commands;
	private ReadFrom readFrom;

	public StreamItemReader(AbstractRedisClient client, RedisCodec<K, V> codec, K stream, Consumer<K> consumer) {
		this.client = client;
		this.codec = codec;
		this.stream = stream;
		this.consumer = consumer;
	}

	private XReadArgs args(long blockMillis) {
		return XReadArgs.Builder.count(count).block(blockMillis);
	}

	@Override
	protected synchronized void doOpen() throws Exception {
		if (messageReader == null) {
			connection = BatchUtils.connection(client, codec, readFrom);
			commands = connection.sync();
			StreamOffset<K> streamOffset = StreamOffset.from(stream, offset);
			XGroupCreateArgs args = XGroupCreateArgs.Builder.mkstream(true);
			try {
				commands.xgroupCreate(streamOffset, consumer.getGroup(), args);
			} catch (RedisBusyException e) {
				// Consumer Group name already exists, ignore
			}
			lastId = offset;
			messageReader = reader();
		}
	}

	@Override
	protected synchronized void doClose() throws Exception {
		messageReader = null;
		lastId = null;
		if (connection != null) {
			connection.close();
			connection = null;
		}
		commands = null;
	}

	private MessageReader<K, V> reader() {
		if (ackPolicy == AckPolicy.MANUAL) {
			return new ExplicitAckPendingMessageReader();
		}
		return new AutoAckPendingMessageReader();
	}

	@Override
	protected StreamMessage<K, V> doRead() throws Exception {
		return poll(DEFAULT_POLL_DURATION.toMillis(), TimeUnit.MILLISECONDS);
	}

	@Override
	public synchronized StreamMessage<K, V> poll(long timeout, TimeUnit unit) {
		if (!iterator.hasNext()) {
			List<StreamMessage<K, V>> messages = messageReader.read(unit.toMillis(timeout));
			if (messages == null || messages.isEmpty()) {
				return null;
			}
			iterator = messages.iterator();
		}
		return iterator.next();
	}

	public List<StreamMessage<K, V>> readMessages() {
		return messageReader.read(block.toMillis());
	}

	/**
	 * Acks given messages
	 * 
	 * @param messages to be acked
	 */
	public Long ack(Iterable<? extends StreamMessage<K, V>> messages) {
		if (messages == null) {
			return 0L;
		}
		Stream<String> ids = StreamSupport.stream(messages.spliterator(), false).map(StreamMessage::getId);
		return doAck(ids.toArray(String[]::new));
	}

	/**
	 * Acks given message ids
	 * 
	 * @param ids message ids to be acked
	 * @return
	 */
	public Long ack(String... ids) {
		if (ids.length == 0) {
			return 0L;
		}
		lastId = ids[ids.length - 1];
		return doAck(ids);
	}

	private Long doAck(String... ids) {
		if (ids.length == 0) {
			return 0L;
		}
		return commands.xack(stream, consumer.getGroup(), ids);
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
		 * @param redisCommands Synchronous executed commands for Streams
		 * @param args          Stream read command args
		 * @return list of messages retrieved from the stream or empty list if no
		 *         messages available
		 * @throws MessageReadException
		 */
		List<StreamMessage<K, V>> read(long blockMillis);

	}

	private class ExplicitAckPendingMessageReader implements MessageReader<K, V> {

		@SuppressWarnings("unchecked")
		protected List<StreamMessage<K, V>> readMessages(XReadArgs args) {
			return recover(commands.xreadgroup(consumer, args, StreamOffset.from(stream, DEFAULT_OFFSET)));
		}

		protected List<StreamMessage<K, V>> recover(List<StreamMessage<K, V>> messages) {
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
			ack(messagesToAck);
			return recoveredMessages;
		}

		protected MessageReader<K, V> messageReader() {
			return new ExplicitAckMessageReader();
		}

		@Override
		public List<StreamMessage<K, V>> read(long blockMillis) {
			List<StreamMessage<K, V>> messages;
			messages = readMessages(args(blockMillis));
			if (messages.isEmpty()) {
				messageReader = messageReader();
				return messageReader.read(blockMillis);
			}
			return messages;
		}

	}

	private class ExplicitAckMessageReader implements MessageReader<K, V> {

		@SuppressWarnings("unchecked")
		@Override
		public List<StreamMessage<K, V>> read(long blockMillis) {
			return commands.xreadgroup(consumer, args(blockMillis), StreamOffset.lastConsumed(stream));
		}

	}

	private class AutoAckPendingMessageReader extends ExplicitAckPendingMessageReader {

		@Override
		protected StreamItemReader.MessageReader<K, V> messageReader() {
			return new AutoAckMessageReader();
		}

		@Override
		protected List<StreamMessage<K, V>> recover(List<StreamMessage<K, V>> messages) {
			ack(messages);
			return Collections.emptyList();
		}

	}

	private class AutoAckMessageReader extends ExplicitAckMessageReader {

		@Override
		public List<StreamMessage<K, V>> read(long blockMillis) {
			List<StreamMessage<K, V>> messages = super.read(blockMillis);
			ack(messages);
			return messages;
		}

	}

	public long streamLength() {
		return commands.xlen(stream);
	}

	public ReadFrom getReadFrom() {
		return readFrom;
	}

	public void setReadFrom(ReadFrom readFrom) {
		this.readFrom = readFrom;
	}

	public String getOffset() {
		return offset;
	}

	public void setOffset(String offset) {
		this.offset = offset;
	}

	public Duration getBlock() {
		return block;
	}

	public void setBlock(Duration block) {
		this.block = block;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public AckPolicy getAckPolicy() {
		return ackPolicy;
	}

	public void setAckPolicy(AckPolicy policy) {
		this.ackPolicy = policy;
	}
}
