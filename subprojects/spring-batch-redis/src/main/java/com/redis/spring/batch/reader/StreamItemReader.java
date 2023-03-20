package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.common.Utils;
import com.redis.spring.batch.reader.StreamReaderOptions.AckPolicy;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisBusyException;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class StreamItemReader<K, V> extends AbstractItemCountingItemStreamItemReader<StreamMessage<K, V>>
		implements PollableItemReader<StreamMessage<K, V>> {

	public static final Duration DEFAULT_POLL_DURATION = Duration.ofSeconds(1);
	public static final String START_OFFSET = "0-0";

	private final AbstractRedisClient client;
	private final RedisCodec<K, V> codec;
	private final K stream;
	private final Consumer<K> consumer;
	private final StreamReaderOptions options;
	private StatefulRedisModulesConnection<K, V> connection;
	private Iterator<StreamMessage<K, V>> iterator = Collections.emptyIterator();
	private MessageReader<K, V> reader;
	private String lastId;

	public StreamItemReader(AbstractRedisClient client, RedisCodec<K, V> codec, K stream, Consumer<K> consumer,
			StreamReaderOptions options) {
		setName(ClassUtils.getShortName(getClass()));
		this.client = client;
		this.codec = codec;
		this.stream = stream;
		this.consumer = consumer;
		this.options = options;
	}

	private XReadArgs args(long blockMillis) {
		return XReadArgs.Builder.count(options.getCount()).block(blockMillis);
	}

	private RedisStreamCommands<K, V> commands(StatefulConnection<K, V> connection) {
		return Utils.sync(connection);
	}

	@Override
	protected void doOpen() throws Exception {
		if (isOpen()) {
			return;
		}
		connection = RedisModulesUtils.connection(client, codec);
		createConsumerGroup();
		lastId = options.getOffset();
		reader = reader();
	}

	private MessageReader<K, V> reader() {
		if (options.getAckPolicy() == AckPolicy.MANUAL) {
			return new ExplicitAckPendingMessageReader();
		}
		return new AutoAckPendingMessageReader();
	}

	private void createConsumerGroup() {
		RedisStreamCommands<K, V> commands = Utils.sync(connection);
		StreamOffset<K> offset = StreamOffset.from(stream, options.getOffset());
		XGroupCreateArgs args = XGroupCreateArgs.Builder.mkstream(true);
		try {
			commands.xgroupCreate(offset, consumer.getGroup(), args);
		} catch (RedisBusyException e) {
			// Consumer Group name already exists, ignore
		}
	}

	@Override
	public boolean isOpen() {
		return reader != null;
	}

	@Override
	public void update(ExecutionContext executionContext) throws ItemStreamException {
		// Do nothing
	}

	@Override
	protected StreamMessage<K, V> doRead() throws Exception {
		return poll(DEFAULT_POLL_DURATION.toMillis(), TimeUnit.MILLISECONDS);
	}

	@Override
	public StreamMessage<K, V> poll(long timeout, TimeUnit unit) throws PollingException {
		if (!iterator.hasNext()) {
			List<StreamMessage<K, V>> messages = reader.read(unit.toMillis(timeout));
			if (messages == null || messages.isEmpty()) {
				return null;
			}
			iterator = messages.iterator();
		}
		return iterator.next();
	}

	public List<StreamMessage<K, V>> readMessages() {
		return reader.read(options.getBlock().toMillis());
	}

	/**
	 * Acks given messages
	 * 
	 * @param messages to be acked
	 * @throws MessageAckException
	 * @throws MessageAckException if any error occurs while trying to ack messages
	 */
	public long ack(Iterable<? extends StreamMessage<K, V>> messages) {
		if (messages == null) {
			return 0;
		}
		List<String> ids = new ArrayList<>();
		messages.forEach(m -> ids.add(m.getId()));
		return ack(ids.toArray(new String[0]));
	}

	/**
	 * Acks given message ids
	 * 
	 * @param ids message ids to be acked
	 * @return
	 * @throws MessageAckException if any error occurs while trying to ack IDs
	 */
	public long ack(String... ids) {
		if (ids.length == 0) {
			return 0;
		}
		long count = ack(Utils.sync(connection), ids);
		lastId = ids[ids.length - 1];
		return count;
	}

	private void ack(RedisStreamCommands<K, V> commands, Iterable<StreamMessage<K, V>> messages) {
		List<String> ids = new ArrayList<>();
		for (StreamMessage<K, V> message : messages) {
			ids.add(message.getId());
		}
		ack(commands, ids.toArray(new String[0]));
	}

	private Long ack(RedisStreamCommands<K, V> commands, String... ids) {
		if (ids.length == 0) {
			return 0L;
		}
		return commands.xack(stream, consumer.getGroup(), ids);
	}

	@Override
	protected void doClose() throws Exception {
		if (isOpen()) {
			reader = null;
			lastId = null;
		}
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
		 * @throws MessageReadException
		 */
		List<StreamMessage<K, V>> read(long blockMillis);

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
		public List<StreamMessage<K, V>> read(long blockMillis) {
			List<StreamMessage<K, V>> messages;
			messages = readMessages(commands(connection), args(blockMillis));
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
		public List<StreamMessage<K, V>> read(long blockMillis) {
			return commands(connection).xreadgroup(consumer, args(blockMillis), StreamOffset.lastConsumed(stream));
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
		public List<StreamMessage<K, V>> read(long blockMillis) {
			List<StreamMessage<K, V>> messages = super.read(blockMillis);
			ack(messages);
			return messages;
		}

	}

	public static StreamBuilder<String, String> client(RedisModulesClient client) {
		return new StreamBuilder<>(client, StringCodec.UTF8);
	}

	public static StreamBuilder<String, String> client(RedisModulesClusterClient client) {
		return new StreamBuilder<>(client, StringCodec.UTF8);
	}

	public static <K, V> StreamBuilder<K, V> client(RedisModulesClient client, RedisCodec<K, V> codec) {
		return new StreamBuilder<>(client, codec);
	}

	public static <K, V> StreamBuilder<K, V> client(RedisModulesClusterClient client, RedisCodec<K, V> codec) {
		return new StreamBuilder<>(client, codec);
	}

	public static class StreamBuilder<K, V> {
		private final AbstractRedisClient client;
		private final RedisCodec<K, V> codec;

		protected StreamBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			this.client = client;
			this.codec = codec;
		}

		public Builder<K, V> stream(K stream) {
			return new Builder<>(client, codec, stream);
		}
	}

	public static class Builder<K, V> {

		public static final String DEFAULT_CONSUMER_GROUP = ClassUtils.getShortName(StreamItemReader.class);
		public static final String DEFAULT_CONSUMER = "consumer1";

		private final AbstractRedisClient client;
		private final RedisCodec<K, V> codec;
		private final K stream;
		private Consumer<K> consumer;
		private StreamReaderOptions options = StreamReaderOptions.builder().build();

		protected Builder(AbstractRedisClient client, RedisCodec<K, V> codec, K stream) {
			this.client = client;
			this.codec = codec;
			this.consumer = Consumer.from(key(DEFAULT_CONSUMER_GROUP, codec), key(DEFAULT_CONSUMER, codec));
			this.stream = stream;
		}

		private static <K, V> K key(String string, RedisCodec<K, V> codec) {
			return codec.decodeKey(StringCodec.UTF8.encodeKey(string));
		}

		public Builder<K, V> consumer(Consumer<K> consumer) {
			this.consumer = consumer;
			return this;
		}

		public Builder<K, V> options(StreamReaderOptions options) {
			this.options = options;
			return this;
		}

		public StreamItemReader<K, V> build() {
			return new StreamItemReader<>(client, codec, stream, consumer, options);
		}

	}

}
