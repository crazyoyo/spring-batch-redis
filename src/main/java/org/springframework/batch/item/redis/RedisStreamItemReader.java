package org.springframework.batch.item.redis;

import java.util.Collections;
import java.util.Iterator;
import java.util.function.Function;

import org.springframework.batch.item.redis.support.RedisConnectionBuilder;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import lombok.Setter;
import lombok.experimental.Accessors;

public class RedisStreamItemReader<K, V> extends AbstractItemCountingItemStreamItemReader<StreamMessage<K, V>> {

	private final StatefulConnection<K, V> connection;
	private final Function<StatefulConnection<K, V>, BaseRedisCommands<K, V>> commands;
	private final XReadArgs args;
	private StreamOffset<K> offset;
	private Iterator<StreamMessage<K, V>> messages = Collections.emptyIterator();

	public RedisStreamItemReader(StatefulConnection<K, V> connection,
			Function<StatefulConnection<K, V>, BaseRedisCommands<K, V>> commands, XReadArgs args,
			StreamOffset<K> offset) {
		setName(ClassUtils.getShortName(getClass()));
		Assert.notNull(connection, "A connection is required.");
		Assert.notNull(commands, "A commands supplier is required.");
		Assert.notNull(args, "XREAD args are required.");
		Assert.notNull(offset, "Stream offset is required.");
		this.connection = connection;
		this.commands = commands;
		this.args = args;
		this.offset = offset;
	}

	@Override
	protected synchronized void doOpen() {
		// do nothing
	}

	@SuppressWarnings("unchecked")
	@Override
	protected StreamMessage<K, V> doRead() throws Exception {
		while (!messages.hasNext()) {
			messages = ((RedisStreamCommands<K, V>) commands.apply(connection)).xread(args, offset).iterator();
		}
		StreamMessage<K, V> message = messages.next();
		this.offset = StreamOffset.from(message.getStream(), message.getId());
		return message;
	}

	@Override
	protected void doClose() throws Exception {
		// do nothing
	}

	public static RedisStreamItemReaderBuilder<String, String> builder() {
		return new RedisStreamItemReaderBuilder<>(StringCodec.UTF8);
	}

	@Setter
	@Accessors(fluent = true)
	public static class RedisStreamItemReaderBuilder<K, V>
			extends RedisConnectionBuilder<K, V, RedisStreamItemReaderBuilder<K, V>> {

		private XReadArgs args = new XReadArgs();
		private StreamOffset<K> offset;

		public RedisStreamItemReaderBuilder(RedisCodec<K, V> codec) {
			super(codec);
		}

		public RedisStreamItemReader<K, V> build() {
			return new RedisStreamItemReader<>(connection(), sync(), args, offset);
		}

	}

}
