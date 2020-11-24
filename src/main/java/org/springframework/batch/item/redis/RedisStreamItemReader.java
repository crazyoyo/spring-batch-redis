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
import lombok.Setter;
import lombok.experimental.Accessors;

public class RedisStreamItemReader extends AbstractItemCountingItemStreamItemReader<StreamMessage<String, String>> {

	private final StatefulConnection<String, String> connection;
	private final Function<StatefulConnection<String, String>, BaseRedisCommands<String, String>> commands;
	private final XReadArgs args;
	private StreamOffset<String> offset;
	private Iterator<StreamMessage<String, String>> messages = Collections.emptyIterator();

	public RedisStreamItemReader(StatefulConnection<String, String> connection,
			Function<StatefulConnection<String, String>, BaseRedisCommands<String, String>> commands, XReadArgs args,
			StreamOffset<String> offset) {
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
	protected StreamMessage<String, String> doRead() throws Exception {
		while (!messages.hasNext()) {
			messages = ((RedisStreamCommands<String, String>) commands.apply(connection)).xread(args, offset)
					.iterator();
		}
		StreamMessage<String, String> message = messages.next();
		this.offset = StreamOffset.from(message.getStream(), message.getId());
		return message;
	}

	@Override
	protected void doClose() throws Exception {
		// do nothing
	}

	public static RedisStreamItemReaderBuilder builder() {
		return new RedisStreamItemReaderBuilder();
	}

	@Setter
	@Accessors(fluent = true)
	public static class RedisStreamItemReaderBuilder extends RedisConnectionBuilder<RedisStreamItemReaderBuilder> {

		private XReadArgs args = new XReadArgs();
		private StreamOffset<String> offset;

		public RedisStreamItemReader build() {
			return new RedisStreamItemReader(connection(), sync(), args, offset);
		}

	}

}
