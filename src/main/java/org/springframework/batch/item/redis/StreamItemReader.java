package org.springframework.batch.item.redis;

import java.util.Collections;
import java.util.Iterator;
import java.util.function.Function;

import org.springframework.batch.item.redis.support.ClientUtils;
import org.springframework.batch.item.redis.support.RedisClientBuilder;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisStreamCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

public class StreamItemReader extends AbstractItemCountingItemStreamItemReader<StreamMessage<String, String>> {

	private final AbstractRedisClient client;
	private final Function<StatefulConnection<String, String>, BaseRedisCommands<String, String>> sync;
	private final XReadArgs args;
	private StreamOffset<String> offset;
	private Iterator<StreamMessage<String, String>> messages = Collections.emptyIterator();
	private StatefulConnection<String, String> connection;

	public StreamItemReader(AbstractRedisClient client, XReadArgs args, StreamOffset<String> offset) {
		setName(ClassUtils.getShortName(getClass()));
		Assert.notNull(client, "A Redis client is required.");
		Assert.notNull(args, "XREAD args are required.");
		Assert.notNull(offset, "Stream offset is required.");
		this.client = client;
		this.sync = ClientUtils.sync(client);
		this.args = args;
		this.offset = offset;
	}

	@Override
	protected synchronized void doOpen() {
		this.connection = ClientUtils.connection(client);
	}

	@Override
	protected void doClose() throws Exception {
		if (connection != null) {
			connection.close();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	protected StreamMessage<String, String> doRead() throws Exception {
		while (!messages.hasNext()) {
			messages = ((RedisStreamCommands<String, String>) sync.apply(connection)).xread(args, offset).iterator();
		}
		StreamMessage<String, String> message = messages.next();
		this.offset = StreamOffset.from(message.getStream(), message.getId());
		return message;
	}

	public static StreamItemReaderBuilder builder(AbstractRedisClient client) {
		return new StreamItemReaderBuilder(client);
	}

	@Setter
	@Accessors(fluent = true)
	public static class StreamItemReaderBuilder extends RedisClientBuilder {

		public StreamItemReaderBuilder(AbstractRedisClient client) {
			super(client);
		}

		private XReadArgs args = new XReadArgs();
		private StreamOffset<String> offset;

		public StreamItemReader build() {
			return new StreamItemReader(client, args, offset);
		}

	}

}
