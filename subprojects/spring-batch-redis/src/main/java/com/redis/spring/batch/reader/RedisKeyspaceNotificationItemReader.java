package com.redis.spring.batch.reader;

import java.util.concurrent.BlockingQueue;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public class RedisKeyspaceNotificationItemReader<K, V> extends AbstractKeyspaceNotificationItemReader<K>
		implements RedisPubSubListener<K, V> {

	private final StatefulRedisPubSubConnection<K, V> connection;

	protected RedisKeyspaceNotificationItemReader(StatefulRedisPubSubConnection<K, V> connection, K[] patterns,
			Converter<K, K> keyExtractor, BlockingQueue<K> queue) {
		super(patterns, keyExtractor, queue);
		Assert.notNull(connection, "A pub/sub connection is required");
		this.connection = connection;
	}

	@Override
	protected void open(K[] patterns) {
		connection.addListener(this);
		connection.sync().psubscribe(patterns);
	}

	@Override
	protected void close(K[] patterns) {
		connection.sync().punsubscribe(patterns);
		connection.removeListener(this);
	}

	@Override
	public void message(K channel, V message) {
		notification(channel);
	}

	@Override
	public void message(K pattern, K channel, V message) {
		notification(channel);
	}

	@Override
	public void psubscribed(K pattern, long count) {
		// do nothing
	}

	@Override
	public void punsubscribed(K pattern, long count) {
		// do nothing
	}

	@Override
	public void subscribed(K channel, long count) {
		// do nothing
	}

	@Override
	public void unsubscribed(K channel, long count) {
		// do nothing
	}

}
