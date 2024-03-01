package com.redis.spring.batch.reader;

import java.util.concurrent.BlockingQueue;

import io.lettuce.core.RedisClient;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public class RedisKeyspaceNotificationListener extends AbstractKeyspaceNotificationListener
		implements RedisPubSubListener<String, String> {

	private final RedisClient client;
	private StatefulRedisPubSubConnection<String, String> connection;

	public RedisKeyspaceNotificationListener(RedisClient client, BlockingQueue<String> queue, String pubSubPattern) {
		super(pubSubPattern, queue);
		this.client = client;
	}

	@Override
	protected synchronized void doStart(String pattern) {
		if (connection == null) {
			connection = client.connectPubSub();
			connection.addListener(this);
			connection.sync().psubscribe(pattern);
		}
	}

	@Override
	protected synchronized void doClose(String pattern) {
		if (connection != null) {
			connection.sync().punsubscribe(pattern);
			connection.removeListener(this);
			connection.close();
		}
	}

	@Override
	public void message(String channel, String message) {
		// ignore
	}

	@Override
	public void message(String pattern, String channel, String message) {
		notification(channel, message);
	}

	@Override
	public void subscribed(String channel, long count) {
		// ignore
	}

	@Override
	public void psubscribed(String pattern, long count) {
		// ignore
	}

	@Override
	public void unsubscribed(String channel, long count) {
		// ignore
	}

	@Override
	public void punsubscribed(String pattern, long count) {
		// ignore
	}

}
