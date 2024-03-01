package com.redis.spring.batch.reader;

import java.util.concurrent.BlockingQueue;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;

public class RedisClusterKeyspaceNotificationListener extends AbstractKeyspaceNotificationListener
		implements RedisClusterPubSubListener<String, String> {

	private final RedisClusterClient client;
	private StatefulRedisClusterPubSubConnection<String, String> connection;

	public RedisClusterKeyspaceNotificationListener(RedisClusterClient client, String pubSubPattern,
			BlockingQueue<String> queue) {
		super(pubSubPattern, queue);
		this.client = client;
	}

	@Override
	protected synchronized void doStart(String pattern) {
		if (connection == null) {
			connection = client.connectPubSub();
			connection.setNodeMessagePropagation(true);
			connection.addListener(this);
			connection.sync().upstream().commands().psubscribe(pattern);
		}
	}

	@Override
	protected synchronized void doClose(String pattern) {
		if (connection != null) {
			connection.sync().upstream().commands().punsubscribe(pattern);
			connection.removeListener(this);
			connection.close();
			connection = null;
		}
	}

	@Override
	public void message(RedisClusterNode node, String channel, String message) {
		// ignore
	}

	@Override
	public void message(RedisClusterNode node, String pattern, String channel, String message) {
		notification(channel, message);
	}

	@Override
	public void subscribed(RedisClusterNode node, String channel, long count) {
		// ignore
	}

	@Override
	public void psubscribed(RedisClusterNode node, String pattern, long count) {
		// ignore
	}

	@Override
	public void unsubscribed(RedisClusterNode node, String channel, long count) {
		// ignore
	}

	@Override
	public void punsubscribed(RedisClusterNode node, String pattern, long count) {
		// ignore
	}

}
