package com.redis.spring.batch.support;

import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;

import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClusterKeyspaceNotificationItemReader
		extends AbstractKeyspaceNotificationItemReader<StatefulRedisClusterPubSubConnection<String, String>>
		implements RedisClusterPubSubListener<String, String> {

	public ClusterKeyspaceNotificationItemReader(
			Supplier<StatefulRedisClusterPubSubConnection<String, String>> connectionSupplier,
			List<String> pubSubPatterns, int queueCapacity, Duration defaultQueuePollTimeout) {
		super(connectionSupplier, pubSubPatterns, queueCapacity, defaultQueuePollTimeout);
	}

	@Override
	protected void subscribe(StatefulRedisClusterPubSubConnection<String, String> connection, List<String> patterns) {
		log.debug("Adding listener");
		connection.addListener(this);
		connection.setNodeMessagePropagation(true);
		log.debug("Subscribing to channel patterns {}", patterns);
		connection.sync().upstream().commands().psubscribe(patterns.toArray(new String[0]));
	}

	@Override
	public void message(RedisClusterNode node, String channel, String message) {
		add(channel);
	}

	@Override
	public void message(RedisClusterNode node, String pattern, String channel, String message) {
		add(channel);
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

	@Override
	protected void unsubscribe(StatefulRedisClusterPubSubConnection<String, String> connection, List<String> patterns) {
		log.debug("Unsubscribing from channel patterns {}", patterns);
		connection.sync().upstream().commands().punsubscribe(patterns.toArray(new String[0]));
		log.debug("Removing listener");
		connection.removeListener(this);
	}

}
