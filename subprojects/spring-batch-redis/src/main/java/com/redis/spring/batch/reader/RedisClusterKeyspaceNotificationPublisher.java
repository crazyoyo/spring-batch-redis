package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.Assert;

import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubAdapter;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;

public class RedisClusterKeyspaceNotificationPublisher<K, V> extends RedisClusterPubSubAdapter<K, V>
		implements KeyspaceNotificationPublisher<K> {

	private final Log log = LogFactory.getLog(getClass());

	private final StatefulRedisClusterPubSubConnection<K, V> connection;
	private final List<KeyspaceNotificationListener<K>> listeners = new ArrayList<>();

	public RedisClusterKeyspaceNotificationPublisher(StatefulRedisClusterPubSubConnection<K, V> connection) {
		Assert.notNull(connection, "A pub/sub connection is required");
		this.connection = connection;
	}

	@Override
	public void addListener(KeyspaceNotificationListener<K> listener) {
		listeners.add(listener);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void subscribe(K... patterns) {
		log.debug("Adding pub/sub listener");
		connection.addListener(this);
		connection.setNodeMessagePropagation(true);
		log.debug("Subscribing to keyspace notifications");
		connection.sync().upstream().commands().psubscribe(patterns);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void unsubscribe(K... patterns) {
		if (connection == null) {
			return;
		}
		log.debug("Unsubscribing from keyspace notifications");
		connection.sync().upstream().commands().punsubscribe(patterns);
		log.debug("Removing pub/sub listener");
		connection.removeListener(this);
	}

	private void notification(K notification) {
		listeners.forEach(l -> l.notification(notification));
	}

	@Override
	public void message(RedisClusterNode node, K channel, V message) {
		notification(channel);
	}

	@Override
	public void message(RedisClusterNode node, K pattern, K channel, V message) {
		notification(channel);
	}

}
