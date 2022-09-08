package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.Assert;

import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public class RedisKeyspaceNotificationPublisher<K, V> extends RedisPubSubAdapter<K, V>
		implements KeyspaceNotificationPublisher<K> {

	private final Log log = LogFactory.getLog(getClass());

	private final StatefulRedisPubSubConnection<K, V> connection;
	private final List<KeyspaceNotificationListener<K>> listeners = new ArrayList<>();

	public RedisKeyspaceNotificationPublisher(StatefulRedisPubSubConnection<K, V> connection) {
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
		log.debug("Adding listener");
		connection.addListener(this);
		log.debug("Subscribing to keyspace notifications");
		connection.sync().psubscribe(patterns);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void unsubscribe(K... patterns) {
		log.debug("Unsubscribing from keyspace notifications");
		connection.sync().punsubscribe(patterns);
		log.debug("Removing listener");
		connection.removeListener(this);
		connection.close();
	}

	@Override
	public void message(K channel, V message) {
		notification(channel);
	}

	private void notification(K notification) {
		listeners.forEach(l -> l.notification(notification));
	}

	@Override
	public void message(K pattern, K channel, V message) {
		notification(channel);
	}

}
