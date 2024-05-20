package com.redis.spring.batch.item.redis.reader;

import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public class RedisKeyNotificationPublisher<K, V> implements AutoCloseable {

	private final StatefulRedisPubSubConnection<K, V> connection;
	private final K pattern;
	private final RedisPubSubListener<K, V> listener;

	@SuppressWarnings("unchecked")
	public RedisKeyNotificationPublisher(RedisClient client, RedisCodec<K, V> codec, RedisPubSubListener<K, V> listener,
			K pattern) {
		this.connection = client.connectPubSub(codec);
		this.listener = listener;
		this.pattern = pattern;
		connection.addListener(listener);
		connection.sync().psubscribe(pattern);
	}

	@SuppressWarnings("unchecked")
	@Override
	public synchronized void close() {
		if (connection.isOpen()) {
			connection.sync().punsubscribe(pattern);
			connection.removeListener(listener);
			connection.close();
		}
	}

}