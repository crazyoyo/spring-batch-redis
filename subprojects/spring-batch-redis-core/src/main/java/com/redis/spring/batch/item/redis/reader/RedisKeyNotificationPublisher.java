package com.redis.spring.batch.item.redis.reader;

import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public class RedisKeyNotificationPublisher<K, V> implements KeyNotificationPublisher {

	private final RedisClient client;
	private final RedisCodec<K, V> codec;
	private final RedisPubSubListener<K, V> listener;
	private final K pattern;

	private StatefulRedisPubSubConnection<K, V> connection;

	public RedisKeyNotificationPublisher(RedisClient client, RedisCodec<K, V> codec, RedisPubSubListener<K, V> listener,
			K pattern) {
		this.client = client;
		this.codec = codec;
		this.listener = listener;
		this.pattern = pattern;
	}

	@SuppressWarnings("unchecked")
	@Override
	public synchronized void open() {
		if (connection == null) {
			connection = client.connectPubSub(codec);
			connection.addListener(listener);
			connection.sync().psubscribe(pattern);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public synchronized void close() {
		if (connection != null) {
			connection.sync().punsubscribe(pattern);
			connection.removeListener(listener);
			connection.close();
			connection = null;
		}
	}

}