package com.redis.spring.batch.item.redis.reader;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.codec.RedisCodec;

public class RedisClusterKeyNotificationPublisher<K, V> implements KeyNotificationPublisher {

	private final RedisClusterClient client;
	private final RedisCodec<K, V> codec;
	private final RedisClusterPubSubListener<K, V> listener;
	private final K pattern;

	private StatefulRedisClusterPubSubConnection<K, V> connection;

	public RedisClusterKeyNotificationPublisher(RedisClusterClient client, RedisCodec<K, V> codec,
			RedisClusterPubSubListener<K, V> listener, K pattern) {
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
			connection.setNodeMessagePropagation(true);
			connection.addListener(listener);
			connection.sync().upstream().commands().psubscribe(pattern);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public synchronized void close() {
		if (connection != null) {
			connection.sync().upstream().commands().punsubscribe(pattern);
			connection.removeListener(listener);
			connection.close();
			connection = null;
		}
	}

}