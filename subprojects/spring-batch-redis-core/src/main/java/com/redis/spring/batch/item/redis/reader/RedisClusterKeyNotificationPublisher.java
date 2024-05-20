package com.redis.spring.batch.item.redis.reader;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.codec.RedisCodec;

public class RedisClusterKeyNotificationPublisher<K, V> implements AutoCloseable {

	private final StatefulRedisClusterPubSubConnection<K, V> connection;
	private final RedisClusterPubSubListener<K, V> listener;
	private final K pattern;

	@SuppressWarnings("unchecked")
	public RedisClusterKeyNotificationPublisher(RedisClusterClient client, RedisCodec<K, V> codec,
			RedisClusterPubSubListener<K, V> listener, K pattern) {
		this.connection = client.connectPubSub(codec);
		this.listener = listener;
		this.pattern = pattern;
		this.connection.setNodeMessagePropagation(true);
		this.connection.addListener(listener);
		this.connection.sync().upstream().commands().psubscribe(pattern);
	}

	@SuppressWarnings("unchecked")
	@Override
	public synchronized void close() throws Exception {
		if (connection.isOpen()) {
			connection.sync().upstream().commands().punsubscribe(pattern);
			connection.removeListener(listener);
			connection.close();
		}
	}

}