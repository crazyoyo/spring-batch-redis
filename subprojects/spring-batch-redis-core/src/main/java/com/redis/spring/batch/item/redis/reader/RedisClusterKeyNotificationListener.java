package com.redis.spring.batch.item.redis.reader;

import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubAdapter;

public class RedisClusterKeyNotificationListener<K, V> extends RedisClusterPubSubAdapter<K, V> {

	private final KeyNotificationConsumer<K, V> consumer;

	public RedisClusterKeyNotificationListener(KeyNotificationConsumer<K, V> consumer) {
		this.consumer = consumer;
	}

	@Override
	public void message(RedisClusterNode node, K pattern, K channel, V message) {
		consumer.accept(channel, message);
	}
}