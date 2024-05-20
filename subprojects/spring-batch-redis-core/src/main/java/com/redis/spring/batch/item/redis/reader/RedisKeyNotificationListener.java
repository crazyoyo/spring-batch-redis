package com.redis.spring.batch.item.redis.reader;

import io.lettuce.core.pubsub.RedisPubSubAdapter;

public class RedisKeyNotificationListener<K, V> extends RedisPubSubAdapter<K, V> {

	private final KeyNotificationConsumer<K, V> consumer;

	public RedisKeyNotificationListener(KeyNotificationConsumer<K, V> consumer) {
		this.consumer = consumer;
	}

	@Override
	public void message(K pattern, K channel, V message) {
		consumer.accept(channel, message);
	}

}
