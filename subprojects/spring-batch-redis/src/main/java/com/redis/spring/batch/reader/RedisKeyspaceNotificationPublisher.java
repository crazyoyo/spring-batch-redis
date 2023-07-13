package com.redis.spring.batch.reader;

import java.util.function.BiConsumer;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamSupport;

import io.lettuce.core.RedisClient;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public class RedisKeyspaceNotificationPublisher extends ItemStreamSupport {

	private final RedisClient client;
	private final String pattern;
	private final BiConsumer<String, String> consumer;
	private StatefulRedisPubSubConnection<String, String> connection;
	private Listener<String, String> listener;

	public RedisKeyspaceNotificationPublisher(RedisClient client, String pattern, BiConsumer<String, String> consumer) {
		this.client = client;
		this.pattern = pattern;
		this.consumer = consumer;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (!isOpen()) {
			doOpen();
		}
	}

	private void doOpen() {
		connection = client.connectPubSub();
		connection.sync().psubscribe(pattern);
		listener = new Listener<>(consumer);
		connection.addListener(listener);
	}

	public boolean isOpen() {
		return connection != null;
	}

	@Override
	public synchronized void close() {
		if (isOpen()) {
			doClose();
		}
		super.close();
	}

	private void doClose() {
		connection.sync().punsubscribe(pattern);
		connection.removeListener(listener);
		connection.close();
		connection = null;
	}

	private static class Listener<K, V> extends RedisPubSubAdapter<K, V> {

		private final BiConsumer<K, V> consumer;

		public Listener(BiConsumer<K, V> consumer) {
			this.consumer = consumer;
		}

		@Override
		public void message(K channel, V message) {
			consumer.accept(channel, message);
		}

		@Override
		public void message(K pattern, K channel, V message) {
			consumer.accept(channel, message);
		}
	}

}