package com.redis.spring.batch.reader;

import java.util.function.BiConsumer;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamSupport;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubAdapter;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;

public class RedisClusterKeyspaceNotificationPublisher extends ItemStreamSupport {

	private final RedisClusterClient client;
	private final String pattern;
	private final BiConsumer<String, String> consumer;
	private StatefulRedisClusterPubSubConnection<String, String> connection;
	private Listener<String, String> listener;

	public RedisClusterKeyspaceNotificationPublisher(RedisClusterClient client, String pattern,
			BiConsumer<String, String> consumer) {
		this.client = client;
		this.pattern = pattern;
		this.consumer = consumer;
	}

	@Override
	public synchronized void open(ExecutionContext context) {
		super.open(context);
		if (!isOpen()) {
			doOpen();
		}
	}

	private void doOpen() {
		connection = client.connectPubSub();
		listener = new Listener<>(consumer);
		connection.addListener(listener);
		connection.setNodeMessagePropagation(true);
		connection.sync().upstream().commands().psubscribe(pattern);

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
		connection.sync().upstream().commands().punsubscribe(pattern);
		connection.removeListener(listener);
		connection.close();
		connection = null;
	}

	private static class Listener<K, V> extends RedisClusterPubSubAdapter<K, V> {

		private final BiConsumer<K, V> consumer;

		public Listener(BiConsumer<K, V> consumer) {
			this.consumer = consumer;
		}

		@Override
		public void message(RedisClusterNode node, K channel, V message) {
			consumer.accept(channel, message);
		}

		@Override
		public void message(RedisClusterNode node, K pattern, K channel, V message) {
			consumer.accept(channel, message);
		}
	}

}