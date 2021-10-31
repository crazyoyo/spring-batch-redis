package com.redis.spring.batch.support;

import java.util.List;
import java.util.function.Supplier;

import org.springframework.util.Assert;

import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubAdapter;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RedisClusterPubSubSubscriber<K, V> implements PubSubSubscriber<K> {

	private final Supplier<StatefulRedisClusterPubSubConnection<K, V>> connectionSupplier;
	private final List<K> patterns;
	private StatefulRedisClusterPubSubConnection<K, V> connection;
	private PubSubListener<K, V> messageListener;

	public RedisClusterPubSubSubscriber(Supplier<StatefulRedisClusterPubSubConnection<K, V>> connectionSupplier,
			List<K> patterns) {
		Assert.notNull(connectionSupplier, "A pub/sub connection supplier is required");
		Assert.notEmpty(patterns, "A pub/sub pattern is required");
		this.connectionSupplier = connectionSupplier;
		this.patterns = patterns;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void open(KeyMessageListener<K>... listeners) {
		connection = connectionSupplier.get();
		messageListener = new PubSubListener<>(listeners);
		log.debug("Adding listener");
		connection.addListener(messageListener);
		connection.setNodeMessagePropagation(true);
		log.debug("Subscribing to channel patterns {}", patterns);
		connection.sync().upstream().commands().psubscribe((K[]) patterns.toArray());
	}

	@SuppressWarnings("unchecked")
	@Override
	public void close() {
		if (connection == null) {
			return;
		}
		log.debug("Unsubscribing from channel patterns {}", patterns);
		connection.sync().upstream().commands().punsubscribe((K[]) patterns.toArray());
		log.debug("Removing listener");
		connection.removeListener(messageListener);
		connection.close();
		connection = null;
	}

	private static class PubSubListener<K, V> extends RedisClusterPubSubAdapter<K, V> {

		private final KeyMessageListener<K>[] listeners;

		public PubSubListener(KeyMessageListener<K>[] listeners) {
			this.listeners = listeners;
		}

		@Override
		public void message(RedisClusterNode node, K channel, V message) {
			for (KeyMessageListener<K> keyListener : listeners) {
				keyListener.message(channel);
			}
		}

		@Override
		public void message(RedisClusterNode node, K pattern, K channel, V message) {
			for (KeyMessageListener<K> keyListener : listeners) {
				keyListener.message(channel);
			}
		}
	}

}
