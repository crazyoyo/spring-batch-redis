package com.redis.spring.batch.support;

import java.util.List;
import java.util.function.Supplier;

import org.springframework.util.Assert;

import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RedisPubSubSubscriber<K, V> implements PubSubSubscriber<K> {

	private final Supplier<StatefulRedisPubSubConnection<K, V>> connectionSupplier;
	private final List<K> patterns;
	private StatefulRedisPubSubConnection<K, V> connection;
	private PubSubListener<K, V> messageListener;

	public RedisPubSubSubscriber(Supplier<StatefulRedisPubSubConnection<K, V>> connectionSupplier, List<K> patterns) {
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
		log.debug("Subscribing to channel patterns {}", patterns);
		connection.sync().psubscribe((K[]) patterns.toArray());
	}

	@SuppressWarnings("unchecked")
	@Override
	public void close() {
		if (connection == null) {
			return;
		}
		log.debug("Unsubscribing from channel pattern {}", patterns);
		connection.sync().punsubscribe((K[]) patterns.toArray());
		log.debug("Removing listener");
		connection.removeListener(messageListener);
		connection.close();
		connection = null;
	}

	private static class PubSubListener<K, V> extends RedisPubSubAdapter<K, V> {

		private final KeyMessageListener<K>[] keyListeners;

		public PubSubListener(KeyMessageListener<K>[] keyListeners) {
			this.keyListeners = keyListeners;
		}

		@Override
		public void message(K channel, V message) {
			for (KeyMessageListener<K> keyListener : keyListeners) {
				keyListener.message(channel);
			}
		}

		@Override
		public void message(K pattern, K channel, V message) {
			for (KeyMessageListener<K> keyListener : keyListeners) {
				keyListener.message(channel);
			}
		}

	}

}
