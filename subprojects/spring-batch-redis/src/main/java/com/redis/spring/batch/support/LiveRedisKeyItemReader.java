package com.redis.spring.batch.support;

import java.util.List;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public class LiveRedisKeyItemReader<K, V> extends LiveKeyItemReader<K> {

	private static final Logger log = LoggerFactory.getLogger(LiveRedisKeyItemReader.class);

	private final Listener listener = new Listener();
	private final Supplier<StatefulRedisPubSubConnection<K, V>> connectionSupplier;
	private StatefulRedisPubSubConnection<K, V> connection;

	public LiveRedisKeyItemReader(Supplier<StatefulRedisPubSubConnection<K, V>> connectionSupplier,
			Converter<K, K> keyExtractor, List<K> patterns) {
		super(keyExtractor, patterns);
		Assert.notNull(connectionSupplier, "A pub/sub connection supplier is required");
		this.connectionSupplier = connectionSupplier;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected synchronized void doOpen() {
		connection = connectionSupplier.get();
		log.debug("Adding listener");
		connection.addListener(listener);
		log.debug("Subscribing to channel patterns {}", patterns);
		connection.sync().psubscribe((K[]) patterns.toArray());
	}

	@SuppressWarnings("unchecked")
	@Override
	protected synchronized void doClose() {
		if (connection == null) {
			return;
		}
		log.debug("Unsubscribing from channel pattern {}", patterns);
		connection.sync().punsubscribe((K[]) patterns.toArray());
		log.debug("Removing listener");
		connection.removeListener(listener);
		connection.close();
		connection = null;
	}

	private class Listener extends RedisPubSubAdapter<K, V> {

		@Override
		public void message(K channel, V message) {
			LiveRedisKeyItemReader.this.message(channel);
		}

		@Override
		public void message(K pattern, K channel, V message) {
			LiveRedisKeyItemReader.this.message(channel);
		}

	}

}
