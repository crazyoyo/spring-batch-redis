package com.redis.spring.batch.reader;

import java.util.function.Supplier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public class RedisKeyspaceNotificationItemReader<K, V> extends AbstractKeyspaceNotificationItemReader<K> {

	private final Log log = LogFactory.getLog(getClass());

	private final Listener listener = new Listener();
	private final Supplier<StatefulRedisPubSubConnection<K, V>> connectionSupplier;
	private StatefulRedisPubSubConnection<K, V> connection;

	public RedisKeyspaceNotificationItemReader(Supplier<StatefulRedisPubSubConnection<K, V>> connectionSupplier,
			Converter<K, K> keyExtractor, K[] patterns) {
		super(keyExtractor, patterns);
		Assert.notNull(connectionSupplier, "A pub/sub connection supplier is required");
		this.connectionSupplier = connectionSupplier;
	}

	@Override
	protected synchronized void doOpen() {
		connection = connectionSupplier.get();
		log.debug("Adding listener");
		connection.addListener(listener);
		log.debug("Subscribing to keyspace notifications");
		connection.sync().psubscribe(patterns);
	}

	@Override
	protected synchronized void doClose() {
		if (connection == null) {
			return;
		}
		log.debug("Unsubscribing from keyspace notifications");
		connection.sync().punsubscribe(patterns);
		log.debug("Removing listener");
		connection.removeListener(listener);
		connection.close();
		connection = null;
	}

	private class Listener extends RedisPubSubAdapter<K, V> {

		@Override
		public void message(K channel, V message) {
			RedisKeyspaceNotificationItemReader.this.message(channel);
		}

		@Override
		public void message(K pattern, K channel, V message) {
			RedisKeyspaceNotificationItemReader.this.message(channel);
		}

	}

}
