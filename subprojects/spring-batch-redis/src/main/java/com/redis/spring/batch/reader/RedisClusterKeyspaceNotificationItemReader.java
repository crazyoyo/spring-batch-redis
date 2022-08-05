package com.redis.spring.batch.reader;

import java.util.function.Supplier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubAdapter;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;

public class RedisClusterKeyspaceNotificationItemReader<K, V> extends AbstractKeyspaceNotificationItemReader<K> {

	private final Log log = LogFactory.getLog(getClass());

	private final Listener listener = new Listener();
	private final Supplier<StatefulRedisClusterPubSubConnection<K, V>> connectionSupplier;
	private StatefulRedisClusterPubSubConnection<K, V> connection;

	public RedisClusterKeyspaceNotificationItemReader(
			Supplier<StatefulRedisClusterPubSubConnection<K, V>> connectionSupplier, Converter<K, K> keyExtractor,
			K[] patterns) {
		super(keyExtractor, patterns);
		Assert.notNull(connectionSupplier, "A pub/sub connection supplier is required");
		this.connectionSupplier = connectionSupplier;
	}

	@Override
	protected synchronized void doOpen() {
		connection = connectionSupplier.get();
		log.debug("Adding pub/sub listener");
		connection.addListener(listener);
		connection.setNodeMessagePropagation(true);
		log.debug("Subscribing to keyspace notifications");
		connection.sync().upstream().commands().psubscribe(patterns);
	}

	@Override
	protected synchronized void doClose() {
		if (connection == null) {
			return;
		}
		log.debug("Unsubscribing from keyspace notifications");
		connection.sync().upstream().commands().punsubscribe(patterns);
		log.debug("Removing pub/sub listener");
		connection.removeListener(listener);
		connection.close();
		connection = null;
	}

	private class Listener extends RedisClusterPubSubAdapter<K, V> {

		@Override
		public void message(RedisClusterNode node, K channel, V message) {
			RedisClusterKeyspaceNotificationItemReader.this.message(channel);
		}

		@Override
		public void message(RedisClusterNode node, K pattern, K channel, V message) {
			RedisClusterKeyspaceNotificationItemReader.this.message(channel);
		}
	}

}
