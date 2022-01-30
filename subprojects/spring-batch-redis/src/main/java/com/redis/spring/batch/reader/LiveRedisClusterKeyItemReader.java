package com.redis.spring.batch.reader;

import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubAdapter;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;

public class LiveRedisClusterKeyItemReader<K, V> extends LiveKeyItemReader<K> {

	private static final Logger log = LoggerFactory.getLogger(LiveRedisClusterKeyItemReader.class);

	private final Listener listener = new Listener();
	private final Supplier<StatefulRedisClusterPubSubConnection<K, V>> connectionSupplier;
	private StatefulRedisClusterPubSubConnection<K, V> connection;

	public LiveRedisClusterKeyItemReader(Supplier<StatefulRedisClusterPubSubConnection<K, V>> connectionSupplier,
			Converter<K, K> keyExtractor, K[] patterns) {
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
		log.debug("Subscribing to channel patterns {}", patterns);
		connection.sync().upstream().commands().psubscribe(patterns);
	}

	@Override
	protected synchronized void doClose() {
		if (connection == null) {
			return;
		}
		log.debug("Unsubscribing from channel patterns {}", patterns);
		connection.sync().upstream().commands().punsubscribe(patterns);
		log.debug("Removing pub/sub listener");
		connection.removeListener(listener);
		connection.close();
		connection = null;
	}

	private class Listener extends RedisClusterPubSubAdapter<K, V> {

		@Override
		public void message(RedisClusterNode node, K channel, V message) {
			LiveRedisClusterKeyItemReader.this.message(channel);
		}

		@Override
		public void message(RedisClusterNode node, K pattern, K channel, V message) {
			LiveRedisClusterKeyItemReader.this.message(channel);
		}
	}

}
