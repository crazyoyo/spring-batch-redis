package com.redis.spring.batch.reader;

import java.util.concurrent.BlockingQueue;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;

public class RedisClusterKeyspaceNotificationItemReader<K, V> extends AbstractKeyspaceNotificationItemReader<K>
		implements RedisClusterPubSubListener<K, V> {

	private final StatefulRedisClusterPubSubConnection<K, V> connection;

	public RedisClusterKeyspaceNotificationItemReader(StatefulRedisClusterPubSubConnection<K, V> connection,
			K[] patterns, Converter<K, K> keyExtractor, BlockingQueue<K> queue) {
		super(patterns, keyExtractor, queue);
		Assert.notNull(connection, "A pub/sub connection is required");
		this.connection = connection;
	}

	@Override
	protected void open(K[] patterns) {
		connection.addListener(this);
		connection.setNodeMessagePropagation(true);
		connection.sync().upstream().commands().psubscribe(patterns);
	}

	@Override
	protected void close(K[] patterns) {
		connection.sync().upstream().commands().punsubscribe(patterns);
		connection.removeListener(this);
	}

	@Override
	public void message(RedisClusterNode node, K channel, V message) {
		notification(channel);
	}

	@Override
	public void message(RedisClusterNode node, K pattern, K channel, V message) {
		notification(channel);
	}

	@Override
	public void subscribed(RedisClusterNode node, K channel, long count) {
		// do nothing
	}

	@Override
	public void psubscribed(RedisClusterNode node, K pattern, long count) {
		// do nothing

	}

	@Override
	public void unsubscribed(RedisClusterNode node, K channel, long count) {
		// do nothing
	}

	@Override
	public void punsubscribed(RedisClusterNode node, K pattern, long count) {
		// do nothing
	}

}
