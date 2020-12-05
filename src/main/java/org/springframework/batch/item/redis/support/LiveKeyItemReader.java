package org.springframework.batch.item.redis.support;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.hybhub.util.concurrent.ConcurrentSetBlockingQueue;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LiveKeyItemReader extends AbstractProgressReportingItemReader<String>
		implements RedisPubSubListener<String, String>, RedisClusterPubSubListener<String, String> {

	private static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";

	private final AbstractRedisClient client;
	private final String pubSubPattern;
	private final BlockingQueue<String> queue;
	private final long queuePollingTimeout;
	private StatefulRedisPubSubConnection<String, String> pubSubConnection;
	private boolean stopped;
	private Tag nameTag;

	public LiveKeyItemReader(AbstractRedisClient client, LiveKeyReaderOptions options) {
		setName(ClassUtils.getShortName(getClass()));
		Assert.notNull(client, "A Redis client is required.");
		Assert.notNull(options, "Options are required.");
		this.client = client;
		this.queue = new ConcurrentSetBlockingQueue<>(options.getQueueOptions().getCapacity());
		this.queuePollingTimeout = options.getQueueOptions().getPollingTimeout().toMillis();
		this.pubSubPattern = String.format(PUBSUB_PATTERN_FORMAT, options.getDatabase(), options.getKeyPattern());
	}

	@Override
	public void setName(String name) {
		this.nameTag = Tag.of("name", name);
		super.setName(name);
	}

	@Override
	protected synchronized void doOpen() throws InterruptedException, ExecutionException, TimeoutException {
		pubSubConnection = ClientUtils.pubSubConnection(client);
		MetricsUtils.createGaugeCollectionSize("livekeyreader.queue.size", queue, nameTag);
		log.info("Subscribing to pub/sub pattern {}, queue capacity: {}", pubSubPattern, queue.remainingCapacity());
		if (pubSubConnection instanceof StatefulRedisClusterPubSubConnection) {
			StatefulRedisClusterPubSubConnection<String, String> clusterPubSubConnection = (StatefulRedisClusterPubSubConnection<String, String>) pubSubConnection;
			clusterPubSubConnection.addListener((RedisClusterPubSubListener<String, String>) this);
			clusterPubSubConnection.setNodeMessagePropagation(true);
			clusterPubSubConnection.sync().upstream().commands().psubscribe(pubSubPattern);
		} else {
			pubSubConnection.addListener(this);
			pubSubConnection.sync().psubscribe(pubSubPattern);
		}
	}

	@Override
	protected synchronized void doClose() {
		log.info("Unsubscribing from pub/sub pattern {}", pubSubPattern);
		if (pubSubConnection instanceof StatefulRedisClusterPubSubConnection) {
			StatefulRedisClusterPubSubConnection<String, String> clusterPubSubConnection = (StatefulRedisClusterPubSubConnection<String, String>) pubSubConnection;
			clusterPubSubConnection.sync().upstream().commands().punsubscribe(pubSubPattern);
			clusterPubSubConnection.removeListener((RedisClusterPubSubListener<String, String>) this);
		} else {
			pubSubConnection.sync().punsubscribe(pubSubPattern);
			pubSubConnection.removeListener(this);
		}
		pubSubConnection.close();
		queue.clear();
	}

	@Override
	public void message(String channel, String message) {
		notification(channel);
	}

	@Override
	public void message(String pattern, String channel, String message) {
		message(channel, message);
	}

	@Override
	public void subscribed(String channel, long count) {
	}

	@Override
	public void psubscribed(String pattern, long count) {
	}

	@Override
	public void unsubscribed(String channel, long count) {
	}

	@Override
	public void punsubscribed(String pattern, long count) {
	}

	@Override
	public void message(RedisClusterNode node, String channel, String message) {
		notification(channel);
	}

	@Override
	public void message(RedisClusterNode node, String pattern, String channel, String message) {
		notification(channel);
	}

	@Override
	public void subscribed(RedisClusterNode node, String channel, long count) {
	}

	@Override
	public void psubscribed(RedisClusterNode node, String pattern, long count) {
	}

	@Override
	public void unsubscribed(RedisClusterNode node, String channel, long count) {
	}

	@Override
	public void punsubscribed(RedisClusterNode node, String pattern, long count) {
	}

	public void stop() {
		this.stopped = true;
	}

	@Override
	protected synchronized String doRead() throws Exception {
		String key;
		do {
			key = queue.poll(queuePollingTimeout, TimeUnit.MILLISECONDS);
		} while (key == null && !stopped);
		return key;
	}

	private String key(String message) {
		int pos = message.indexOf(":");
		return message.substring(pos + 1);
	}

	private void notification(String notification) {
		if (notification == null) {
			return;
		}
		queue.offer(key(notification));
	}

}
