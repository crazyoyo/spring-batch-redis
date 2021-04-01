package org.springframework.batch.item.redis.support;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.core.convert.converter.Converter;

import java.time.Duration;
import java.util.function.Function;

public class LiveKeyValueItemReaderBuilder<B extends LiveKeyValueItemReaderBuilder<B>> extends AbstractKeyValueItemReader.KeyValueItemReaderBuilder<B> {

    public static final int DEFAULT_DATABASE = 0;
    public static final int DEFAULT_NOTIFICATION_QUEUE_CAPACITY = 1000;
    public static final String DEFAULT_KEY_PATTERN = "*";
    private static final Converter<String, String> DEFAULT_KEY_EXTRACTOR = m -> m.substring(m.indexOf(":") + 1);
    private static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";

    private Duration flushingInterval = FlushingStepBuilder.DEFAULT_FLUSHING_INTERVAL;
    private Duration idleTimeout;
    private int notificationQueueCapacity = DEFAULT_NOTIFICATION_QUEUE_CAPACITY;
    private String keyPattern = DEFAULT_KEY_PATTERN;
    private int database = DEFAULT_DATABASE;

    public B flushingInterval(Duration flushingInterval) {
        this.flushingInterval = flushingInterval;
        return (B) this;
    }

    public B idleTimeout(Duration idleTimeout) {
        this.idleTimeout = idleTimeout;
        return (B) this;
    }

    public B notificationQueueCapacity(int notificationQueueCapacity) {
        this.notificationQueueCapacity = notificationQueueCapacity;
        return (B) this;
    }

    public B keyPattern(String keyPattern) {
        this.keyPattern = keyPattern;
        return (B) this;
    }

    public B database(int database) {
        this.database = database;
        return (B) this;
    }

    protected Function<SimpleStepBuilder<String, String>, SimpleStepBuilder<String, String>> stepBuilderProvider() {
        return b -> new FlushingStepBuilder<>(b).flushingInterval(flushingInterval).idleTimeout(idleTimeout);
    }

    protected String pubSubPattern() {
        return String.format(PUBSUB_PATTERN_FORMAT, database, keyPattern);
    }

    protected RedisKeyspaceNotificationItemReader<String, String> keyspaceNotificationReader(StatefulRedisPubSubConnection<String, String> connection) {
        return new RedisKeyspaceNotificationItemReader<>(readTimeout, connection, pubSubPattern(), DEFAULT_KEY_EXTRACTOR, notificationQueueCapacity);
    }

    protected RedisClusterKeyspaceNotificationItemReader<String, String> keyspaceNotificationReader(StatefulRedisClusterPubSubConnection<String, String> connection) {
        return new RedisClusterKeyspaceNotificationItemReader<>(readTimeout, connection, pubSubPattern(), DEFAULT_KEY_EXTRACTOR, notificationQueueCapacity);
    }

}