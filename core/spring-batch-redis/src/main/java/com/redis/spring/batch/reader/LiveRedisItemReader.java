package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.redis.spring.batch.common.Dump;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Struct;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader.OrderingStrategy;
import com.redis.spring.batch.step.FlushingChunkProvider;
import com.redis.spring.batch.step.FlushingStepBuilder;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class LiveRedisItemReader<K, V, T extends KeyValue<K, ?>> extends AbstractRedisItemReader<K, V, T>
        implements PollableItemReader<T> {

    public static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";

    public static final int DEFAULT_DATABASE = 0;

    public static final String DEFAULT_PUBSUB_PATTERN = pattern(DEFAULT_DATABASE, MATCH_ALL);

    public static final OrderingStrategy DEFAULT_ORDERING = OrderingStrategy.PRIORITY;

    public static final int DEFAULT_NOTIFICATION_QUEUE_CAPACITY = 10000;

    public static final Duration DEFAULT_FLUSHING_INTERVAL = FlushingChunkProvider.DEFAULT_FLUSHING_INTERVAL;

    private int database = DEFAULT_DATABASE;

    private OrderingStrategy orderingStrategy = DEFAULT_ORDERING;

    private int notificationQueueCapacity = DEFAULT_NOTIFICATION_QUEUE_CAPACITY;

    private Duration flushingInterval = DEFAULT_FLUSHING_INTERVAL;

    private Duration idleTimeout;

    public LiveRedisItemReader(AbstractRedisClient client, RedisCodec<K, V> codec, LuaToKeyValueFunction<T> lua) {
        super(client, codec, lua);
    }

    public void setIdleTimeout(Duration idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public void setFlushingInterval(Duration interval) {
        this.flushingInterval = interval;
    }

    public void setNotificationQueueCapacity(int notificationQueueCapacity) {
        this.notificationQueueCapacity = notificationQueueCapacity;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public void setOrderingStrategy(OrderingStrategy orderingStrategy) {
        this.orderingStrategy = orderingStrategy;
    }

    @Override
    protected SimpleStepBuilder<K, K> step(ItemReader<K> reader, ItemWriter<K> writer) {
        FlushingStepBuilder<K, K> flushingStep = new FlushingStepBuilder<>(super.step(reader, writer));
        flushingStep.interval(flushingInterval);
        flushingStep.idleTimeout(idleTimeout);
        return flushingStep;
    }

    @Override
    protected KeyspaceNotificationItemReader<K> keyReader() {
        KeyspaceNotificationItemReader<K> reader = new KeyspaceNotificationItemReader<>(client, codec);
        reader.setKeyType(keyType);
        reader.setOrderingStrategy(orderingStrategy);
        reader.setQueueCapacity(notificationQueueCapacity);
        reader.setPollTimeout(pollTimeout);
        reader.setPattern(pattern(database, keyPattern));
        return reader;
    }

    private static String pattern(int database, String match) {
        return String.format(PUBSUB_PATTERN_FORMAT, database, match == null ? MATCH_ALL : match);
    }

    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    public static <K, V> LiveRedisItemReader<K, V, Struct<K>> struct(AbstractRedisClient client, RedisCodec<K, V> codec) {
        return new LiveRedisItemReader<>(client, codec, new LuaToStructFunction<>(codec));
    }

    public static <K, V> LiveRedisItemReader<K, V, Dump<K>> dump(AbstractRedisClient client, RedisCodec<K, V> codec) {
        return new LiveRedisItemReader<>(client, codec, new LuaToDumpFunction<>());
    }

}
