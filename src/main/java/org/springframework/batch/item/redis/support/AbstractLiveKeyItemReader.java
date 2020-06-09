package org.springframework.batch.item.redis.support;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.text.StringSubstitutor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
public abstract class AbstractLiveKeyItemReader<K, V, C extends StatefulConnection<K, V>> extends AbstractKeyItemReader<K, V, C> {

    public static final Converter<String, String> DEFAULT_CHANNEL_KEY_CONVERTER = c -> c.substring(c.indexOf(":") + 1);
    public static final int DEFAULT_QUEUE_CAPACITY = 10000;
    public static final long DEFAULT_QUEUE_POLLING_TIMEOUT = 100;

    public static final String DEFAULT_PATTERN = "*";
    public static final String KEYSPACE_CHANNEL_TEMPLATE = "__keyspace@${database}__:${pattern}";

    protected static String[] channels(int database, String scanPattern, String[] patterns) {
        String[] actualPatterns = patterns == null || patterns.length == 0 ? new String[]{scanPattern} : patterns;
        String[] channels = new String[actualPatterns.length];
        for (int index = 0; index < actualPatterns.length; index++) {
            channels[index] = channel(database, actualPatterns[index]);
        }
        return channels;
    }

    private static String channel(int database, String pattern) {
        Map<String, String> variables = new HashMap<>();
        variables.put("database", String.valueOf(database));
        variables.put("pattern", pattern == null ? DEFAULT_PATTERN : pattern);
        StringSubstitutor substitutor = new StringSubstitutor(variables);
        return substitutor.replace(KEYSPACE_CHANNEL_TEMPLATE);
    }

    private final K[] patterns;
    private final long queuePollingTimeout;
    private final Converter<K, K> channelToKeyConverter;
    private final BlockingQueue<K> queue;
    @Getter
    private boolean stopped;

    protected AbstractLiveKeyItemReader(C connection, Function<C, BaseRedisCommands<K, V>> commands, Long scanCount, String scanPattern, K[] patterns, Integer queueCapacity, Long queuePollingTimeout, Converter<K, K> channelToKeyConverter) {
        super(connection, commands, scanCount, scanPattern);
        Assert.isTrue(patterns != null && patterns.length > 0, "Patterns are required.");
        Assert.notNull(channelToKeyConverter, "Channel -> key converter is required.");
        this.patterns = patterns;
        this.queue = new LinkedBlockingDeque<>(queueCapacity == null ? DEFAULT_QUEUE_CAPACITY : queueCapacity);
        this.queuePollingTimeout = queuePollingTimeout == null ? DEFAULT_QUEUE_POLLING_TIMEOUT : queuePollingTimeout;
        this.channelToKeyConverter = channelToKeyConverter;
    }

    @Override
    protected synchronized void doOpen() {
        open(patterns);
        super.doOpen();
    }

    protected abstract void open(K[] patterns);

    public void stop() {
        this.stopped = true;
    }

    @Override
    protected synchronized void doClose() {
        super.doClose();
        close(patterns);
    }

    protected abstract void close(K[] patterns);

    @Override
    protected synchronized K doRead() throws Exception {
        if (queue.isEmpty()) {
            K key = super.doRead();
            if (key != null) {
                return key;
            }
        }
        K key;
        do {
            key = queue.poll(queuePollingTimeout, TimeUnit.MILLISECONDS);
        } while (key == null && !stopped);
        return key;
    }

    protected void enqueue(K channel) {
        K key = channelToKeyConverter.convert(channel);
        if (key == null) {
            return;
        }
        try {
            queue.put(key);
        } catch (InterruptedException e) {
            // ignore
        }
    }

}
