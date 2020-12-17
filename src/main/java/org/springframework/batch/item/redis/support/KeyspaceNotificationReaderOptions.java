package org.springframework.batch.item.redis.support;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.core.convert.converter.Converter;

import java.time.Duration;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KeyspaceNotificationReaderOptions<K> {

    private K pubSubPattern;
    private Converter<K, K> keyExtractor;
    private int queueCapacity;
    private Duration queuePollingTimeout;

    public static KeyspaceNotificationReaderOptionsBuilder builder() {
        return new KeyspaceNotificationReaderOptionsBuilder();
    }

    @Setter
    @Accessors(fluent = true)
    public static class KeyspaceNotificationReaderOptionsBuilder {

        public static final String DEFAULT_PUBSUB_PATTERN = pubSubPattern(0);
        public static final Converter<String, String> DEFAULT_KEY_EXTRACTOR = m -> m.substring(m.indexOf(":") + 1);
        private static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";

        public static String pubSubPattern(int database) {
            return pubSubPattern(database, AbstractKeyItemReaderBuilder.DEFAULT_SCAN_MATCH);
        }

        public static String pubSubPattern(int database, String keyPattern) {
            return String.format(PUBSUB_PATTERN_FORMAT, database, keyPattern);
        }

        private static final int DEFAULT_QUEUE_CAPACITY = 1000;
        private static final Duration DEFAULT_QUEUE_POLLING_TIMEOUT = Duration.ofMillis(100);

        private String pubSubPattern = DEFAULT_PUBSUB_PATTERN;
        private Converter<String, String> keyExtractor = DEFAULT_KEY_EXTRACTOR;
        private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
        private Duration queuePollingTimeout = DEFAULT_QUEUE_POLLING_TIMEOUT;

        public KeyspaceNotificationReaderOptions<String> build() {
            return new KeyspaceNotificationReaderOptions<>(pubSubPattern, keyExtractor, queueCapacity, queuePollingTimeout);
        }

    }

}
