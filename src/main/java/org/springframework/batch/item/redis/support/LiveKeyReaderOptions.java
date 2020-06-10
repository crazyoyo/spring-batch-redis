package org.springframework.batch.item.redis.support;

import io.lettuce.core.ScanArgs;
import lombok.*;
import lombok.experimental.Accessors;
import org.apache.commons.text.StringSubstitutor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LiveKeyReaderOptions<K> {


    private static final ScanArgs DEFAULT_SCAN_ARGS = new ScanArgs();
    public static final int DEFAULT_QUEUE_CAPACITY = 10000;
    public static final long DEFAULT_QUEUE_POLLING_TIMEOUT = 100;

    private ScanArgs scanArgs = DEFAULT_SCAN_ARGS;
    private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
    private long queuePollingTimeout = DEFAULT_QUEUE_POLLING_TIMEOUT;
    private Converter<K, K> channelConverter;
    private K pubsubPattern;

    public static LiveKeyReaderOptionsBuilder builder() {
        return new LiveKeyReaderOptionsBuilder();
    }

    @Setter
    @Accessors(fluent = true)
    public static class LiveKeyReaderOptionsBuilder {

        public static final int DEFAULT_DATABASE = 0;
        public static final String DEFAULT_KEY_PATTERN = "*";
        public static final String KEYSPACE_CHANNEL_TEMPLATE = "__keyspace@${database}__:${pattern}";
        public static final Converter<String, String> DEFAULT_CHANNEL_CONVERTER = new StringChannelConverter();

        private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
        private long queuePollingTimeout = DEFAULT_QUEUE_POLLING_TIMEOUT;
        private int database = DEFAULT_DATABASE;
        private String keyPattern = DEFAULT_KEY_PATTERN;
        private Converter<String, String> channelConverter = DEFAULT_CHANNEL_CONVERTER;

        public LiveKeyReaderOptions<String> build() {
            Assert.notNull(channelConverter, "A channel converter is required.");
            return new LiveKeyReaderOptions<>(ScanArgs.Builder.matches(keyPattern), queueCapacity, queuePollingTimeout, channelConverter, pubsubPattern(database, keyPattern));
        }

        private static String pubsubPattern(int database, String keyPattern) {
            Assert.notNull(keyPattern, "A key pattern is required.");
            Map<String, String> variables = new HashMap<>();
            variables.put("database", String.valueOf(database));
            variables.put("pattern", keyPattern);
            StringSubstitutor substitutor = new StringSubstitutor(variables);
            return substitutor.replace(KEYSPACE_CHANNEL_TEMPLATE);
        }

    }

}
