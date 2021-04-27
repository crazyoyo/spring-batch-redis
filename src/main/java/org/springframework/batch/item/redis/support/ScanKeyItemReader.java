package org.springframework.batch.item.redis.support;

import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;

import java.util.function.Function;
import java.util.function.Supplier;

public class ScanKeyItemReader<K, V> extends AbstractItemStreamItemReader<K> {

    private final Supplier<StatefulConnection<K, V>> connectionSupplier;
    private final Function<StatefulConnection<K, V>, BaseRedisCommands<K, V>> sync;
    private final String scanMatch;
    private final long scanCount;
    private final String scanType;
    private StatefulConnection<K, V> connection;
    private ScanIterator<K> scanIterator;

    protected ScanKeyItemReader(Supplier<StatefulConnection<K, V>> connectionSupplier, Function<StatefulConnection<K, V>, BaseRedisCommands<K, V>> sync, String scanMatch, long scanCount, String scanType) {
        this.connectionSupplier = connectionSupplier;
        this.sync = sync;
        this.scanMatch = scanMatch;
        this.scanCount = scanCount;
        this.scanType = scanType;
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized void open(ExecutionContext executionContext) {
        super.open(executionContext);
        if (connection == null) {
            connection = connectionSupplier.get();
        }
        if (scanIterator == null) {
            KeyScanArgs args = KeyScanArgs.Builder.limit(scanCount);
            if (scanMatch != null) {
                args.match(scanMatch);
            }
            if (scanType != null) {
                args.type(scanType);
            }
            scanIterator = ScanIterator.scan((RedisKeyCommands<K, V>) sync.apply(connection), args);
        }
    }

    @Override
    public synchronized K read() {
        if (scanIterator.hasNext()) {
            return scanIterator.next();
        }
        return null;
    }

    @Override
    public synchronized void close() {
        if (connection != null) {
            connection.close();
        }
        super.close();
    }

    public static ScanKeyItemReaderBuilder client(RedisClient client) {
        return new ScanKeyItemReaderBuilder(client);
    }

    public static ScanKeyItemReaderBuilder client(RedisClusterClient client) {
        return new ScanKeyItemReaderBuilder(client);
    }

    @Setter
    @Accessors(fluent = true)
    public static class ScanKeyItemReaderBuilder extends CommandBuilder<ScanKeyItemReaderBuilder> {
        public static final String DEFAULT_SCAN_MATCH = "*";
        public static final long DEFAULT_SCAN_COUNT = 1000;

        private String scanMatch = DEFAULT_SCAN_MATCH;
        private long scanCount = DEFAULT_SCAN_COUNT;
        private String scanType;

        public ScanKeyItemReaderBuilder(RedisClient client) {
            super(client);
        }

        public ScanKeyItemReaderBuilder(RedisClusterClient client) {
            super(client);
        }

        public ScanKeyItemReader<String, String> build() {
            return new ScanKeyItemReader<>(connectionSupplier, sync, scanMatch, scanCount, scanType);
        }
    }

}
