package org.springframework.batch.item.redis;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractKeyValueItemWriter;
import org.springframework.batch.item.redis.support.CommandTimeoutBuilder;
import org.springframework.batch.item.redis.support.KeyValue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class KeyDumpItemWriter<K, V> extends AbstractKeyValueItemWriter<K, V, KeyValue<K, byte[]>> {

    private final boolean replace;

    public KeyDumpItemWriter(GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout, boolean replace) {
        super(pool, commands, commandTimeout);
        this.replace = replace;
    }


    @Override
    protected List<RedisFuture<?>> write(List<? extends KeyValue<K, byte[]>> items, BaseRedisAsyncCommands<K, V> commands) {
        List<RedisFuture<?>> futures = new ArrayList<>(items.size());
        for (KeyValue<K, byte[]> item : items) {
            futures.add(restore((RedisKeyAsyncCommands<K, V>) commands, item));
        }
        return futures;
    }

    private RedisFuture<?> restore(RedisKeyAsyncCommands<K, V> keyCommands, KeyValue<K, byte[]> item) {
        if (item.getValue() == null || item.noKeyTtl()) {
            return keyCommands.del(item.getKey());
        }
        RestoreArgs restoreArgs = new RestoreArgs().replace(replace);
        if (item.hasTtl()) {
            restoreArgs.ttl(item.getTtl() * 1000);
        }
        return keyCommands.restore(item.getKey(), item.getValue(), restoreArgs);
    }

    @Setter
    @Accessors(fluent = true)
    public static abstract class KeyDumpItemWriterBuilder<K, V> extends CommandTimeoutBuilder<KeyDumpItemWriterBuilder<K, V>> {

        protected boolean replace;

        public abstract KeyDumpItemWriter<K, V> build();

    }

    public static <K, V> KeyDumpItemWriterBuilder<K, V> builder(GenericObjectPool<StatefulRedisConnection<K, V>> pool) {
        return new KeyDumpItemWriterBuilder<K, V>() {
            @Override
            public KeyDumpItemWriter<K, V> build() {
                return new KeyDumpItemWriter<>(pool, c -> ((StatefulRedisConnection<K, V>) c).async(), commandTimeout, replace);
            }
        };
    }

    public static <K, V> KeyDumpItemWriterBuilder<K, V> clusterBuilder(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool) {
        return new KeyDumpItemWriterBuilder<K, V>() {
            @Override
            public KeyDumpItemWriter<K, V> build() {
                return new KeyDumpItemWriter<>(pool, c -> ((StatefulRedisClusterConnection<K, V>) c).async(), commandTimeout, replace);
            }
        };
    }

}
