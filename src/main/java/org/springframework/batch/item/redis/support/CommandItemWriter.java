package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public class CommandItemWriter<K, V, T> extends AbstractRedisItemWriter<K, V, T> {

    private final BiFunction<BaseRedisAsyncCommands<K, V>, T, RedisFuture<?>> command;

    public CommandItemWriter(GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> async, BiFunction<BaseRedisAsyncCommands<K, V>, T, RedisFuture<?>> command) {
        super(pool, async);
        this.command = command;
    }

    @Override
    protected List<RedisFuture<?>> write(List<? extends T> items, BaseRedisAsyncCommands<K, V> commands) {
        List<RedisFuture<?>> futures = new ArrayList<>(items.size());
        for (T item : items) {
            RedisFuture<?> future = command.apply(commands, item);
            if (future != null) {
                futures.add(future);
            }
        }
        return futures;
    }

    public abstract static class CommandItemWriterBuilder<T> {

        protected final BiFunction<BaseRedisAsyncCommands<String, String>, T, RedisFuture<?>> command;

        protected CommandItemWriterBuilder(BiFunction<BaseRedisAsyncCommands<String, String>, T, RedisFuture<?>> command) {
            this.command = command;
        }

        public abstract CommandItemWriter<String, String, T> build();

    }

    public static <T> CommandItemWriterBuilder<T> builder(GenericObjectPool<StatefulRedisConnection<String, String>> pool, BiFunction<BaseRedisAsyncCommands<String, String>, T, RedisFuture<?>> command) {
        return new CommandItemWriterBuilder<T>(command) {
            @Override
            public CommandItemWriter<String, String, T> build() {
                return new CommandItemWriter<>(pool, c -> ((StatefulRedisConnection<String, String>) c).async(), command);
            }
        };
    }

    public static <T> CommandItemWriterBuilder<T> clusterBuilder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, BiFunction<BaseRedisAsyncCommands<String, String>, T, RedisFuture<?>> command) {
        return new CommandItemWriterBuilder<T>(command) {
            @Override
            public CommandItemWriter<String, String, T> build() {
                return new CommandItemWriter<>(pool, c -> ((StatefulRedisClusterConnection<String, String>) c).async(), command);
            }
        };
    }

}
