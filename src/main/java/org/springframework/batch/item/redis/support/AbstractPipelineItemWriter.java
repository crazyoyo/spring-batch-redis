package org.springframework.batch.item.redis.support;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ItemWriter;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class AbstractPipelineItemWriter<K, V, T> extends ConnectionPoolItemStream<K, V> implements ItemWriter<T> {

    private final Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> async;

    protected AbstractPipelineItemWriter(Supplier<StatefulConnection<K, V>> connectionSupplier, GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> async) {
        super(connectionSupplier, poolConfig);
        this.async = async;
    }

    @Override
    public void write(List<? extends T> items) throws Exception {
        try (StatefulConnection<K, V> connection = pool.borrowObject()) {
            BaseRedisAsyncCommands<K, V> commands = async.apply(connection);
            commands.setAutoFlushCommands(false);
            try {
                write(commands, connection.getTimeout().toMillis(), items);
            } finally {
                commands.setAutoFlushCommands(true);
            }
        }
    }

    protected abstract void write(BaseRedisAsyncCommands<K, V> commands, long timeout, List<? extends T> items);


}
