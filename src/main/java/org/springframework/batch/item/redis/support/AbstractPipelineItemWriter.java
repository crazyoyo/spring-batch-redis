package org.springframework.batch.item.redis.support;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ItemWriter;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class AbstractPipelineItemWriter<T> extends ConnectionPoolItemStream implements ItemWriter<T> {

    private final Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> async;

    protected AbstractPipelineItemWriter(Supplier<StatefulConnection<String, String>> connectionSupplier, GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig, Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> async) {
        super(connectionSupplier, poolConfig);
        this.async = async;
    }

    @Override
    public void write(List<? extends T> items) throws Exception {
        try (StatefulConnection<String, String> connection = pool.borrowObject()) {
            BaseRedisAsyncCommands<String, String> commands = async.apply(connection);
            commands.setAutoFlushCommands(false);
            try {
                write(commands, connection.getTimeout().toMillis(), items);
            } finally {
                commands.setAutoFlushCommands(true);
            }
        }
    }

    protected abstract void write(BaseRedisAsyncCommands<String, String> commands, long timeout, List<? extends T> items);


}
