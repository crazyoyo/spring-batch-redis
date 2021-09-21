package org.springframework.batch.item.redis.support;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.OperationItemWriter;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class TransactionalOperationItemWriter<K, V, T> extends OperationItemWriter<K, V, T> {

    public TransactionalOperationItemWriter(Supplier<StatefulConnection<K, V>> connectionSupplier, GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig, Function<StatefulConnection<K, V>, RedisModulesAsyncCommands<K, V>> async, RedisOperation<K, V, T> operation) {
        super(connectionSupplier, poolConfig, async, operation);
    }

    @Override
    protected void write(RedisModulesAsyncCommands<K, V> commands, List<? extends T> items, List<RedisFuture<?>> futures) {
        futures.add(commands.multi());
        super.write(commands, items, futures);
        futures.add(commands.exec());
    }

    @Override
    protected int futureCount(List<? extends T> items) {
        return super.futureCount(items) + 2; // Add 2 for MULTI and EXEC commands
    }
}
