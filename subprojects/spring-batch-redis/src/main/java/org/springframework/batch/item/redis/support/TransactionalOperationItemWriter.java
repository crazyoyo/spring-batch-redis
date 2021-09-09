package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisTransactionalAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.OperationItemWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class TransactionalOperationItemWriter<K, V, T> extends OperationItemWriter<K, V, T> {

    public TransactionalOperationItemWriter(Supplier<StatefulConnection<K, V>> connectionSupplier, GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> async, RedisOperation<K, V, T> operation) {
        super(connectionSupplier, poolConfig, async, operation);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected List<RedisFuture<?>> write(BaseRedisAsyncCommands<K, V> commands, List<? extends T> items) {
        RedisFuture<String> multiFuture = ((RedisTransactionalAsyncCommands<K, V>) commands).multi();
        List<RedisFuture<?>> futures = super.write(commands, items);
        RedisFuture<TransactionResult> execFuture = ((RedisTransactionalAsyncCommands<K, V>) commands).exec();
        List<RedisFuture<?>> allFutures = new ArrayList<>(futures.size() + 2);
        allFutures.add(multiFuture);
        allFutures.addAll(futures);
        allFutures.add(execFuture);
        return allFutures;
    }

}
