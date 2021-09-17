package org.springframework.batch.item.redis.support;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulConnection;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.OperationItemWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class TransactionalOperationItemWriter<K, V, T> extends OperationItemWriter<K, V, T> {

    public TransactionalOperationItemWriter(Supplier<StatefulConnection<K, V>> connectionSupplier, GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig, Function<StatefulConnection<K, V>, RedisModulesAsyncCommands<K, V>> async, RedisOperation<K, V, T> operation) {
        super(connectionSupplier, poolConfig, async, operation);
    }

    @Override
    protected List<RedisFuture<?>> write(RedisModulesAsyncCommands<K, V> commands, List<? extends T> items) {
        RedisFuture<String> multiFuture = commands.multi();
        List<RedisFuture<?>> futures = super.write(commands, items);
        RedisFuture<TransactionResult> execFuture = commands.exec();
        List<RedisFuture<?>> allFutures = new ArrayList<>(futures.size() + 2);
        allFutures.add(multiFuture);
        allFutures.addAll(futures);
        allFutures.add(execFuture);
        return allFutures;
    }

}
