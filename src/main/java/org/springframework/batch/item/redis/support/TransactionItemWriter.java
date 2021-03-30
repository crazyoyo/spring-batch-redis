package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisTransactionalAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class TransactionItemWriter<K, V, C extends StatefulConnection<K, V>, T> extends OperationItemWriter<K, V, C, T> {

    public TransactionItemWriter(GenericObjectPool<C> pool, Function<C, BaseRedisAsyncCommands<K, V>> async, RedisOperation<K, V, T> operation) {
        super(pool, async, operation);
    }

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
