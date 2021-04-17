package org.springframework.batch.item.redis;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisTransactionalAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.OperationItemWriter;
import org.springframework.batch.item.redis.support.RedisOperation;

import java.util.ArrayList;
import java.util.List;

public class RedisTransactionItemWriter<K, V, T> extends OperationItemWriter<K, V, StatefulRedisConnection<K, V>, T> {

    public RedisTransactionItemWriter(GenericObjectPool<StatefulRedisConnection<K, V>> pool, RedisOperation<K, V, T> operation) {
        super(pool, StatefulRedisConnection::async, operation);
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
