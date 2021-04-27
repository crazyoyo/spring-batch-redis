package org.springframework.batch.item.redis;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisTransactionalAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.support.AbstractPipelineItemWriter;
import org.springframework.batch.item.redis.support.CommandBuilder;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

public class OperationItemWriter<K, V, T> extends AbstractPipelineItemWriter<K, V, T> {

    private final RedisOperation<K, V, T> operation;

    public OperationItemWriter(Supplier<StatefulConnection<K, V>> connectionSupplier, GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> async, RedisOperation<K, V, T> operation) {
        super(connectionSupplier, poolConfig, async);
        Assert.notNull(operation, "A Redis operation is required");
        this.operation = operation;
    }

    @Override
    protected void write(BaseRedisAsyncCommands<K, V> commands, long timeout, List<? extends T> items) {
        List<RedisFuture<?>> futures = write(commands, items);
        commands.flushCommands();
        LettuceFutures.awaitAll(timeout, TimeUnit.MILLISECONDS, futures.toArray(new RedisFuture[0]));
    }

    protected List<RedisFuture<?>> write(BaseRedisAsyncCommands<K, V> commands, List<? extends T> items) {
        List<RedisFuture<?>> futures = new ArrayList<>(items.size());
        for (T item : items) {
            futures.add(operation.execute(commands, item));
        }
        return futures;
    }

    public static class TransactionItemWriter<K, V, T> extends OperationItemWriter<K, V, T> {

        public TransactionItemWriter(Supplier<StatefulConnection<K, V>> connectionSupplier, GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> async, RedisOperation<K, V, T> operation) {
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

    public static <T> OperationItemWriterBuilder<T> operation(RedisOperation<String, String, T> operation) {
        return new OperationItemWriterBuilder<>(operation);
    }

    @Setter
    @Accessors(fluent = true)
    public static class OperationItemWriterBuilder<T> {

        private final RedisOperation<String, String, T> operation;

        public OperationItemWriterBuilder(RedisOperation<String, String, T> operation) {
            this.operation = operation;
        }

        public FinalOperationItemWriterBuilder<T> client(RedisClusterClient client) {
            return new FinalOperationItemWriterBuilder<>(client, operation);
        }

        public FinalOperationItemWriterBuilder<T> client(RedisClient client) {
            return new FinalOperationItemWriterBuilder<>(client, operation);
        }

        public TransactionItemWriterBuilder<T> transactional() {
            return new TransactionItemWriterBuilder<>(operation);
        }

    }

    public static class FinalOperationItemWriterBuilder<T> extends CommandBuilder<FinalOperationItemWriterBuilder<T>> {

        protected final RedisOperation<String, String, T> operation;

        public FinalOperationItemWriterBuilder(RedisClusterClient client, RedisOperation<String, String, T> operation) {
            super(client);
            this.operation = operation;
        }

        public FinalOperationItemWriterBuilder(RedisClient client, RedisOperation<String, String, T> operation) {
            super(client);
            this.operation = operation;
        }

        public OperationItemWriter<String, String, T> build() {
            return new OperationItemWriter<>(connectionSupplier, poolConfig, async, operation);
        }

    }

    @Setter
    @Accessors(fluent = true)
    public static class TransactionItemWriterBuilder<T> {

        private final RedisOperation<String, String, T> operation;

        public TransactionItemWriterBuilder(RedisOperation<String, String, T> operation) {
            this.operation = operation;
        }

        public FinalTransactionItemWriterBuilder<T> client(RedisClient client) {
            return new FinalTransactionItemWriterBuilder<>(client, operation);
        }

        public FinalTransactionItemWriterBuilder<T> client(RedisClusterClient client) {
            return new FinalTransactionItemWriterBuilder<>(client, operation);
        }

    }

    public static class FinalTransactionItemWriterBuilder<T> extends FinalOperationItemWriterBuilder<T> {

        public FinalTransactionItemWriterBuilder(RedisClient client, RedisOperation<String, String, T> operation) {
            super(client, operation);
        }

        public FinalTransactionItemWriterBuilder(RedisClusterClient client, RedisOperation<String, String, T> operation) {
            super(client, operation);
        }

        public TransactionItemWriter<String, String, T> build() {
            return new TransactionItemWriter<>(connectionSupplier, poolConfig, async, operation);
        }

    }

}
