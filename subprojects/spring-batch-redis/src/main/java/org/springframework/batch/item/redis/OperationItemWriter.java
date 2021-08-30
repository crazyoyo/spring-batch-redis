package org.springframework.batch.item.redis;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisTransactionalAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
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

    private final RedisOperation<T> operation;

    public OperationItemWriter(Supplier<StatefulConnection<K, V>> connectionSupplier, GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> async, RedisOperation<T> operation) {
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

        public TransactionItemWriter(Supplier<StatefulConnection<K, V>> connectionSupplier, GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> async, RedisOperation<T> operation) {
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

    public interface RedisOperation<T> {

        <K, V> RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item);

    }

    public static <K, V, T> OperationItemWriterBuilder<K, V, T> operation(RedisOperation<T> operation) {
        return new OperationItemWriterBuilder<>(operation);
    }

    public static class OperationItemWriterBuilder<K, V, T> {

        private final RedisOperation<T> operation;

        public OperationItemWriterBuilder(RedisOperation<T> operation) {
            this.operation = operation;
        }

        public CommandOperationItemWriterBuilder<String, String, T> client(RedisClient client) {
            return new CommandOperationItemWriterBuilder<>(client, StringCodec.UTF8, operation);
        }

        public CommandOperationItemWriterBuilder<String, String, T> client(RedisClusterClient client) {
            return new CommandOperationItemWriterBuilder<>(client, StringCodec.UTF8, operation);
        }

        public CommandOperationItemWriterBuilder<K, V, T> client(RedisClient client, RedisCodec<K, V> codec) {
            return new CommandOperationItemWriterBuilder<>(client, codec, operation);
        }

        public CommandOperationItemWriterBuilder<K, V, T> client(RedisClusterClient client, RedisCodec<K, V> codec) {
            return new CommandOperationItemWriterBuilder<>(client, codec, operation);
        }

    }

    @Setter
    @Accessors(fluent = true)
    public static class CommandOperationItemWriterBuilder<K, V, T> extends CommandBuilder<K, V, CommandOperationItemWriterBuilder<K, V, T>> {

        private final RedisOperation<T> operation;
        private boolean transactional;

        public CommandOperationItemWriterBuilder(RedisClient client, RedisCodec<K, V> codec, RedisOperation<T> operation) {
            super(client, codec);
            this.operation = operation;
        }

        public CommandOperationItemWriterBuilder(RedisClusterClient client, RedisCodec<K, V> codec, RedisOperation<T> operation) {
            super(client, codec);
            this.operation = operation;
        }

        public OperationItemWriter<K, V, T> build() {
            if (transactional) {
                return new TransactionItemWriter<>(connectionSupplier, poolConfig, async, operation);
            }
            return new OperationItemWriter<>(connectionSupplier, poolConfig, async, operation);
        }

    }


}
