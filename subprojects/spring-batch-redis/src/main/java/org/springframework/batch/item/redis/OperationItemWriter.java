package org.springframework.batch.item.redis;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.support.AbstractPipelineItemWriter;
import org.springframework.batch.item.redis.support.CommandBuilder;
import org.springframework.batch.item.redis.support.RedisOperation;
import org.springframework.batch.item.redis.support.TransactionalOperationItemWriter;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

public class OperationItemWriter<K, V, T> extends AbstractPipelineItemWriter<K, V, T> {

    private final RedisOperation<K, V, T> operation;

    public OperationItemWriter(Supplier<StatefulConnection<K, V>> connectionSupplier, GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig, Function<StatefulConnection<K, V>, RedisModulesAsyncCommands<K, V>> async, RedisOperation<K, V, T> operation) {
        super(connectionSupplier, poolConfig, async);
        Assert.notNull(operation, "A Redis operation is required");
        this.operation = operation;
    }

    @Override
    protected void write(RedisModulesAsyncCommands<K, V> commands, long timeout, List<? extends T> items) {
        List<RedisFuture<?>> futures = write(commands, items);
        commands.flushCommands();
        LettuceFutures.awaitAll(timeout, TimeUnit.MILLISECONDS, futures.toArray(new RedisFuture[0]));
    }

    protected List<RedisFuture<?>> write(RedisModulesAsyncCommands<K, V> commands, List<? extends T> items) {
        List<RedisFuture<?>> futures = new ArrayList<>(items.size());
        for (T item : items) {
            futures.add(operation.execute(commands, item));
        }
        return futures;
    }

    public static ClientOperationItemWriterBuilder<String, String> client(AbstractRedisClient client) {
        return new ClientOperationItemWriterBuilder<>(client, StringCodec.UTF8);
    }

    public static <K, V> ClientOperationItemWriterBuilder<K, V> client(AbstractRedisClient client, RedisCodec<K, V> codec) {
        return new ClientOperationItemWriterBuilder<>(client, codec);
    }

    public static class ClientOperationItemWriterBuilder<K, V> {

        private final AbstractRedisClient client;
        private final RedisCodec<K, V> codec;

        public ClientOperationItemWriterBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
            this.client = client;
            this.codec = codec;
        }

        public <T> OperationItemWriterBuilder<K, V, T> operation(RedisOperation<K, V, T> operation) {
            return new OperationItemWriterBuilder<>(client, codec, operation);
        }

    }

    public static class OperationItemWriterBuilder<K, V, T> extends CommandBuilder<K, V, OperationItemWriterBuilder<K, V, T>> {

        protected final RedisOperation<K, V, T> operation;

        public OperationItemWriterBuilder(AbstractRedisClient client, RedisCodec<K, V> codec, RedisOperation<K, V, T> operation) {
            super(client, codec);
            this.operation = operation;
        }

        public TransactionalOperationItemWriter.TransactionalOperationItemWriterBuilder<K, V, T> transactional() {
            return new TransactionalOperationItemWriter.TransactionalOperationItemWriterBuilder<>(client, codec, operation);
        }

        public OperationItemWriter<K, V, T> build() {
            return new OperationItemWriter<>(connectionSupplier(), poolConfig, async(), operation);
        }

    }

    public static class TransactionalOperationItemWriterBuilder<K, V, T> extends OperationItemWriterBuilder<K, V, T> {

        public TransactionalOperationItemWriterBuilder(AbstractRedisClient client, RedisCodec<K, V> codec, RedisOperation<K, V, T> operation) {
            super(client, codec, operation);
        }

        @Override
        public OperationItemWriter<K, V, T> build() {
            return new TransactionalOperationItemWriter<>(connectionSupplier(), poolConfig, async(), operation);
        }
    }

}
