package com.redis.spring.batch;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

import com.redis.spring.batch.common.BatchOperationFunction;
import com.redis.spring.batch.common.BatchOperationFunction.BatchOperation;
import com.redis.spring.batch.common.Dump;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.common.Struct;
import com.redis.spring.batch.util.ConnectionUtils;
import com.redis.spring.batch.writer.StructOverwriteOperation;
import com.redis.spring.batch.writer.StructWriteOperation;
import com.redis.spring.batch.writer.operation.Restore;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisTransactionalAsyncCommands;
import io.lettuce.core.cluster.PipelinedRedisFuture;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.support.ConnectionPoolSupport;

public class RedisItemWriter<K, V, T> extends AbstractItemStreamItemWriter<T> {

    public static final int DEFAULT_POOL_SIZE = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;

    public static final Duration DEFAULT_WAIT_TIMEOUT = Duration.ofSeconds(1);

    private final AbstractRedisClient client;

    private final RedisCodec<K, V> codec;

    private final Operation<K, V, T, Object> operation;

    private int poolSize = DEFAULT_POOL_SIZE;

    private int waitReplicas;

    private Duration waitTimeout = DEFAULT_WAIT_TIMEOUT;

    private boolean multiExec;

    private GenericObjectPool<StatefulConnection<K, V>> pool;

    private BatchOperationFunction<K, V, T, Object> operationFunction;

    public RedisItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec, Operation<K, V, T, Object> operation) {
        this.client = client;
        this.codec = codec;
        this.operation = operation;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    public void setWaitReplicas(int replicas) {
        this.waitReplicas = replicas;
    }

    public void setWaitTimeout(Duration timeout) {
        this.waitTimeout = timeout;
    }

    public void setMultiExec(boolean multiExec) {
        this.multiExec = multiExec;
    }

    @Override
    public synchronized void open(ExecutionContext executionContext) {
        super.open(executionContext);
        if (isOpen()) {
            return;
        }
        pool = pool();
        operationFunction = new BatchOperationFunction<>(pool, new WriteOperation(operation));
    }

    @Override
    public synchronized void close() {
        super.close();
        if (!isOpen()) {
            return;
        }
        pool.close();
        operationFunction = null;
    }

    public boolean isOpen() {
        return operationFunction != null;
    }

    public GenericObjectPool<StatefulConnection<K, V>> pool() {
        Supplier<StatefulConnection<K, V>> connectionSupplier = ConnectionUtils.supplier(client, codec);
        GenericObjectPoolConfig<StatefulConnection<K, V>> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(poolSize);
        return ConnectionPoolSupport.createGenericObjectPool(connectionSupplier, config);
    }

    @Override
    public void write(List<? extends T> items) throws Exception {
        operationFunction.apply(items);
    }

    private class WriteOperation implements BatchOperation<K, V, T, Object> {

        private final Operation<K, V, T, Object> operation;

        public WriteOperation(Operation<K, V, T, Object> operation) {
            this.operation = operation;
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Override
        public List<RedisFuture<Object>> execute(BaseRedisAsyncCommands<K, V> commands, List<? extends T> items) {
            List<RedisFuture<Object>> futures = new ArrayList<>(items.size());
            if (multiExec) {
                futures.add((RedisFuture) ((RedisTransactionalAsyncCommands<K, V>) commands).multi());
            }
            for (T item : items) {
                operation.execute(commands, item, futures);
            }
            if (waitReplicas > 0) {
                RedisFuture<Long> waitFuture = commands.waitForReplication(waitReplicas, waitTimeout.toMillis());
                futures.add((RedisFuture) new PipelinedRedisFuture<>(waitFuture.thenAccept(this::checkReplicas)));
            }
            if (multiExec) {
                futures.add((RedisFuture) ((RedisTransactionalAsyncCommands<K, V>) commands).exec());
            }
            return futures;
        }

        private void checkReplicas(Long actual) {
            if (actual == null || actual < waitReplicas) {
                throw new RedisCommandExecutionException(
                        MessageFormat.format("Insufficient replication level ({0}/{1})", actual, waitReplicas));
            }
        }

    }

    public static <K, V> RedisItemWriter<K, V, Dump<K>> dump(AbstractRedisClient client, RedisCodec<K, V> codec) {
        Restore<K, V, Dump<K>> operation = new Restore<>();
        operation.setKeyFunction(Dump::getKey);
        operation.setBytes(KeyValue::getValue);
        operation.setTtl(KeyValue::getTtl);
        operation.setReplace(true);
        return new RedisItemWriter<>(client, codec, operation);
    }

    public static <K, V> RedisItemWriter<K, V, Struct<K>> struct(AbstractRedisClient client, RedisCodec<K, V> codec) {
        return new RedisItemWriter<>(client, codec, new StructOverwriteOperation<>());
    }

    public static <K, V> RedisItemWriter<K, V, Struct<K>> structMerge(AbstractRedisClient client, RedisCodec<K, V> codec) {
        return new RedisItemWriter<>(client, codec, new StructWriteOperation<>());
    }

}
