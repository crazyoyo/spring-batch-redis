package com.redis.spring.batch;

import java.time.Duration;
import java.util.List;

import org.springframework.batch.item.ItemStreamWriter;

import com.redis.spring.batch.common.AbstractBatchOperationExecutor;
import com.redis.spring.batch.common.BatchOperation;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.common.SimpleBatchOperation;
import com.redis.spring.batch.writer.DumpItemWriter;
import com.redis.spring.batch.writer.MultiExecBatchOperation;
import com.redis.spring.batch.writer.ReplicaWaitBatchOperation;
import com.redis.spring.batch.writer.StructItemWriter;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisItemWriter<K, V, T> extends AbstractBatchOperationExecutor<K, V, T, Object> implements ItemStreamWriter<T> {

    public static final Duration DEFAULT_WAIT_TIMEOUT = Duration.ofSeconds(1);

    private int waitReplicas;

    private Duration waitTimeout = DEFAULT_WAIT_TIMEOUT;

    private boolean multiExec;

    protected final Operation<K, V, T, Object> operation;

    public RedisItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec, Operation<K, V, T, Object> operation) {
        super(client, codec);
        this.operation = operation;
    }

    public Operation<K, V, T, Object> getOperation() {
        return operation;
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
    public void write(List<? extends T> items) throws Exception {
        process(items);
    }

    @Override
    protected BatchOperation<K, V, T, Object> batchOperation() {
        BatchOperation<K, V, T, Object> batchOperation = new SimpleBatchOperation<>(operation);
        batchOperation = replicaWaitOperation(batchOperation);
        return multiExec(batchOperation);
    }

    private BatchOperation<K, V, T, Object> multiExec(BatchOperation<K, V, T, Object> batchOperation) {
        if (multiExec) {
            return new MultiExecBatchOperation<>(batchOperation);
        }
        return batchOperation;
    }

    private BatchOperation<K, V, T, Object> replicaWaitOperation(BatchOperation<K, V, T, Object> operation) {
        if (waitReplicas > 0) {
            ReplicaWaitBatchOperation<K, V, T> waitOperation = new ReplicaWaitBatchOperation<>(operation);
            waitOperation.setWaitReplicas(waitReplicas);
            waitOperation.setWaitTimeout(waitTimeout);
            return waitOperation;
        }
        return operation;
    }

    public static StructItemWriter<String, String> struct(AbstractRedisClient client) {
        return struct(client, StringCodec.UTF8);
    }

    public static <K, V> StructItemWriter<K, V> struct(AbstractRedisClient client, RedisCodec<K, V> codec) {
        return new StructItemWriter<>(client, codec);
    }

    public static DumpItemWriter<String, String> dump(AbstractRedisClient client) {
        return dump(client, StringCodec.UTF8);
    }

    public static <K, V> DumpItemWriter<K, V> dump(AbstractRedisClient client, RedisCodec<K, V> codec) {
        return new DumpItemWriter<>(client, codec);
    }

}
