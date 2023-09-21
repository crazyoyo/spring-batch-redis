package com.redis.spring.batch.writer;

import java.time.Duration;
import java.util.List;

import org.springframework.batch.item.ItemStreamWriter;

import com.redis.spring.batch.common.AbstractBatchOperationExecutor;
import com.redis.spring.batch.common.BatchOperation;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.common.SimpleBatchOperation;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class OperationItemWriter<K, V, T> extends AbstractBatchOperationExecutor<K, V, T, Object>
        implements ItemStreamWriter<T> {

    public static final Duration DEFAULT_WAIT_TIMEOUT = Duration.ofSeconds(1);

    private int waitReplicas;

    private Duration waitTimeout = DEFAULT_WAIT_TIMEOUT;

    private boolean multiExec;

    private final Operation<K, V, T, Object> operation;

    public OperationItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec, Operation<K, V, T, Object> operation) {
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
        execute(items);
    }

    @Override
    protected BatchOperation<K, V, T, Object> batchOperation() {
        BatchOperation<K, V, T, Object> batchOperation = new SimpleBatchOperation<>(operation);
        if (waitReplicas > 0) {
            batchOperation = replicaWaitOperation(batchOperation);
        }
        if (multiExec) {
            batchOperation = new MultiExecBatchOperation<>(batchOperation);
        }
        return batchOperation;
    }

    private ReplicaWaitBatchOperation<K, V, T> replicaWaitOperation(BatchOperation<K, V, T, Object> batchOperation) {
        ReplicaWaitBatchOperation<K, V, T> waitOperation = new ReplicaWaitBatchOperation<>(batchOperation);
        waitOperation.setWaitReplicas(waitReplicas);
        waitOperation.setWaitTimeout(waitTimeout);
        return waitOperation;
    }

}
