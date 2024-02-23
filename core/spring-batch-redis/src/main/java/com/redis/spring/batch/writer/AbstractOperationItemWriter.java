package com.redis.spring.batch.writer;

import java.time.Duration;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemStreamWriter;

import com.redis.spring.batch.common.AbstractOperationExecutor;
import com.redis.spring.batch.writer.operation.MultiExec;
import com.redis.spring.batch.writer.operation.ReplicaWait;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.RedisCodec;

public abstract class AbstractOperationItemWriter<K, V, T> extends AbstractOperationExecutor<K, V, T, Object>
        implements ItemStreamWriter<T> {

    public static final Duration DEFAULT_WAIT_TIMEOUT = Duration.ofSeconds(1);

    private int waitReplicas;

    private Duration waitTimeout = DEFAULT_WAIT_TIMEOUT;

    private boolean multiExec;

    protected AbstractOperationItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec) {
        super(client, codec);
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

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void write(Chunk<? extends T> items) throws RedisException {
        process((Iterable) items);
    }

    @Override
    protected BatchWriteOperation<K, V, T> batchOperation() {
        BatchWriteOperation<K, V, T> batchOperation = batchWriteOperation();
        batchOperation = replicaWaitOperation(batchOperation);
        return multiExec(batchOperation);
    }

    protected abstract BatchWriteOperation<K, V, T> batchWriteOperation();

    private BatchWriteOperation<K, V, T> multiExec(BatchWriteOperation<K, V, T> operation) {
        if (multiExec) {
            if (client instanceof RedisClusterClient) {
                throw new UnsupportedOperationException("Multi/exec is not supported on Redis Cluster");
            }
            return new MultiExec<>(operation);
        }
        return operation;
    }

    private BatchWriteOperation<K, V, T> replicaWaitOperation(BatchWriteOperation<K, V, T> operation) {
        if (waitReplicas > 0) {
            return new ReplicaWait<>(operation, waitReplicas, waitTimeout);
        }
        return operation;
    }

}
