package com.redis.spring.batch;

import java.time.Duration;
import java.util.List;

import org.springframework.batch.item.ItemStreamWriter;

import com.redis.spring.batch.common.AbstractOperationExecutor;
import com.redis.spring.batch.common.BatchOperation;
import com.redis.spring.batch.common.Dump;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.common.SimpleBatchOperation;
import com.redis.spring.batch.common.Struct;
import com.redis.spring.batch.writer.MultiExecBatchOperation;
import com.redis.spring.batch.writer.ReplicaWaitBatchOperation;
import com.redis.spring.batch.writer.StructOverwriteOperation;
import com.redis.spring.batch.writer.StructWriteOperation;
import com.redis.spring.batch.writer.operation.Restore;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class RedisItemWriter<K, V, T> extends AbstractOperationExecutor<K, V, T, Object> implements ItemStreamWriter<T> {

    public static final Duration DEFAULT_WAIT_TIMEOUT = Duration.ofSeconds(1);

    private int waitReplicas;

    private Duration waitTimeout = DEFAULT_WAIT_TIMEOUT;

    private boolean multiExec;

    private Operation<K, V, T, Object> operation;

    public RedisItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec, Operation<K, V, T, Object> operation) {
        super(client, codec);
        this.operation = operation;
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
    protected BatchOperation<K, V, T, Object> operation() {
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
