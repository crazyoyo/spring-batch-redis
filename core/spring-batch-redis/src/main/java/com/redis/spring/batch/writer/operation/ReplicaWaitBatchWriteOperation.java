package com.redis.spring.batch.writer.operation;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import com.redis.spring.batch.writer.BatchWriteOperation;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.cluster.PipelinedRedisFuture;

public class ReplicaWaitBatchWriteOperation<K, V, I> implements BatchWriteOperation<K, V, I> {

    private final BatchWriteOperation<K, V, I> delegate;

    private int waitReplicas;

    private Duration waitTimeout;

    public ReplicaWaitBatchWriteOperation(BatchWriteOperation<K, V, I> delegate) {
        this.delegate = delegate;
    }

    public void setWaitReplicas(int count) {
        this.waitReplicas = count;
    }

    public void setWaitTimeout(Duration timeout) {
        this.waitTimeout = timeout;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public List<RedisFuture<Object>> execute(BaseRedisAsyncCommands<K, V> commands, List<? extends I> items) {
        List<RedisFuture<Object>> futures = new ArrayList<>();
        futures.addAll(delegate.execute(commands, items));
        RedisFuture<Long> waitFuture = commands.waitForReplication(waitReplicas, waitTimeout.toMillis());
        futures.add((RedisFuture) new PipelinedRedisFuture<>(waitFuture.thenAccept(this::checkReplicas)));
        return futures;
    }

    private void checkReplicas(Long actual) {
        if (actual == null || actual < waitReplicas) {
            throw new RedisCommandExecutionException(errorMessage(actual));
        }
    }

    private String errorMessage(Long actual) {
        return MessageFormat.format("Insufficient replication level ({0}/{1})", actual, waitReplicas);
    }

}
