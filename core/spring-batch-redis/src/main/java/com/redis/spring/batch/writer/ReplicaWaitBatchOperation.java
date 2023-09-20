package com.redis.spring.batch.writer;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import com.redis.spring.batch.common.BatchOperation;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.cluster.PipelinedRedisFuture;

public class ReplicaWaitBatchOperation<K, V, I> implements BatchOperation<K, V, I, Object> {

    private final BatchOperation<K, V, I, Object> delegate;

    private int waitReplicas;

    private Duration waitTimeout;

    public ReplicaWaitBatchOperation(BatchOperation<K, V, I, Object> delegate) {
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
