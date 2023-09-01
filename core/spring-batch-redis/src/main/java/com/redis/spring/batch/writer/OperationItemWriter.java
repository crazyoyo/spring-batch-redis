package com.redis.spring.batch.writer;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.Collection;
import java.util.List;

import org.springframework.batch.item.ItemStreamWriter;

import com.redis.spring.batch.AbstractRedisItemStreamSupport;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisTransactionalAsyncCommands;
import io.lettuce.core.cluster.PipelinedRedisFuture;
import io.lettuce.core.codec.RedisCodec;

public class OperationItemWriter<K, V, T> extends AbstractRedisItemStreamSupport<K, V, T> implements ItemStreamWriter<T> {

    public static final Duration DEFAULT_WAIT_TIMEOUT = Duration.ofSeconds(1);

    private int waitReplicas;

    private Duration waitTimeout = DEFAULT_WAIT_TIMEOUT;

    private boolean multiExec;

    private Operation<K, V, T> operation;

    public OperationItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec) {
        super(client, codec);
    }

    public OperationItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec, Operation<K, V, T> operation) {
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

    public void setOperation(Operation<K, V, T> operation) {
        this.operation = operation;
    }

    @Override
    public void write(List<? extends T> items) throws Exception {
        execute(items);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void execute(BaseRedisAsyncCommands<K, V> commands, Collection<? extends T> items, List<RedisFuture<?>> futures) {
        if (multiExec) {
            futures.add(((RedisTransactionalAsyncCommands<K, V>) commands).multi());
        }
        super.execute(commands, items, futures);
        if (waitReplicas > 0) {
            RedisFuture<Long> waitFuture = commands.waitForReplication(waitReplicas, waitTimeout.toMillis());
            futures.add(new PipelinedRedisFuture<>(waitFuture.thenAccept(this::checkReplicas)));
        }
        if (multiExec) {
            futures.add(((RedisTransactionalAsyncCommands<K, V>) commands).exec());
        }
    }

    @Override
    protected void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<?>> futures) {
        operation.execute(commands, item, futures);
    }

    private void checkReplicas(Long actual) {
        if (actual == null || actual < waitReplicas) {
            throw new RedisCommandExecutionException(
                    MessageFormat.format("Insufficient replication level ({0}/{1})", actual, waitReplicas));
        }
    }

}
