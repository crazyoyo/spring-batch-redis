package com.redis.spring.batch.writer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.springframework.util.Assert;

import com.redis.spring.batch.common.BatchOperation;
import com.redis.spring.batch.common.DelegatingItemStreamSupport;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.cluster.PipelinedRedisFuture;

public class ReplicaWaitWriteOperation<K, V, T, U> extends DelegatingItemStreamSupport
		implements BatchOperation<K, V, T, U> {

	public static final int DEFAULT_REPLICAS = 0;
	public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(1);

	private final BatchOperation<K, V, T, ?> delegate;
	private int replicas = DEFAULT_REPLICAS;
	private Duration timeout = DEFAULT_TIMEOUT;

	public ReplicaWaitWriteOperation(BatchOperation<K, V, T, ?> delegate) {
		super(delegate);
		this.delegate = delegate;
	}

	public void setReplicas(int replicas) {
		this.replicas = replicas;
	}

	public void setTimeout(Duration timeout) {
		Assert.notNull(timeout, "Replication timeout should not be null");
		this.timeout = timeout;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public List<RedisFuture<U>> execute(BaseRedisAsyncCommands<K, V> commands, List<? extends T> items) {
		List<RedisFuture<?>> futures = new ArrayList<>();
		futures.addAll(delegate.execute(commands, items));
		futures.add(replicationFuture(commands));
		return (List) futures;
	}

	private RedisFuture<?> replicationFuture(BaseRedisAsyncCommands<K, V> commands) {
		return new PipelinedRedisFuture<>(commands.waitForReplication(replicas, timeout.toMillis()).thenAccept(r -> {
			if (r < replicas) {
				throw new RedisCommandExecutionException(
						String.format("Insufficient replication level - expected: %s, actual: %s", replicas, r));
			}
		}));
	}

}
