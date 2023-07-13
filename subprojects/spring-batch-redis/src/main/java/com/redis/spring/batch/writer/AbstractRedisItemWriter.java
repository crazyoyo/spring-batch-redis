package com.redis.spring.batch.writer;

import java.util.List;

import org.springframework.batch.item.ItemStreamWriter;

import com.redis.spring.batch.common.AbstractOperationItemStreamSupport;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisTransactionalAsyncCommands;
import io.lettuce.core.cluster.PipelinedRedisFuture;
import io.lettuce.core.codec.RedisCodec;

public abstract class AbstractRedisItemWriter<K, V, T> extends AbstractOperationItemStreamSupport<K, V, T, Object>
		implements ItemStreamWriter<T> {

	private WriterOptions options = WriterOptions.builder().build();

	protected AbstractRedisItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec) {
		super(client, codec);
	}

	public WriterOptions getOptions() {
		return options;
	}

	public void setOptions(WriterOptions options) {
		this.options = options;
	}

	@Override
	public void write(List<? extends T> items) throws Exception {
		execute(items);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void execute(BaseRedisAsyncCommands<K, V> commands, List<? extends T> items,
			List<RedisFuture<Object>> futures) {
		if (options.isMultiExec()) {
			futures.add((RedisFuture) ((RedisTransactionalAsyncCommands<K, V>) commands).multi());
		}
		super.execute(commands, items, futures);
		if (options.getReplicaWaitOptions().getReplicas() > 0) {
			RedisFuture<Long> waitFuture = commands.waitForReplication(options.getReplicaWaitOptions().getReplicas(),
					waitTimeout());
			PipelinedRedisFuture replicaWaitFuture = new PipelinedRedisFuture(
					waitFuture.thenAccept(this::checkReplicas));
			futures.add(replicaWaitFuture);
		}
		if (options.isMultiExec()) {
			futures.add((RedisFuture) ((RedisTransactionalAsyncCommands<K, V>) commands).exec());
		}
	}

	private long waitTimeout() {
		return options.getReplicaWaitOptions().getTimeout().toMillis();
	}

	private void checkReplicas(Long actual) {
		if (actual == null || actual < options.getReplicaWaitOptions().getReplicas()) {
			throw new InsufficientReplicasException(options.getReplicaWaitOptions().getReplicas(), actual);
		}
	}

	private static class InsufficientReplicasException extends RedisCommandExecutionException {

		private static final long serialVersionUID = 1L;
		private static final String MESSAGE = "Insufficient replication level - expected: %s, actual: %s";

		public InsufficientReplicasException(long expected, long actual) {
			super(String.format(MESSAGE, expected, actual));
		}

	}

}
