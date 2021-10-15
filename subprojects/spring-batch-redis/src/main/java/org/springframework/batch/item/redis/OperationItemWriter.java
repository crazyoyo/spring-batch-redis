package org.springframework.batch.item.redis;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.support.CommandBuilder;
import org.springframework.batch.item.redis.support.ConnectionPoolItemStream;
import org.springframework.batch.item.redis.support.RedisOperation;
import org.springframework.batch.item.redis.support.operation.executor.MultiExecOperationExecutor;
import org.springframework.batch.item.redis.support.operation.executor.OperationExecutor;
import org.springframework.batch.item.redis.support.operation.executor.SimpleOperationExecutor;
import org.springframework.batch.item.redis.support.operation.executor.WaitForReplicationOperationExecutor;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class OperationItemWriter<K, V, T> extends ConnectionPoolItemStream<K, V> implements ItemWriter<T> {

	private final Function<StatefulConnection<K, V>, RedisModulesAsyncCommands<K, V>> async;
	private final OperationExecutor<K, V, T> executor;

	public OperationItemWriter(Supplier<StatefulConnection<K, V>> connectionSupplier,
			GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig,
			Function<StatefulConnection<K, V>, RedisModulesAsyncCommands<K, V>> async,
			OperationExecutor<K, V, T> executor) {
		super(connectionSupplier, poolConfig);
		this.async = async;
		this.executor = executor;
	}

	@Override
	public void write(List<? extends T> items) throws Exception {
		try (StatefulConnection<K, V> connection = pool.borrowObject()) {
			RedisModulesAsyncCommands<K, V> commands = async.apply(connection);
			commands.setAutoFlushCommands(false);
			try {
				List<RedisFuture<?>> futures = executor.execute(commands, items);
				commands.flushCommands();
				LettuceFutures.awaitAll(connection.getTimeout().toMillis(), TimeUnit.MILLISECONDS,
						futures.toArray(new RedisFuture[0]));
			} finally {
				commands.setAutoFlushCommands(true);
			}
		}
	}

	public static ClientOperationItemWriterBuilder<String, String> client(AbstractRedisClient client) {
		return new ClientOperationItemWriterBuilder<>(client, StringCodec.UTF8);
	}

	public static <K, V> ClientOperationItemWriterBuilder<K, V> client(AbstractRedisClient client,
			RedisCodec<K, V> codec) {
		return new ClientOperationItemWriterBuilder<>(client, codec);
	}

	public static class ClientOperationItemWriterBuilder<K, V> {

		private final AbstractRedisClient client;
		private final RedisCodec<K, V> codec;

		public ClientOperationItemWriterBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			this.client = client;
			this.codec = codec;
		}

		public <T> OperationItemWriterBuilder<K, V, T> operation(RedisOperation<K, V, T> operation) {
			return new OperationItemWriterBuilder<>(client, codec, new SimpleOperationExecutor<>(operation));
		}

	}

	public static class OperationItemWriterBuilder<K, V, T>
			extends CommandBuilder<K, V, OperationItemWriterBuilder<K, V, T>> {

		private OperationExecutor<K, V, T> executor;

		public OperationItemWriterBuilder(AbstractRedisClient client, RedisCodec<K, V> codec,
				SimpleOperationExecutor<K, V, T> executor) {
			super(client, codec);
			this.executor = executor;
		}

		public OperationItemWriterBuilder<K, V, T> multiExec() {
			this.executor = new MultiExecOperationExecutor<>(executor);
			return this;
		}

		public OperationItemWriterBuilder<K, V, T> waitForReplication(int replicas, long timeout) {
			this.executor = new WaitForReplicationOperationExecutor<>(executor, replicas, timeout);
			return this;
		}

		public OperationItemWriter<K, V, T> build() {
			return new OperationItemWriter<>(connectionSupplier(), poolConfig, async(), executor);
		}

	}

}
