package com.redis.spring.batch;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.convert.converter.Converter;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.spring.batch.support.CommandBuilder;
import com.redis.spring.batch.support.ConnectionPoolItemStream;
import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.KeyValue;
import com.redis.spring.batch.support.RedisOperation;
import com.redis.spring.batch.support.operation.RestoreReplace;
import com.redis.spring.batch.support.operation.executor.DataStructureOperationExecutor;
import com.redis.spring.batch.support.operation.executor.MultiExecOperationExecutor;
import com.redis.spring.batch.support.operation.executor.OperationExecutor;
import com.redis.spring.batch.support.operation.executor.SimpleOperationExecutor;
import com.redis.spring.batch.support.operation.executor.WaitForReplicationOperationExecutor;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisItemWriter<K, V, T> extends ConnectionPoolItemStream<K, V> implements ItemWriter<T> {

	private final Function<StatefulConnection<K, V>, RedisModulesAsyncCommands<K, V>> async;
	private final OperationExecutor<K, V, T> executor;

	public RedisItemWriter(Supplier<StatefulConnection<K, V>> connectionSupplier,
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
				List<Future<?>> futures = executor.execute(commands, items);
				commands.flushCommands();
				LettuceFutures.awaitAll(connection.getTimeout().toMillis(), TimeUnit.MILLISECONDS,
						futures.toArray(new Future[0]));
			} finally {
				commands.setAutoFlushCommands(true);
			}
		}
	}

	public static RedisItemWriterBuilder<String, String, KeyValue<String, byte[]>> keyDump(AbstractRedisClient client) {
		RestoreReplace<String, String, KeyValue<String, byte[]>> operation = new RestoreReplace<>(KeyValue::getKey,
				KeyValue<String, byte[]>::getValue, KeyValue::getAbsoluteTTL);
		return new RedisItemWriterBuilder<>(client, StringCodec.UTF8, new SimpleOperationExecutor<>(operation));
	}

	public static RedisItemWriterBuilder<String, String, DataStructure<String>> dataStructure(
			AbstractRedisClient client) {
		return dataStructure(client, m -> new XAddArgs().id(m.getId()));
	}

	public static RedisItemWriterBuilder<String, String, DataStructure<String>> dataStructure(
			AbstractRedisClient client, Converter<StreamMessage<String, String>, XAddArgs> args) {
		return new RedisItemWriterBuilder<>(client, StringCodec.UTF8,
				new DataStructureOperationExecutor<>(client.getDefaultTimeout(), args));
	}

	public static <T> RedisOperationItemWriterBuilder<T> operation(RedisOperation<String, String, T> operation) {
		return new RedisOperationItemWriterBuilder<>(operation);
	}

	public static class RedisOperationItemWriterBuilder<T> {

		private RedisOperation<String, String, T> operation;

		public RedisOperationItemWriterBuilder(RedisOperation<String, String, T> operation) {
			this.operation = operation;
		}

		public RedisItemWriterBuilder<String, String, T> client(AbstractRedisClient client) {
			return new RedisItemWriterBuilder<>(client, StringCodec.UTF8, new SimpleOperationExecutor<>(operation));
		}

	}

	public static class RedisItemWriterBuilder<K, V, T> extends CommandBuilder<K, V, RedisItemWriterBuilder<K, V, T>> {

		private OperationExecutor<K, V, T> executor;

		public RedisItemWriterBuilder(AbstractRedisClient client, RedisCodec<K, V> codec,
				OperationExecutor<K, V, T> executor) {
			super(client, codec);
			this.executor = executor;
		}

		public RedisItemWriterBuilder<K, V, T> multiExec() {
			this.executor = new MultiExecOperationExecutor<>(executor);
			return this;
		}

		public RedisItemWriterBuilder<K, V, T> waitForReplication(int replicas, long timeout) {
			this.executor = new WaitForReplicationOperationExecutor<>(executor, replicas, timeout);
			return this;
		}

		public RedisItemWriter<K, V, T> build() {
			return new RedisItemWriter<>(connectionSupplier(), poolConfig, async(), executor);
		}

	}

}
