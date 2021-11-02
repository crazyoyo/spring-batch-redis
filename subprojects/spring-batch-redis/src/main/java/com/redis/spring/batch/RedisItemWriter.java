package com.redis.spring.batch;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.spring.batch.support.CommandBuilder;
import com.redis.spring.batch.support.ConnectionPoolItemStream;
import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.KeyValue;
import com.redis.spring.batch.support.RedisOperation;
import com.redis.spring.batch.support.operation.JsonSet;
import com.redis.spring.batch.support.operation.RestoreReplace;
import com.redis.spring.batch.support.operation.Sugadd;
import com.redis.spring.batch.support.operation.TsAdd;
import com.redis.spring.batch.support.operation.executor.DataStructureOperationExecutor;
import com.redis.spring.batch.support.operation.executor.MultiExecOperationExecutor;
import com.redis.spring.batch.support.operation.executor.OperationExecutor;
import com.redis.spring.batch.support.operation.executor.SimpleOperationExecutor;
import com.redis.spring.batch.support.operation.executor.WaitForReplicationOperationExecutor;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisItemWriter<K, V, T> extends ConnectionPoolItemStream<K, V> implements ItemWriter<T> {

	private final Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> async;
	private final OperationExecutor<K, V, T> executor;

	public RedisItemWriter(Supplier<StatefulConnection<K, V>> connectionSupplier,
			GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig,
			Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> async,
			OperationExecutor<K, V, T> executor) {
		super(connectionSupplier, poolConfig);
		this.async = async;
		this.executor = executor;
	}

	@Override
	public void write(List<? extends T> items) throws Exception {
		try (StatefulConnection<K, V> connection = pool.borrowObject()) {
			BaseRedisAsyncCommands<K, V> commands = async.apply(connection);
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

	public static RedisItemWriterBuilder<String, String, KeyValue<String, byte[]>> keyDump(RedisClient client) {
		return keyDumpBuilder(client);
	}

	public static RedisItemWriterBuilder<String, String, KeyValue<String, byte[]>> keyDump(RedisClusterClient client) {
		return keyDumpBuilder(client);
	}

	private static RedisItemWriterBuilder<String, String, KeyValue<String, byte[]>> keyDumpBuilder(
			AbstractRedisClient client) {
		return new RedisItemWriterBuilder<>(client, StringCodec.UTF8, new SimpleOperationExecutor<>(
				new RestoreReplace<>(KeyValue::getKey, KeyValue<String, byte[]>::getValue, KeyValue::getAbsoluteTTL)));
	}

	public static RedisItemWriterBuilder<String, String, DataStructure<String>> dataStructure(RedisClient client) {
		return dataStructureBuilder(client);
	}

	public static RedisItemWriterBuilder<String, String, DataStructure<String>> dataStructure(
			RedisClusterClient client) {
		return dataStructureBuilder(client);
	}

	public static RedisItemWriterBuilder<String, String, DataStructure<String>> dataStructure(RedisClient client,
			Converter<StreamMessage<String, String>, XAddArgs> args) {
		return dataStructureBuilder(client, args);
	}

	public static RedisItemWriterBuilder<String, String, DataStructure<String>> dataStructure(RedisClusterClient client,
			Converter<StreamMessage<String, String>, XAddArgs> args) {
		return dataStructureBuilder(client, args);
	}

	private static RedisItemWriterBuilder<String, String, DataStructure<String>> dataStructureBuilder(
			AbstractRedisClient client) {
		return dataStructureBuilder(client, m -> new XAddArgs().id(m.getId()));
	}

	private static RedisItemWriterBuilder<String, String, DataStructure<String>> dataStructureBuilder(
			AbstractRedisClient client, Converter<StreamMessage<String, String>, XAddArgs> args) {
		return new RedisItemWriterBuilder<>(client, StringCodec.UTF8,
				new DataStructureOperationExecutor<>(client.getDefaultTimeout(), args));
	}

	public static <T> RedisItemWriterBuilder<String, String, T> operation(RedisClient client,
			RedisOperation<String, String, T> operation) {
		if (isModuleOperation(operation)) {
			Assert.isTrue(client instanceof RedisModulesClient,
					"A RedisModulesClient is required for operation " + ClassUtils.getShortName(operation.getClass()));
		}
		return operationBuilder(client, operation);
	}

	public static <T> RedisItemWriterBuilder<String, String, T> operation(RedisClusterClient client,
			RedisOperation<String, String, T> operation) {
		if (isModuleOperation(operation)) {
			Assert.isTrue(client instanceof RedisModulesClusterClient,
					"A RedisModulesClusterClient is required for operation "
							+ ClassUtils.getShortName(operation.getClass()));
		}
		return operationBuilder(client, operation);
	}

	private static <T> RedisItemWriterBuilder<String, String, T> operationBuilder(AbstractRedisClient client,
			RedisOperation<String, String, T> operation) {
		return new RedisItemWriterBuilder<>(client, StringCodec.UTF8, new SimpleOperationExecutor<>(operation));
	}

	private static boolean isModuleOperation(RedisOperation<?, ?, ?> operation) {
		return operation instanceof JsonSet || operation instanceof TsAdd || operation instanceof Sugadd;
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
			return new RedisItemWriter<>(connectionSupplier(), poolConfig, super.async(), executor);
		}

	}

}
