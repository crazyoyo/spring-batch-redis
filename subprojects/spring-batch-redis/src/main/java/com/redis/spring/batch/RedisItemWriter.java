package com.redis.spring.batch;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.spring.batch.builder.RedisBuilder;
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

	private static final Logger log = LoggerFactory.getLogger(RedisItemWriter.class);

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
		log.debug("Getting connection from pool");
		try (StatefulConnection<K, V> connection = pool.borrowObject()) {
			BaseRedisAsyncCommands<K, V> commands = async.apply(connection);
			commands.setAutoFlushCommands(false);
			try {
				List<Future<?>> futures = executor.execute(commands, items);
				commands.flushCommands();
				LettuceFutures.awaitAll(connection.getTimeout().toMillis(), TimeUnit.MILLISECONDS,
						futures.toArray(new Future[0]));
				log.debug("Wrote {} items", items.size());
			} finally {
				commands.setAutoFlushCommands(true);
			}
		}
	}

	public static OperationItemWriterBuilder<String, String> client(RedisClient client) {
		return new OperationItemWriterBuilder<>(client, StringCodec.UTF8);
	}

	public static OperationItemWriterBuilder<String, String> client(RedisClusterClient client) {
		return new OperationItemWriterBuilder<>(client, StringCodec.UTF8);
	}

	public static class RedisItemWriterBuilder<K, V, T>
			extends BaseRedisItemWriterBuilder<K, V, T, RedisItemWriterBuilder<K, V, T>> {

		public RedisItemWriterBuilder(AbstractRedisClient client, RedisCodec<K, V> codec,
				OperationExecutor<K, V, T> executor) {
			super(client, codec, executor);
		}

	}

	public static class OperationItemWriterBuilder<K, V> {

		private final AbstractRedisClient client;
		private final RedisCodec<K, V> codec;

		public OperationItemWriterBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			this.client = client;
			this.codec = codec;
		}

		public <T> RedisItemWriterBuilder<K, V, T> operation(RedisOperation<K, V, T> operation) {
			if (operation instanceof JsonSet || operation instanceof TsAdd || operation instanceof Sugadd) {
				Assert.isTrue(client instanceof RedisModulesClusterClient || client instanceof RedisModulesClient,
						"A Redis modules client is required for operation "
								+ ClassUtils.getShortName(operation.getClass()));
			}
			return new RedisItemWriterBuilder<>(client, codec, new SimpleOperationExecutor<>(operation));
		}

		public DataStructureItemWriterBuilder<K, V> dataStructure() {
			return new DataStructureItemWriterBuilder<>(client, codec);
		}

		public RedisItemWriterBuilder<K, V, KeyValue<K, byte[]>> keyDump() {
			RestoreReplace<K, V, KeyValue<K, byte[]>> operation = new RestoreReplace<>(KeyValue::getKey,
					KeyValue<K, byte[]>::getValue, KeyValue::getAbsoluteTTL);
			return new RedisItemWriterBuilder<>(client, codec, new SimpleOperationExecutor<>(operation));
		}
	}

	public static class BaseRedisItemWriterBuilder<K, V, T, B extends BaseRedisItemWriterBuilder<K, V, T, B>>
			extends RedisBuilder<K, V, B> {

		protected OperationExecutor<K, V, T> executor;

		public BaseRedisItemWriterBuilder(AbstractRedisClient client, RedisCodec<K, V> codec,
				OperationExecutor<K, V, T> executor) {
			super(client, codec);
			this.executor = executor;
		}

		@SuppressWarnings("unchecked")
		public B multiExec() {
			this.executor = new MultiExecOperationExecutor<>(executor);
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B waitForReplication(int replicas, long timeout) {
			this.executor = new WaitForReplicationOperationExecutor<>(executor, replicas, timeout);
			return (B) this;
		}

		public RedisItemWriter<K, V, T> build() {
			return new RedisItemWriter<>(connectionSupplier(), poolConfig, super.async(), executor);
		}

	}

	public static class DataStructureItemWriterBuilder<K, V>
			extends BaseRedisItemWriterBuilder<K, V, DataStructure<K>, DataStructureItemWriterBuilder<K, V>> {

		public DataStructureItemWriterBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			super(client, codec, dataStructureOperationExecutor(client));
		}

		private static <K, V> OperationExecutor<K, V, DataStructure<K>> dataStructureOperationExecutor(
				AbstractRedisClient client) {
			DataStructureOperationExecutor<K, V> executor = new DataStructureOperationExecutor<>();
			executor.setTimeout(client.getDefaultTimeout());
			return executor;
		}

		public DataStructureItemWriterBuilder<K, V> xaddArgs(Converter<StreamMessage<K, V>, XAddArgs> xaddArgs) {
			((DataStructureOperationExecutor<K, V>) executor).setXaddArgs(xaddArgs);
			return this;
		}

	}

}
