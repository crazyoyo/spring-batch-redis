package com.redis.spring.batch;

import java.util.ArrayList;
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
import com.redis.spring.batch.support.ConnectionPoolItemStream;
import com.redis.spring.batch.writer.AbstractRedisItemWriterBuilder;
import com.redis.spring.batch.writer.DataStructureOperationExecutor;
import com.redis.spring.batch.writer.OperationExecutor;
import com.redis.spring.batch.writer.RedisOperation;
import com.redis.spring.batch.writer.SimpleOperationExecutor;
import com.redis.spring.batch.writer.operation.JsonSet;
import com.redis.spring.batch.writer.operation.RestoreReplace;
import com.redis.spring.batch.writer.operation.Sugadd;
import com.redis.spring.batch.writer.operation.TsAdd;

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
		log.trace("Getting connection from pool");
		try (StatefulConnection<K, V> connection = borrowConnection()) {
			BaseRedisAsyncCommands<K, V> commands = async.apply(connection);
			commands.setAutoFlushCommands(false);
			List<Future<?>> futures = new ArrayList<>();
			try {
				executor.execute(commands, items, futures);
				commands.flushCommands();
				LettuceFutures.awaitAll(connection.getTimeout().toMillis(), TimeUnit.MILLISECONDS,
						futures.toArray(new Future[0]));
				log.trace("Wrote {} items", items.size());
			} finally {
				commands.setAutoFlushCommands(true);
			}
		}
	}

	public static class DataStructureBuilder<K, V>
			extends AbstractRedisItemWriterBuilder<K, V, DataStructure<K>, DataStructureBuilder<K, V>> {

		public DataStructureBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			super(client, codec, operationExecutor(client, codec));
		}

		private static <K, V> OperationExecutor<K, V, DataStructure<K>> operationExecutor(AbstractRedisClient client,
				RedisCodec<K, V> codec) {
			DataStructureOperationExecutor<K, V> executor = new DataStructureOperationExecutor<>(codec);
			executor.setTimeout(client.getDefaultTimeout());
			return executor;
		}

		public DataStructureBuilder<K, V> xaddArgs(Converter<StreamMessage<K, V>, XAddArgs> xaddArgs) {
			((DataStructureOperationExecutor<K, V>) executor).setXaddArgs(xaddArgs);
			return this;
		}

	}

	public static class OperationBuilder<K, V> {

		private final AbstractRedisClient client;
		private final RedisCodec<K, V> codec;

		public OperationBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			this.client = client;
			this.codec = codec;
		}

		public <T> Builder<K, V, T> operation(RedisOperation<K, V, T> operation) {
			if (operation instanceof JsonSet || operation instanceof TsAdd || operation instanceof Sugadd) {
				Assert.isTrue(client instanceof RedisModulesClusterClient || client instanceof RedisModulesClient,
						"A Redis modules client is required for operation "
								+ ClassUtils.getShortName(operation.getClass()));
			}
			return new Builder<>(client, codec, new SimpleOperationExecutor<>(operation));
		}

		public DataStructureBuilder<K, V> dataStructure() {
			return new DataStructureBuilder<>(client, codec);
		}

		public Builder<K, V, KeyValue<K, byte[]>> keyDump() {
			RestoreReplace<K, V, KeyValue<K, byte[]>> operation = new RestoreReplace<>(KeyValue::getKey,
					KeyValue<K, byte[]>::getValue, KeyValue::getAbsoluteTTL);
			return new Builder<>(client, codec, new SimpleOperationExecutor<>(operation));
		}
	}

	public static OperationBuilder<String, String> client(RedisClient client) {
		return new OperationBuilder<>(client, StringCodec.UTF8);
	}

	public static OperationBuilder<String, String> client(RedisClusterClient client) {
		return new OperationBuilder<>(client, StringCodec.UTF8);
	}

	public static <K, V> OperationBuilder<K, V> client(RedisClient client, RedisCodec<K, V> codec) {
		return new OperationBuilder<>(client, codec);
	}

	public static <K, V> OperationBuilder<K, V> client(RedisClusterClient client, RedisCodec<K, V> codec) {
		return new OperationBuilder<>(client, codec);
	}

	public static class Builder<K, V, T> extends AbstractRedisItemWriterBuilder<K, V, T, Builder<K, V, T>> {

		public Builder(AbstractRedisClient client, RedisCodec<K, V> codec, OperationExecutor<K, V, T> executor) {
			super(client, codec, executor);
		}

	}

}
