package com.redis.spring.batch;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.ClassUtils;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.spring.batch.common.ConnectionPoolBuilder;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.common.PoolOptions;
import com.redis.spring.batch.writer.DataStructureOperation;
import com.redis.spring.batch.writer.MultiExecOperation;
import com.redis.spring.batch.writer.Operation;
import com.redis.spring.batch.writer.PipelinedOperation;
import com.redis.spring.batch.writer.SimplePipelinedOperation;
import com.redis.spring.batch.writer.WaitForReplication;
import com.redis.spring.batch.writer.WaitForReplicationOperation;
import com.redis.spring.batch.writer.WriterOptions;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisItemWriter<K, V, T> extends AbstractItemStreamItemWriter<T> {

	private final AbstractRedisClient client;
	private final RedisCodec<K, V> codec;
	private final PoolOptions poolOptions;
	private final PipelinedOperation<K, V, T> operation;
	private GenericObjectPool<StatefulConnection<K, V>> pool;

	public RedisItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec, PoolOptions poolOptions,
			PipelinedOperation<K, V, T> operation) {
		setName(ClassUtils.getShortName(getClass()));
		this.client = client;
		this.codec = codec;
		this.poolOptions = poolOptions;
		this.operation = operation;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (pool == null) {
			pool = ConnectionPoolBuilder.client(client).options(poolOptions).codec(codec);
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void write(List<? extends T> items) throws Exception {
		try (StatefulConnection<K, V> connection = pool.borrowObject()) {
			connection.setAutoFlushCommands(false);
			try {
				Collection<RedisFuture> futures = operation.execute(connection, items);
				connection.flushCommands();
				long timeout = connection.getTimeout().toMillis();
				LettuceFutures.awaitAll(timeout, TimeUnit.MILLISECONDS, futures.toArray(new Future[0]));
			} finally {
				connection.setAutoFlushCommands(true);
			}
		}
	}

	public boolean isOpen() {
		return pool != null;
	}

	@Override
	public synchronized void close() {
		super.close();
		if (pool != null) {
			pool.close();
			pool = null;
		}
	}

	public static Builder<String, String> client(RedisModulesClient client) {
		return new Builder<>(client, StringCodec.UTF8);
	}

	public static Builder<String, String> client(RedisModulesClusterClient client) {
		return new Builder<>(client, StringCodec.UTF8);
	}

	public static <K, V> Builder<K, V> client(RedisModulesClient client, RedisCodec<K, V> codec) {
		return new Builder<>(client, codec);
	}

	public static <K, V> Builder<K, V> client(RedisModulesClusterClient client, RedisCodec<K, V> codec) {
		return new Builder<>(client, codec);
	}

	public static class Builder<K, V> {

		private final AbstractRedisClient client;
		private final RedisCodec<K, V> codec;
		private WriterOptions options = WriterOptions.builder().build();

		protected Builder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			this.client = client;
			this.codec = codec;
		}

		public Builder<K, V> options(WriterOptions options) {
			this.options = options;
			return this;
		}

		private <T> PipelinedOperation<K, V, T> waitForReplicationOperation(PipelinedOperation<K, V, T> operation) {
			Optional<WaitForReplication> waitForReplication = options.getWaitForReplication();
			if (waitForReplication.isPresent()) {
				return new WaitForReplicationOperation<>(operation, waitForReplication.get());
			}
			return operation;
		}

		private <T> RedisItemWriter<K, V, T> writer(PipelinedOperation<K, V, T> operation) {
			return new RedisItemWriter<>(client, codec, options.getPoolOptions(), operation(operation));
		}

		private <T> PipelinedOperation<K, V, T> operation(PipelinedOperation<K, V, T> operation) {
			PipelinedOperation<K, V, T> replicationOperation = waitForReplicationOperation(operation);
			if (options.isMultiExec()) {
				return new MultiExecOperation<>(replicationOperation);
			}
			return replicationOperation;
		}

		public <T> RedisItemWriter<K, V, T> operation(Operation<K, V, T> operation) {
			return writer(new SimplePipelinedOperation<>(operation));
		}

		public RedisItemWriter<K, V, DataStructure<K>> dataStructure() {
			return writer(new DataStructureOperation<>());
		}

		public RedisItemWriter<K, V, DataStructure<K>> dataStructure(
				Converter<StreamMessage<K, V>, XAddArgs> xaddArgs) {
			return writer(new DataStructureOperation<>(xaddArgs));
		}

		public RedisItemWriter<K, V, KeyDump<K>> keyDump() {
			return writer(SimplePipelinedOperation.keyDump());
		}

	}

}
