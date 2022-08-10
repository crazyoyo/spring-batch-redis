package com.redis.spring.batch;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.convert.converter.Converter;

import com.redis.spring.batch.support.ConnectionPoolItemStream;
import com.redis.spring.batch.support.RedisConnectionBuilder;
import com.redis.spring.batch.writer.DataStructureOperation;
import com.redis.spring.batch.writer.MultiExecOperation;
import com.redis.spring.batch.writer.Operation;
import com.redis.spring.batch.writer.PipelinedOperation;
import com.redis.spring.batch.writer.SimplePipelinedOperation;
import com.redis.spring.batch.writer.WaitForReplicationOperation;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisItemWriter<K, V, T> extends ConnectionPoolItemStream<K, V> implements ItemWriter<T> {

	private final Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> async;
	private final PipelinedOperation<K, V, T> operation;

	public RedisItemWriter(Supplier<StatefulConnection<K, V>> connectionSupplier,
			GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig,
			Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> async,
			PipelinedOperation<K, V, T> operation) {
		super(connectionSupplier, poolConfig);
		this.async = async;
		this.operation = operation;
	}

	@Override
	public void write(List<? extends T> items) throws Exception {
		try (StatefulConnection<K, V> connection = borrowConnection()) {
			BaseRedisAsyncCommands<K, V> commands = async.apply(connection);
			commands.setAutoFlushCommands(false);
			Collection<RedisFuture<?>> futures = operation.execute(commands, items);
			commands.flushCommands();
			long timeout = connection.getTimeout().toMillis();
			try {
				LettuceFutures.awaitAll(timeout, TimeUnit.MILLISECONDS, futures.toArray(Future[]::new));
			} finally {
				commands.setAutoFlushCommands(true);
			}
		}
	}

	public static <K, V> OperationBuilder<K, V> operation(AbstractRedisClient client, RedisCodec<K, V> codec) {
		return new OperationBuilder<>(client, codec);
	}

	public static OperationBuilder<String, String> operation(AbstractRedisClient client) {
		return new OperationBuilder<>(client, StringCodec.UTF8);
	}

	public static <K, V, O extends PipelinedOperation<K, V, DataStructure<K>>> Builder<K, V, DataStructure<K>> dataStructure(
			AbstractRedisClient client, RedisCodec<K, V> codec) {
		return new Builder<>(client, codec, new DataStructureOperation<>(codec));
	}

	public static <K, V> Builder<K, V, DataStructure<K>> dataStructure(AbstractRedisClient client,
			RedisCodec<K, V> codec, Converter<StreamMessage<K, V>, XAddArgs> xaddArgs) {
		DataStructureOperation<K, V> executor = new DataStructureOperation<>(codec);
		executor.getXadd().setArgs(xaddArgs);
		return new Builder<>(client, codec, executor);
	}

	public static Builder<String, String, DataStructure<String>> dataStructure(AbstractRedisClient client) {
		return new Builder<>(client, StringCodec.UTF8, new DataStructureOperation<>(StringCodec.UTF8));
	}

	public static Builder<String, String, DataStructure<String>> dataStructure(AbstractRedisClient client,
			Converter<StreamMessage<String, String>, XAddArgs> xaddArgs) {
		DataStructureOperation<String, String> executor = new DataStructureOperation<>(StringCodec.UTF8);
		executor.getXadd().setArgs(xaddArgs);
		return new Builder<>(client, StringCodec.UTF8, executor);
	}

	public static <K, V> Builder<K, V, KeyValue<K, byte[]>> keyDump(AbstractRedisClient client,
			RedisCodec<K, V> codec) {
		return new Builder<>(client, codec, SimplePipelinedOperation.keyDump());
	}

	public static Builder<String, String, KeyValue<String, byte[]>> keyDump(AbstractRedisClient client) {
		return new Builder<>(client, StringCodec.UTF8, SimplePipelinedOperation.keyDump());
	}

	public static class OperationBuilder<K, V> {

		private final AbstractRedisClient client;
		private final RedisCodec<K, V> codec;

		public OperationBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			this.client = client;
			this.codec = codec;
		}

		public <T> Builder<K, V, T> operation(Operation<K, V, T> operation) {
			return new Builder<>(client, codec, operation);
		}

	}

	public static class Builder<K, V, T> extends RedisConnectionBuilder<K, V, Builder<K, V, T>> {

		private final PipelinedOperation<K, V, T> operation;
		private boolean multiExec;
		private Optional<WaitForReplication> waitForReplication = Optional.empty();

		public Builder(AbstractRedisClient client, RedisCodec<K, V> codec, Operation<K, V, T> operation) {
			this(client, codec, new SimplePipelinedOperation<>(operation));
		}

		public Builder(AbstractRedisClient client, RedisCodec<K, V> codec, PipelinedOperation<K, V, T> operation) {
			super(client, codec);
			this.operation = operation;
		}

		public Builder<K, V, T> multiExec() {
			this.multiExec = true;
			return this;
		}

		public Builder<K, V, T> waitForReplication(WaitForReplication replication) {
			this.waitForReplication = Optional.of(replication);
			return this;
		}

		private PipelinedOperation<K, V, T> operation() {
			PipelinedOperation<K, V, T> op = multiExec ? new MultiExecOperation<>(operation) : operation;
			if (waitForReplication.isPresent()) {
				return new WaitForReplicationOperation<>(op, waitForReplication.get());
			}
			return op;
		}

		public RedisItemWriter<K, V, T> build() {
			return new RedisItemWriter<>(connectionSupplier(), poolConfig, super.async(), operation());
		}

	}

	public static class WaitForReplication {

		public static final int DEFAULT_REPLICAS = 1;
		public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(1);

		private final int replicas;
		private final Duration timeout;

		private WaitForReplication(int replicas, Duration timeout) {
			this.replicas = replicas;
			this.timeout = timeout;
		}

		public int getReplicas() {
			return replicas;
		}

		public Duration getTimeout() {
			return timeout;
		}

		public static WaitForReplication of(int replicas, Duration timeout) {
			return new WaitForReplication(replicas, timeout);
		}

		public static WaitForReplication of(int replicas) {
			return new WaitForReplication(replicas, DEFAULT_TIMEOUT);
		}

		public static WaitForReplication of(Duration timeout) {
			return new WaitForReplication(DEFAULT_REPLICAS, timeout);
		}

	}
}
