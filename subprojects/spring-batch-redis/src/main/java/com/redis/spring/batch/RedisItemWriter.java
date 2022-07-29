package com.redis.spring.batch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
import com.redis.spring.batch.support.RedisConnectionBuilder;
import com.redis.spring.batch.writer.DataStructureOperationExecutor;
import com.redis.spring.batch.writer.MultiExecOperationExecutor;
import com.redis.spring.batch.writer.OperationExecutor;
import com.redis.spring.batch.writer.RedisOperation;
import com.redis.spring.batch.writer.SimpleOperationExecutor;
import com.redis.spring.batch.writer.WaitForReplicationOperationExecutor;
import com.redis.spring.batch.writer.operation.JsonSet;
import com.redis.spring.batch.writer.operation.RestoreReplace;
import com.redis.spring.batch.writer.operation.Sugadd;
import com.redis.spring.batch.writer.operation.TsAdd;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
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
			extends AbstractBuilder<K, V, DataStructure<K>, DataStructureBuilder<K, V>> {

		private Optional<Converter<StreamMessage<K, V>, XAddArgs>> xaddArgs = Optional.empty();

		public DataStructureBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			super(client, codec);
		}

		@Override
		protected OperationExecutor<K, V, DataStructure<K>> getOperationExecutor() {
			DataStructureOperationExecutor<K, V> executor = new DataStructureOperationExecutor<>(codec);
			xaddArgs.ifPresent(executor::setXaddArgs);
			return executor;
		}

		public DataStructureBuilder<K, V> xaddArgs(Converter<StreamMessage<K, V>, XAddArgs> xaddArgs) {
			this.xaddArgs = Optional.of(xaddArgs);
			return this;
		}

	}

	public static CodecBuilder client(AbstractRedisClient client) {
		return new CodecBuilder(client);
	}

	public static class CodecBuilder {

		private final AbstractRedisClient client;

		public CodecBuilder(AbstractRedisClient client) {
			this.client = client;
		}

		public <K, V> OperationBuilder<K, V> codec(RedisCodec<K, V> codec) {
			return new OperationBuilder<>(client, codec);
		}

		public OperationBuilder<String, String> string() {
			return new OperationBuilder<>(client, StringCodec.UTF8);
		}

		public OperationBuilder<byte[], byte[]> bytes() {
			return new OperationBuilder<>(client, ByteArrayCodec.INSTANCE);
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
					KeyValue<K, byte[]>::getValue, KeyValue::getTtl);
			return new Builder<>(client, codec, new SimpleOperationExecutor<>(operation));
		}
	}

	public static class Builder<K, V, T> extends AbstractBuilder<K, V, T, Builder<K, V, T>> {

		private final OperationExecutor<K, V, T> executor;

		public Builder(AbstractRedisClient client, RedisCodec<K, V> codec, OperationExecutor<K, V, T> executor) {
			super(client, codec);
			this.executor = executor;
		}

		@Override
		protected OperationExecutor<K, V, T> getOperationExecutor() {
			return executor;
		}

	}

	public abstract static class AbstractBuilder<K, V, T, B extends AbstractBuilder<K, V, T, B>>
			extends RedisConnectionBuilder<K, V, B> {

		private boolean multiExec;
		private Optional<WaitForReplication> waitForReplication = Optional.empty();

		protected AbstractBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			super(client, codec);
		}

		@SuppressWarnings("unchecked")
		public B multiExec() {
			this.multiExec = true;
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B waitForReplication(WaitForReplication replication) {
			this.waitForReplication = Optional.of(replication);
			return (B) this;
		}

		protected OperationExecutor<K, V, T> operationExecutor() {
			OperationExecutor<K, V, T> executor = getOperationExecutor();
			if (multiExec) {
				executor = new MultiExecOperationExecutor<>(executor);
			}
			if (waitForReplication.isPresent()) {
				return new WaitForReplicationOperationExecutor<>(executor, waitForReplication.get());
			}
			return executor;
		}

		protected abstract OperationExecutor<K, V, T> getOperationExecutor();

		public RedisItemWriter<K, V, T> build() {
			return new RedisItemWriter<>(connectionSupplier(), poolConfig, super.async(), operationExecutor());
		}

	}

	public static class WaitForReplication {

		private final int replicas;
		private final Duration timeout;

		private WaitForReplication(Builder builder) {
			this.replicas = builder.replicas;
			this.timeout = builder.timeout;
		}

		public int getReplicas() {
			return replicas;
		}

		public Duration getTimeout() {
			return timeout;
		}

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {

			public static final int DEFAULT_REPLICAS = 1;
			public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(1);

			private int replicas = DEFAULT_REPLICAS;
			private Duration timeout = DEFAULT_TIMEOUT;

			private Builder() {
			}

			public Builder replicas(int replicas) {
				this.replicas = replicas;
				return this;
			}

			public Builder timeout(Duration timeout) {
				Assert.notNull(timeout, "Timeout must not be null");
				Assert.isTrue(!timeout.isNegative(), "Timeout must not be negative");
				this.timeout = timeout;
				return this;
			}

			public WaitForReplication build() {
				return new WaitForReplication(this);
			}
		}

	}
}
