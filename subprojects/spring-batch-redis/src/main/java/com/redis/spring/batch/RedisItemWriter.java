package com.redis.spring.batch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.spring.batch.common.ConnectionPoolBuilder;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.common.Openable;
import com.redis.spring.batch.common.PoolOptions;
import com.redis.spring.batch.common.Utils;
import com.redis.spring.batch.writer.BatchWriteOperation;
import com.redis.spring.batch.writer.DataStructureWriteOperation;
import com.redis.spring.batch.writer.MultiExecWriteOperation;
import com.redis.spring.batch.writer.OverwriteOperation;
import com.redis.spring.batch.writer.ReplicatedWriteOperation;
import com.redis.spring.batch.writer.SimpleBatchWriteOperation;
import com.redis.spring.batch.writer.WaitForReplication;
import com.redis.spring.batch.writer.WriteOperation;
import com.redis.spring.batch.writer.operation.Hset;
import com.redis.spring.batch.writer.operation.JsonSet;
import com.redis.spring.batch.writer.operation.RestoreReplace;
import com.redis.spring.batch.writer.operation.RpushAll;
import com.redis.spring.batch.writer.operation.SaddAll;
import com.redis.spring.batch.writer.operation.Set;
import com.redis.spring.batch.writer.operation.TsAddAll;
import com.redis.spring.batch.writer.operation.XAddAll;
import com.redis.spring.batch.writer.operation.Xadd;
import com.redis.spring.batch.writer.operation.ZaddAll;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisItemWriter<K, V, T> extends AbstractItemStreamItemWriter<T> implements Openable {

	private final AbstractRedisClient client;
	private final RedisCodec<K, V> codec;
	private final PoolOptions poolOptions;
	private final BatchWriteOperation<K, V, T> operation;
	private GenericObjectPool<StatefulConnection<K, V>> pool;

	public RedisItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec, PoolOptions poolOptions,
			BatchWriteOperation<K, V, T> operation) {
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

	@Override
	public void write(List<? extends T> items) throws Exception {
		try (StatefulConnection<K, V> connection = pool.borrowObject()) {
			connection.setAutoFlushCommands(false);
			List<RedisFuture<?>> futures = new ArrayList<>();
			BaseRedisAsyncCommands<K, V> commands = Utils.async(connection);
			try {
				operation.execute(commands, futures, items);
				connection.flushCommands();
				long timeout = connection.getTimeout().toMillis();
				LettuceFutures.awaitAll(timeout, TimeUnit.MILLISECONDS, futures.toArray(new Future[0]));
			} finally {
				connection.setAutoFlushCommands(true);
			}
		}
	}

	@Override
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

	public static WriterBuilder<String, String> client(RedisModulesClient client) {
		return new WriterBuilder<>(client, StringCodec.UTF8);
	}

	public static WriterBuilder<String, String> client(RedisModulesClusterClient client) {
		return new WriterBuilder<>(client, StringCodec.UTF8);
	}

	public static <K, V> WriterBuilder<K, V> client(RedisModulesClient client, RedisCodec<K, V> codec) {
		return new WriterBuilder<>(client, codec);
	}

	public static <K, V> WriterBuilder<K, V> client(RedisModulesClusterClient client, RedisCodec<K, V> codec) {
		return new WriterBuilder<>(client, codec);
	}

	public static class WriterBuilder<K, V> {

		private final AbstractRedisClient client;
		private final RedisCodec<K, V> codec;

		public WriterBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			this.client = client;
			this.codec = codec;
		}

		public DataStructureBuilder<K, V> dataStructure() {
			return new DataStructureBuilder<>(client, codec);
		}

		public KeyDumpBuilder<K, V> keyDump() {
			return new KeyDumpBuilder<>(client, codec);
		}

		public <T> OperationBuilder<K, V, T> operation(WriteOperation<K, V, T> operation) {
			return new OperationBuilder<>(client, codec, operation);
		}

	}

	public abstract static class Builder<K, V, T, B extends Builder<K, V, T, B>> {

		private final AbstractRedisClient client;
		private final RedisCodec<K, V> codec;
		private PoolOptions poolOptions = PoolOptions.builder().build();
		private Optional<WaitForReplication> waitForReplication = Optional.empty();
		private boolean multiExec;

		@SuppressWarnings("unchecked")
		public B poolOptions(PoolOptions options) {
			this.poolOptions = options;
			return (B) this;
		}

		public B waitForReplication(WaitForReplication waitForReplication) {
			return waitForReplication(Optional.of(waitForReplication));
		}

		@SuppressWarnings("unchecked")
		public B waitForReplication(Optional<WaitForReplication> waitForReplication) {
			this.waitForReplication = waitForReplication;
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B multiExec(boolean multiExec) {
			this.multiExec = multiExec;
			return (B) this;
		}

		protected Builder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			this.client = client;
			this.codec = codec;
		}

		public RedisItemWriter<K, V, T> build() {
			BatchWriteOperation<K, V, T> operation = getOperation();
			if (waitForReplication.isPresent()) {
				operation = new ReplicatedWriteOperation<>(operation, waitForReplication.get());
			}
			if (multiExec) {
				operation = new MultiExecWriteOperation<>(operation);
			}
			return new RedisItemWriter<>(client, codec, poolOptions, operation);
		}

		protected abstract BatchWriteOperation<K, V, T> getOperation();

	}

	public static class OperationBuilder<K, V, T> extends Builder<K, V, T, OperationBuilder<K, V, T>> {

		private final WriteOperation<K, V, T> operation;

		public OperationBuilder(AbstractRedisClient client, RedisCodec<K, V> codec, WriteOperation<K, V, T> operation) {
			super(client, codec);
			this.operation = operation;
		}

		@Override
		protected BatchWriteOperation<K, V, T> getOperation() {
			return new SimpleBatchWriteOperation<>(operation);
		}

	}

	public static class KeyDumpBuilder<K, V> extends Builder<K, V, KeyDump<K>, KeyDumpBuilder<K, V>> {

		public KeyDumpBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			super(client, codec);
		}

		@Override
		protected BatchWriteOperation<K, V, KeyDump<K>> getOperation() {
			return new SimpleBatchWriteOperation<>(RestoreReplace.keyDump());
		}

	}

	public static class DataStructureBuilder<K, V> extends Builder<K, V, DataStructure<K>, DataStructureBuilder<K, V>> {

		public enum StreamIdPolicy {
			PROPAGATE, DROP
		}

		public enum MergePolicy {
			MERGE, OVERWRITE
		}

		public static final StreamIdPolicy DEFAULT_STREAM_ID_POLICY = StreamIdPolicy.DROP;
		public static final MergePolicy DEFAULT_MERGE_POLICY = MergePolicy.OVERWRITE;

		private StreamIdPolicy streamIdPolicy = DEFAULT_STREAM_ID_POLICY;
		private MergePolicy mergePolicy = DEFAULT_MERGE_POLICY;

		public DataStructureBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			super(client, codec);
		}

		public DataStructureBuilder<K, V> streamIdPolicy(StreamIdPolicy streamIdPolicy) {
			this.streamIdPolicy = streamIdPolicy;
			return this;
		}

		public DataStructureBuilder<K, V> mergePolicy(MergePolicy mergePolicy) {
			this.mergePolicy = mergePolicy;
			return this;
		}

		@Override
		protected BatchWriteOperation<K, V, DataStructure<K>> getOperation() {
			Map<String, WriteOperation<K, V, DataStructure<K>>> operations = new HashMap<>();
			operations.put(DataStructure.HASH, configure(hset()));
			operations.put(DataStructure.STRING, set());
			operations.put(DataStructure.JSON, jsonSet());
			operations.put(DataStructure.LIST, configure(rpush()));
			operations.put(DataStructure.SET, configure(sadd()));
			operations.put(DataStructure.ZSET, configure(zadd()));
			operations.put(DataStructure.TIMESERIES, configure(tsAdd()));
			operations.put(DataStructure.STREAM, configure(xadd(xaddArgs())));
			return new SimpleBatchWriteOperation<>(new DataStructureWriteOperation<>(operations));
		}

		private WriteOperation<K, V, DataStructure<K>> configure(WriteOperation<K, V, DataStructure<K>> operation) {
			if (mergePolicy == MergePolicy.MERGE) {
				return operation;
			}
			return new OverwriteOperation<>(operation);
		}

		private Function<StreamMessage<K, V>, XAddArgs> xaddArgs() {
			if (streamIdPolicy == StreamIdPolicy.PROPAGATE) {
				return Xadd.idArgs();
			}
			return t -> null;
		}

		private Hset<K, V, DataStructure<K>> hset() {
			return new Hset<>(DataStructure::getKey, DataStructure::getValue);
		}

		private Set<K, V, DataStructure<K>> set() {
			return new Set<>(DataStructure::getKey, DataStructure::getValue);
		}

		private JsonSet<K, V, DataStructure<K>> jsonSet() {
			return new JsonSet<>(DataStructure::getKey, DataStructure::getValue);
		}

		private RpushAll<K, V, DataStructure<K>> rpush() {
			return new RpushAll<>(DataStructure::getKey, DataStructure::getValue);
		}

		private SaddAll<K, V, DataStructure<K>> sadd() {
			return new SaddAll<>(DataStructure::getKey, DataStructure::getValue);
		}

		private ZaddAll<K, V, DataStructure<K>> zadd() {
			return new ZaddAll<>(DataStructure::getKey, DataStructure::getValue);
		}

		private TsAddAll<K, V, DataStructure<K>> tsAdd() {
			return new TsAddAll<>(DataStructure::getKey, DataStructure::getValue, AddOptions.<K, V>builder().build());
		}

		private XAddAll<K, V, DataStructure<K>> xadd(Function<StreamMessage<K, V>, XAddArgs> args) {
			return new XAddAll<>(DataStructure::getKey, DataStructure::getValue, args);
		}

	}

}
