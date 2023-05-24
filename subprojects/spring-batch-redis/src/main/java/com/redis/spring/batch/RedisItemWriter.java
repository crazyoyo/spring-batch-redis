package com.redis.spring.batch;

import java.util.List;
import java.util.Optional;

import org.springframework.batch.item.ItemStreamWriter;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.spring.batch.common.AsyncOperation;
import com.redis.spring.batch.common.BatchAsyncOperation;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.common.OperationItemStreamSupport;
import com.redis.spring.batch.common.PoolOptions;
import com.redis.spring.batch.common.SimpleBatchAsyncOperation;
import com.redis.spring.batch.writer.DataStructureWriteOperation;
import com.redis.spring.batch.writer.DataStructureWriteOptions;
import com.redis.spring.batch.writer.MultiExecOperation;
import com.redis.spring.batch.writer.ReplicaWaitOperation;
import com.redis.spring.batch.writer.ReplicaWaitOptions;
import com.redis.spring.batch.writer.operation.RestoreReplace;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisItemWriter<K, V, T> extends OperationItemStreamSupport<K, V, T, Object>
		implements ItemStreamWriter<T> {

	public RedisItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec, PoolOptions poolOptions,
			BatchAsyncOperation<K, V, T, Object> operation) {
		super(client, codec, poolOptions, operation);
	}

	@Override
	public void write(List<? extends T> items) throws Exception {
		process(items);
	}

	public static DataStructureBuilder<String, String> dataStructure(RedisModulesClient client) {
		return dataStructure(client, StringCodec.UTF8);
	}

	public static DataStructureBuilder<String, String> dataStructure(RedisModulesClusterClient client) {
		return dataStructure(client, StringCodec.UTF8);
	}

	public static <K, V> DataStructureBuilder<K, V> dataStructure(RedisModulesClient client, RedisCodec<K, V> codec) {
		return new DataStructureBuilder<>(client, codec);
	}

	public static <K, V> DataStructureBuilder<K, V> dataStructure(RedisModulesClusterClient client,
			RedisCodec<K, V> codec) {
		return new DataStructureBuilder<>(client, codec);
	}

	public static KeyDumpBuilder<byte[], byte[]> keyDump(RedisModulesClusterClient client) {
		return new KeyDumpBuilder<>(client, ByteArrayCodec.INSTANCE);
	}

	public static KeyDumpBuilder<byte[], byte[]> keyDump(RedisModulesClient client) {
		return new KeyDumpBuilder<>(client, ByteArrayCodec.INSTANCE);
	}

	public abstract static class AbstractBuilder<K, V, T, B extends AbstractBuilder<K, V, T, B>> {

		private final AbstractRedisClient client;
		private final RedisCodec<K, V> codec;

		private PoolOptions poolOptions = PoolOptions.builder().build();
		private Optional<ReplicaWaitOptions> waitForReplication = Optional.empty();
		private boolean multiExec;

		protected AbstractBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			this.client = client;
			this.codec = codec;
		}

		@SuppressWarnings("unchecked")
		public B poolOptions(PoolOptions options) {
			this.poolOptions = options;
			return (B) this;
		}

		public B waitForReplication(ReplicaWaitOptions waitForReplication) {
			return waitForReplication(Optional.of(waitForReplication));
		}

		@SuppressWarnings("unchecked")
		public B waitForReplication(Optional<ReplicaWaitOptions> waitForReplication) {
			this.waitForReplication = waitForReplication;
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B multiExec(boolean multiExec) {
			this.multiExec = multiExec;
			return (B) this;
		}

		public RedisItemWriter<K, V, T> build() {
			BatchAsyncOperation<K, V, T, Object> operation = getOperation();
			return new RedisItemWriter<>(client, codec, poolOptions, multiExec(replicaWait(operation)));
		}

		private BatchAsyncOperation<K, V, T, Object> replicaWait(BatchAsyncOperation<K, V, T, Object> operation) {
			if (waitForReplication.isPresent()) {
				return new ReplicaWaitOperation<>(operation, waitForReplication.get());
			}
			return operation;
		}

		private BatchAsyncOperation<K, V, T, Object> multiExec(BatchAsyncOperation<K, V, T, Object> operation) {
			if (multiExec) {
				return new MultiExecOperation<>(operation);
			}
			return operation;
		}

		protected abstract BatchAsyncOperation<K, V, T, Object> getOperation();

	}

	public static class OperationBuilder<K, V, T> extends AbstractBuilder<K, V, T, OperationBuilder<K, V, T>> {

		private final AsyncOperation<K, V, T, Object> operation;

		public OperationBuilder(AbstractRedisClient client, RedisCodec<K, V> codec,
				AsyncOperation<K, V, T, Object> operation) {
			super(client, codec);
			this.operation = operation;
		}

		@Override
		protected BatchAsyncOperation<K, V, T, Object> getOperation() {
			return new SimpleBatchAsyncOperation<>(operation);
		}

	}

	public static class KeyDumpBuilder<K, V> extends AbstractBuilder<K, V, KeyDump<K>, KeyDumpBuilder<K, V>> {

		public KeyDumpBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			super(client, codec);
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Override
		protected BatchAsyncOperation<K, V, KeyDump<K>, Object> getOperation() {
			return (BatchAsyncOperation) new SimpleBatchAsyncOperation<>(RestoreReplace.keyDump());
		}

	}

	public static class DataStructureBuilder<K, V>
			extends AbstractBuilder<K, V, DataStructure<K>, DataStructureBuilder<K, V>> {

		private DataStructureWriteOptions options = DataStructureWriteOptions.builder().build();

		public DataStructureBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			super(client, codec);
		}

		public DataStructureBuilder<K, V> dataStructureOptions(DataStructureWriteOptions options) {
			this.options = options;
			return this;
		}

		@Override
		protected DataStructureWriteOperation<K, V> getOperation() {
			return new DataStructureWriteOperation<>(options);
		}

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T> OperationBuilder<String, String, T> operation(AbstractRedisClient client,
			AsyncOperation<String, String, T, ?> operation) {
		return new OperationBuilder<>(client, StringCodec.UTF8, (AsyncOperation) operation);
	}

}
