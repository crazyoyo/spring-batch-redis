package com.redis.spring.batch;

import java.util.List;

import org.springframework.batch.item.ItemStreamWriter;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.spring.batch.common.BatchOperation;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.common.OperationItemStreamSupport;
import com.redis.spring.batch.common.SimpleBatchOperation;
import com.redis.spring.batch.writer.DataStructureWriteOperation;
import com.redis.spring.batch.writer.DataStructureWriteOptions;
import com.redis.spring.batch.writer.MultiExecWriteOperation;
import com.redis.spring.batch.writer.ReplicaWaitWriteOperation;
import com.redis.spring.batch.writer.WriterOptions;
import com.redis.spring.batch.writer.operation.RestoreReplace;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisItemWriter<K, V, T> extends OperationItemStreamSupport<K, V, T, Object>
		implements ItemStreamWriter<T> {

	public RedisItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec,
			BatchOperation<K, V, T, Object> operation, WriterOptions options) {
		super(client, codec, options.getPoolOptions(), multiExec(replicaWait(operation, options), options));
	}

	private static <K, V, T> BatchOperation<K, V, T, Object> replicaWait(BatchOperation<K, V, T, Object> operation,
			WriterOptions options) {
		if (options.getReplicaOptions().isPresent()) {
			return new ReplicaWaitWriteOperation<>(operation, options.getReplicaOptions().get());
		}
		return operation;
	}

	private static <K, V, T> BatchOperation<K, V, T, Object> multiExec(BatchOperation<K, V, T, Object> operation,
			WriterOptions options) {
		if (options.isMultiExec()) {
			return new MultiExecWriteOperation<>(operation);
		}
		return operation;
	}

	@Override
	public void write(List<? extends T> items) throws Exception {
		process(items);
	}

	public static DataStructureWriterBuilder<String, String> dataStructure(RedisModulesClient client) {
		return dataStructure(client, StringCodec.UTF8);
	}

	public static DataStructureWriterBuilder<String, String> dataStructure(RedisModulesClusterClient client) {
		return dataStructure(client, StringCodec.UTF8);
	}

	public static <K, V> DataStructureWriterBuilder<K, V> dataStructure(RedisModulesClient client,
			RedisCodec<K, V> codec) {
		return new DataStructureWriterBuilder<>(client, codec);
	}

	public static <K, V> DataStructureWriterBuilder<K, V> dataStructure(RedisModulesClusterClient client,
			RedisCodec<K, V> codec) {
		return new DataStructureWriterBuilder<>(client, codec);
	}

	public static KeyDumpWriterBuilder<byte[], byte[]> keyDump(RedisModulesClusterClient client) {
		return new KeyDumpWriterBuilder<>(client, ByteArrayCodec.INSTANCE);
	}

	public static KeyDumpWriterBuilder<byte[], byte[]> keyDump(RedisModulesClient client) {
		return new KeyDumpWriterBuilder<>(client, ByteArrayCodec.INSTANCE);
	}

	public abstract static class AbstractBuilder<K, V, T, B extends AbstractBuilder<K, V, T, B>> {

		private final AbstractRedisClient client;
		private final RedisCodec<K, V> codec;
		private WriterOptions options = WriterOptions.builder().build();

		protected AbstractBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			this.client = client;
			this.codec = codec;
		}

		@SuppressWarnings("unchecked")
		public B options(WriterOptions options) {
			this.options = options;
			return (B) this;
		}

		public RedisItemWriter<K, V, T> build() {
			return new RedisItemWriter<>(client, codec, getOperation(), options);
		}

		protected abstract BatchOperation<K, V, T, Object> getOperation();

	}

	public static class OperationWriterBuilder<K, V, T>
			extends AbstractBuilder<K, V, T, OperationWriterBuilder<K, V, T>> {

		private final Operation<K, V, T, Object> operation;

		public OperationWriterBuilder(AbstractRedisClient client, RedisCodec<K, V> codec,
				Operation<K, V, T, Object> operation) {
			super(client, codec);
			this.operation = operation;
		}

		@Override
		protected BatchOperation<K, V, T, Object> getOperation() {
			return new SimpleBatchOperation<>(operation);
		}

	}

	public static class KeyDumpWriterBuilder<K, V>
			extends AbstractBuilder<K, V, KeyDump<K>, KeyDumpWriterBuilder<K, V>> {

		public KeyDumpWriterBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			super(client, codec);
		}

		@Override
		protected BatchOperation<K, V, KeyDump<K>, Object> getOperation() {
			return new SimpleBatchOperation<>(new RestoreReplace<>(KeyDump::getKey, KeyDump::getDump, KeyDump::getTtl));
		}

	}

	public static class DataStructureWriterBuilder<K, V>
			extends AbstractBuilder<K, V, DataStructure<K>, DataStructureWriterBuilder<K, V>> {

		private DataStructureWriteOptions options = DataStructureWriteOptions.builder().build();

		public DataStructureWriterBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			super(client, codec);
		}

		public DataStructureWriterBuilder<K, V> dataStructureOptions(DataStructureWriteOptions options) {
			this.options = options;
			return this;
		}

		@Override
		protected DataStructureWriteOperation<K, V> getOperation() {
			return new DataStructureWriteOperation<>(options);
		}

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <K, V, T> OperationWriterBuilder<K, V, T> operation(AbstractRedisClient client,
			RedisCodec<K, V> codec, Operation<K, K, T, ?> operation) {
		return new OperationWriterBuilder<>(client, codec, (Operation) operation);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T> OperationWriterBuilder<String, String, T> operation(AbstractRedisClient client,
			Operation<String, String, T, ?> operation) {
		return new OperationWriterBuilder<>(client, StringCodec.UTF8, (Operation) operation);
	}

}
