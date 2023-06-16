package com.redis.spring.batch;

import java.time.Duration;
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
import com.redis.spring.batch.writer.MergePolicy;
import com.redis.spring.batch.writer.MultiExecWriteOperation;
import com.redis.spring.batch.writer.ReplicaWaitWriteOperation;
import com.redis.spring.batch.writer.StreamIdPolicy;
import com.redis.spring.batch.writer.operation.RestoreReplace;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisItemWriter<K, V, T> extends OperationItemStreamSupport<K, V, T, Object>
		implements ItemStreamWriter<T> {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public RedisItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec, BatchOperation<K, V, T, ?> operation) {
		super(client, codec, (BatchOperation) operation);
	}

	@Override
	public synchronized void write(List<? extends T> items) throws Exception {
		process(items);
	}

	public static WriterBuilder client(RedisModulesClient client) {
		return new WriterBuilder(client);
	}

	public static WriterBuilder client(RedisModulesClusterClient client) {
		return new WriterBuilder(client);
	}

	public static class WriterBuilder {

		protected final AbstractRedisClient client;

		private int waitReplicas = ReplicaWaitWriteOperation.DEFAULT_REPLICAS;
		private Duration waitTimeout = ReplicaWaitWriteOperation.DEFAULT_TIMEOUT;
		private boolean multiExec;
		protected MergePolicy mergePolicy = DataStructureWriteOperation.DEFAULT_MERGE_POLICY;
		protected StreamIdPolicy streamIdPolicy = DataStructureWriteOperation.DEFAULT_STREAM_ID_POLICY;

		public WriterBuilder(AbstractRedisClient client) {
			this.client = client;
		}

		public WriterBuilder mergePolicy(MergePolicy policy) {
			this.mergePolicy = policy;
			return this;
		}

		public WriterBuilder streamIdPolicy(StreamIdPolicy policy) {
			this.streamIdPolicy = policy;
			return this;
		}

		public WriterBuilder multiExec(boolean multiExec) {
			this.multiExec = multiExec;
			return this;
		}

		public WriterBuilder waitReplicas(int replicas) {
			this.waitReplicas = replicas;
			return this;
		}

		public WriterBuilder waitTimeout(Duration timeout) {
			this.waitTimeout = timeout;
			return this;
		}

		protected <K, V, T> RedisItemWriter<K, V, T> build(RedisCodec<K, V> codec, Operation<K, V, T, ?> operation) {
			return build(codec, new SimpleBatchOperation<>(operation));
		}

		protected <K, V, T> RedisItemWriter<K, V, T> build(RedisCodec<K, V> codec,
				BatchOperation<K, V, T, ?> operation) {
			if (waitReplicas > 0) {
				ReplicaWaitWriteOperation<K, V, T, ?> replicaOperation = new ReplicaWaitWriteOperation<>(operation);
				replicaOperation.setReplicas(waitReplicas);
				replicaOperation.setTimeout(waitTimeout);
				operation = replicaOperation;
			}
			if (multiExec) {
				operation = new MultiExecWriteOperation<>(operation);
			}
			return new RedisItemWriter<>(client, codec, operation);
		}

		public RedisItemWriter<String, String, DataStructure<String>> dataStructure() {
			return dataStructure(StringCodec.UTF8);
		}

		public <K, V> RedisItemWriter<K, V, DataStructure<K>> dataStructure(RedisCodec<K, V> codec) {
			DataStructureWriteOperation<K, V> operation = new DataStructureWriteOperation<>();
			operation.setMergePolicy(mergePolicy);
			operation.setStreamIdPolicy(streamIdPolicy);
			return build(codec, operation);
		}

		public RedisItemWriter<byte[], byte[], KeyDump<byte[]>> keyDump() {
			return build(ByteArrayCodec.INSTANCE, new RestoreReplace<byte[], byte[], KeyDump<byte[]>>(KeyDump::getKey,
					KeyDump::getDump, KeyDump::getTtl));
		}

		public <T> RedisItemWriter<String, String, T> operation(Operation<String, String, T, ?> operation) {
			return operation(StringCodec.UTF8, operation);
		}

		public <K, V, T> RedisItemWriter<K, V, T> operation(RedisCodec<K, V> codec, Operation<K, V, T, ?> operation) {
			return build(codec, operation);
		}

	}

}
