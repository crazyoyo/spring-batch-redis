package com.redis.spring.batch;

import java.time.Duration;
import java.util.List;

import org.springframework.batch.item.ItemStreamWriter;

import com.redis.lettucemod.RedisModulesClient;
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
import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisItemWriter<K, V, T> extends OperationItemStreamSupport<K, V, T, Object>
		implements ItemStreamWriter<T> {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public RedisItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec, BatchOperation<K, V, T, ?> operation) {
		super(client, codec, (BatchOperation) operation);
	}

	@Override
	public void write(List<? extends T> items) throws Exception {
		process(items);
	}

	public static <K, V> WriterBuilder<K, V> client(RedisClient client, RedisCodec<K, V> codec) {
		return new WriterBuilder<>(client, codec);
	}

	public static <K, V> WriterBuilder<K, V> client(RedisModulesClient client, RedisCodec<K, V> codec) {
		return new WriterBuilder<>(client, codec);
	}

	public static WriterBuilder<String, String> client(RedisClient client) {
		return new WriterBuilder<>(client, StringCodec.UTF8);
	}

	public static WriterBuilder<String, String> client(RedisModulesClient client) {
		return new WriterBuilder<>(client, StringCodec.UTF8);
	}

	public static class WriterBuilder<K, V> {

		protected final AbstractRedisClient client;
		protected final RedisCodec<K, V> codec;

		private int waitReplicas = ReplicaWaitWriteOperation.DEFAULT_REPLICAS;
		private Duration waitTimeout = ReplicaWaitWriteOperation.DEFAULT_TIMEOUT;
		private boolean multiExec;
		protected MergePolicy mergePolicy = DataStructureWriteOperation.DEFAULT_MERGE_POLICY;
		protected StreamIdPolicy streamIdPolicy = DataStructureWriteOperation.DEFAULT_STREAM_ID_POLICY;

		public WriterBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			this.client = client;
			this.codec = codec;
		}

		public WriterBuilder<K, V> mergePolicy(MergePolicy policy) {
			this.mergePolicy = policy;
			return this;
		}

		public WriterBuilder<K, V> streamIdPolicy(StreamIdPolicy policy) {
			this.streamIdPolicy = policy;
			return this;
		}

		public WriterBuilder<K, V> multiExec(boolean multiExec) {
			this.multiExec = multiExec;
			return this;
		}

		public WriterBuilder<K, V> waitReplicas(int replicas) {
			this.waitReplicas = replicas;
			return this;
		}

		public WriterBuilder<K, V> waitTimeout(Duration timeout) {
			this.waitTimeout = timeout;
			return this;
		}

		protected <T> RedisItemWriter<K, V, T> build(Operation<K, V, T, ?> operation) {
			return build(new SimpleBatchOperation<>(operation));
		}

		protected <T> RedisItemWriter<K, V, T> build(BatchOperation<K, V, T, ?> operation) {
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

		public RedisItemWriter<K, V, DataStructure<K>> dataStructure() {
			DataStructureWriteOperation<K, V> operation = new DataStructureWriteOperation<>();
			operation.setMergePolicy(mergePolicy);
			operation.setStreamIdPolicy(streamIdPolicy);
			return build(operation);
		}

		public RedisItemWriter<K, V, KeyDump<K>> keyDump() {
			return build(new RestoreReplace<>(KeyDump::getKey, KeyDump::getDump, KeyDump::getTtl));
		}

		public <T> RedisItemWriter<K, V, T> operation(Operation<K, V, T, ?> operation) {
			return build(operation);
		}

	}

}
