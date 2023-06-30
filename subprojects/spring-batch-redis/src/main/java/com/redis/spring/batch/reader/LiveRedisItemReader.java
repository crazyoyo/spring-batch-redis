package com.redis.spring.batch.reader;

import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.spring.batch.RedisItemReader.BaseBuilder;
import com.redis.spring.batch.common.BatchOperation;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.common.SimpleBatchOperation;
import com.redis.spring.batch.step.FlushingStepBuilder;
import com.redis.spring.batch.step.FlushingStepOptions;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class LiveRedisItemReader<K, V, T> extends AbstractRedisItemReader<K, V, T> {

	private final KeyspaceNotificationItemReader<K, V> keyReader;
	private KeyspaceNotificationOptions keyspaceNotificationOptions = KeyspaceNotificationOptions.builder().build();
	private FlushingStepOptions flushingOptions = FlushingStepOptions.builder().build();

	public LiveRedisItemReader(AbstractRedisClient client, RedisCodec<K, V> codec,
			BatchOperation<K, V, K, T> operation) {
		super(client, codec, operation);
		this.keyReader = new KeyspaceNotificationItemReader<>(client, codec);
	}

	public KeyspaceNotificationOptions getKeyspaceNotificationOptions() {
		return keyspaceNotificationOptions;
	}

	public void setKeyspaceNotificationOptions(KeyspaceNotificationOptions keyspaceNotificationOptions) {
		this.keyspaceNotificationOptions = keyspaceNotificationOptions;
	}

	public FlushingStepOptions getFlushingOptions() {
		return flushingOptions;
	}

	public void setFlushingOptions(FlushingStepOptions options) {
		this.flushingOptions = options;
	}

	public KeyspaceNotificationItemReader<K, V> getKeyReader() {
		return keyReader;
	}

	@Override
	protected ItemReader<K> keyReader() {
		keyReader.setOptions(keyspaceNotificationOptions);
		return keyReader;
	}

	@Override
	protected SimpleStepBuilder<K, K> step(StepBuilder stepBuilder) {
		SimpleStepBuilder<K, K> step = super.step(stepBuilder);
		return new FlushingStepBuilder<>(step).options(flushingOptions);
	}

	public static Builder client(RedisModulesClient client) {
		return new Builder(client);
	}

	public static Builder client(RedisModulesClusterClient client) {
		return new Builder(client);
	}

	public static class Builder extends BaseBuilder<Builder> {

		private KeyspaceNotificationOptions keyspaceNotificationOptions = KeyspaceNotificationOptions.builder().build();
		private FlushingStepOptions flushingOptions = FlushingStepOptions.builder().build();

		public Builder(AbstractRedisClient client) {
			super(client);
		}

		public Builder keyspaceNotificationOptions(KeyspaceNotificationOptions options) {
			this.keyspaceNotificationOptions = options;
			return this;
		}

		public Builder flushingOptions(FlushingStepOptions options) {
			this.flushingOptions = options;
			return this;
		}

		public LiveRedisItemReader<byte[], byte[], KeyDump<byte[]>> keyDump() {
			return reader(ByteArrayCodec.INSTANCE, new KeyDumpReadOperation(client));
		}

		public LiveRedisItemReader<String, String, DataStructure<String>> dataStructure() {
			return dataStructure(StringCodec.UTF8);
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		public <K, V> LiveRedisItemReader<K, V, DataStructure<K>> dataStructure(RedisCodec<K, V> codec) {
			if (codec instanceof StringCodec) {
				return reader(StringCodec.UTF8, (Operation) new StringDataStructureReadOperation(client));
			}
			return reader(codec, new DataStructureReadOperation<>(client, codec));
		}

		private <K, V, T> LiveRedisItemReader<K, V, T> reader(RedisCodec<K, V> codec, Operation<K, V, K, T> operation) {
			LiveRedisItemReader<K, V, T> reader = new LiveRedisItemReader<>(client, codec,
					SimpleBatchOperation.of(operation));
			reader.setJobRepository(jobRepository);
			reader.setOptions(options);
			reader.setFlushingOptions(flushingOptions);
			reader.setKeyspaceNotificationOptions(keyspaceNotificationOptions);
			return reader;
		}

	}

}
