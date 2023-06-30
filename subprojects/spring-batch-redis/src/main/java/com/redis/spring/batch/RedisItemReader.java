package com.redis.spring.batch;

import java.util.function.Supplier;

import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemReader;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.spring.batch.common.BatchOperation;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.common.SimpleBatchOperation;
import com.redis.spring.batch.common.Utils;
import com.redis.spring.batch.reader.AbstractRedisItemReader;
import com.redis.spring.batch.reader.DataStructureReadOperation;
import com.redis.spring.batch.reader.KeyDumpReadOperation;
import com.redis.spring.batch.reader.KeyspaceNotificationOptions;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.ReaderOptions;
import com.redis.spring.batch.reader.ScanKeyItemReader;
import com.redis.spring.batch.reader.ScanOptions;
import com.redis.spring.batch.reader.StringDataStructureReadOperation;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisItemReader<K, V, T> extends AbstractRedisItemReader<K, V, T> {

	private ScanOptions scanOptions = ScanOptions.builder().build();

	public RedisItemReader(AbstractRedisClient client, RedisCodec<K, V> codec, BatchOperation<K, V, K, T> operation) {
		super(client, codec, operation);
	}

	public ScanOptions getScanOptions() {
		return scanOptions;
	}

	public void setScanOptions(ScanOptions scanOptions) {
		this.scanOptions = scanOptions;
	}

	@Override
	protected ItemReader<K> keyReader() {
		Supplier<StatefulConnection<K, V>> supplier = Utils.connectionSupplier(client, codec, options.getReadFrom());
		ScanKeyItemReader<K, V> keyReader = new ScanKeyItemReader<>(supplier);
		keyReader.setOptions(scanOptions);
		return keyReader;
	}

	public static ScanBuilder client(RedisModulesClient client) {
		return new ScanBuilder(client);
	}

	public static ScanBuilder client(RedisModulesClusterClient client) {
		return new ScanBuilder(client);
	}

	public static class BaseBuilder<B extends BaseBuilder<B>> {

		protected final AbstractRedisClient client;
		protected JobRepository jobRepository;
		protected ReaderOptions options = ReaderOptions.builder().build();

		protected BaseBuilder(AbstractRedisClient client) {
			this.client = client;
		}

		@SuppressWarnings("unchecked")
		public B jobRepository(JobRepository jobRepository) {
			this.jobRepository = jobRepository;
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B options(ReaderOptions options) {
			this.options = options;
			return (B) this;
		}

	}

	public static class BaseScanBuilder<B extends BaseScanBuilder<B>> extends BaseBuilder<B> {

		protected ScanOptions scanOptions = ScanOptions.builder().build();

		protected BaseScanBuilder(AbstractRedisClient client) {
			super(client);
		}

		@SuppressWarnings("unchecked")
		public B scanOptions(ScanOptions options) {
			this.scanOptions = options;
			return (B) this;
		}

		protected <K, V, T> RedisItemReader<K, V, T> reader(RedisCodec<K, V> codec, Operation<K, V, K, T> operation) {
			return reader(codec, SimpleBatchOperation.of(operation));
		}

		protected <K, V, T> RedisItemReader<K, V, T> reader(RedisCodec<K, V> codec,
				BatchOperation<K, V, K, T> operation) {
			RedisItemReader<K, V, T> reader = new RedisItemReader<>(client, codec, operation);
			reader.setJobRepository(jobRepository);
			reader.setOptions(options);
			reader.setScanOptions(scanOptions);
			return reader;

		}

	}

	public static class ScanBuilder extends BaseScanBuilder<ScanBuilder> {

		public ScanBuilder(AbstractRedisClient client) {
			super(client);
		}

		public LiveRedisItemReader.Builder live() {
			LiveRedisItemReader.Builder builder = new LiveRedisItemReader.Builder(client);
			builder.jobRepository(jobRepository);
			builder.options(options);
			builder.keyspaceNotificationOptions(KeyspaceNotificationOptions.builder().match(scanOptions.getMatch())
					.type(scanOptions.getType()).build());
			return builder;
		}

		public RedisItemReader<byte[], byte[], KeyDump<byte[]>> keyDump() {
			return reader(ByteArrayCodec.INSTANCE, new KeyDumpReadOperation(client));
		}

		public RedisItemReader<String, String, DataStructure<String>> dataStructure() {
			return dataStructure(StringCodec.UTF8);
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		public <K, V> RedisItemReader<K, V, DataStructure<K>> dataStructure(RedisCodec<K, V> codec) {
			if (codec instanceof StringCodec) {
				reader(StringCodec.UTF8, (Operation) new StringDataStructureReadOperation(client));
			}
			return reader(codec, new DataStructureReadOperation<>(client, codec));
		}

	}

}
