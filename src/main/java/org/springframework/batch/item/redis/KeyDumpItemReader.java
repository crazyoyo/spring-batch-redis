package org.springframework.batch.item.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.support.AbstractKeyValueItemReader;
import org.springframework.batch.item.redis.support.DumpReader;
import org.springframework.batch.item.redis.support.KeyItemReader;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.ReaderOptions;
import org.springframework.batch.item.redis.support.RedisConnectionPoolBuilder;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import lombok.Setter;
import lombok.experimental.Accessors;

public class KeyDumpItemReader extends AbstractKeyValueItemReader<KeyValue<byte[]>> {

	public KeyDumpItemReader(AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig, ReaderOptions options) {
		super(new KeyItemReader(client, options.getKeyReaderOptions()), new DumpReader(client, poolConfig),
				options.getTransferOptions(), options.getQueueOptions());
	}

	public static KeyDumpItemReaderBuilder builder(AbstractRedisClient client) {
		return new KeyDumpItemReaderBuilder(client);
	}

	@Setter
	@Accessors(fluent = true)
	public static class KeyDumpItemReaderBuilder extends RedisConnectionPoolBuilder<KeyDumpItemReaderBuilder> {

		public KeyDumpItemReaderBuilder(AbstractRedisClient client) {
			super(client);
		}

		private ReaderOptions options = ReaderOptions.builder().build();

		public KeyDumpItemReader build() {
			return new KeyDumpItemReader(client, poolConfig, options);
		}

	}

}
