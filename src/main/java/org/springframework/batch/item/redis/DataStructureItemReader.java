package org.springframework.batch.item.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.support.AbstractKeyValueItemReader;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.DataStructureReader;
import org.springframework.batch.item.redis.support.KeyItemReader;
import org.springframework.batch.item.redis.support.ReaderOptions;
import org.springframework.batch.item.redis.support.RedisConnectionPoolBuilder;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import lombok.Setter;
import lombok.experimental.Accessors;

public class DataStructureItemReader extends AbstractKeyValueItemReader<DataStructure> {

	public DataStructureItemReader(AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig, ReaderOptions options) {
		super(new KeyItemReader(client, options.getKeyReaderOptions()), new DataStructureReader(client, poolConfig),
				options.getTransferOptions(), options.getQueueOptions());
	}

	public static DataStructureItemReaderBuilder builder(AbstractRedisClient client) {
		return new DataStructureItemReaderBuilder(client);
	}

	@Setter
	@Accessors(fluent = true)
	public static class DataStructureItemReaderBuilder
			extends RedisConnectionPoolBuilder<DataStructureItemReaderBuilder> {

		public DataStructureItemReaderBuilder(AbstractRedisClient client) {
			super(client);
		}

		private ReaderOptions options = ReaderOptions.builder().build();

		public DataStructureItemReader build() {
			return new DataStructureItemReader(client, poolConfig, options);
		}

	}

}
