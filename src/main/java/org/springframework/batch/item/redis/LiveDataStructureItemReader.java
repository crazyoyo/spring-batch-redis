package org.springframework.batch.item.redis;

import org.springframework.batch.item.redis.support.AbstractKeyValueItemReader;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.DataStructureReader;
import org.springframework.batch.item.redis.support.LiveKeyItemReader;
import org.springframework.batch.item.redis.support.LiveReaderOptions;
import org.springframework.batch.item.redis.support.RedisClientBuilder;

import io.lettuce.core.AbstractRedisClient;
import lombok.Setter;
import lombok.experimental.Accessors;

public class LiveDataStructureItemReader extends AbstractKeyValueItemReader<DataStructure> {

	public LiveDataStructureItemReader(AbstractRedisClient client, LiveReaderOptions options) {
		super(new LiveKeyItemReader(client, options.getLiveKeyReaderOptions()),
				new DataStructureReader(client, options.getPoolConfig()), options.getTransferOptions(),
				options.getQueueOptions());
	}

	public static LiveDataStructureItemReaderBuilder builder() {
		return new LiveDataStructureItemReaderBuilder();
	}

	@Setter
	@Accessors(fluent = true)
	public static class LiveDataStructureItemReaderBuilder
			extends RedisClientBuilder<LiveDataStructureItemReaderBuilder> {

		private LiveReaderOptions options = LiveReaderOptions.builder().build();

		public LiveDataStructureItemReader build() {
			return new LiveDataStructureItemReader(client, options);
		}

	}

}
