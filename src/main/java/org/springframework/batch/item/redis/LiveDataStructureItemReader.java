package org.springframework.batch.item.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.support.AbstractKeyValueItemReader;
import org.springframework.batch.item.redis.support.ClientBuilder;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.DataStructureReader;
import org.springframework.batch.item.redis.support.LiveKeyItemReader;
import org.springframework.batch.item.redis.support.LiveReaderOptions;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import lombok.Setter;
import lombok.experimental.Accessors;

public class LiveDataStructureItemReader extends AbstractKeyValueItemReader<DataStructure> {

	public LiveDataStructureItemReader(AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig, LiveReaderOptions options) {
		super(new LiveKeyItemReader(client, options.getLiveKeyReaderOptions()),
				new DataStructureReader(client, poolConfig), options.getTransferOptions(), options.getQueueOptions());
	}

	public static LiveDataStructureItemReaderBuilder builder() {
		return new LiveDataStructureItemReaderBuilder();
	}

	@Setter
	@Accessors(fluent = true)
	public static class LiveDataStructureItemReaderBuilder extends ClientBuilder<LiveDataStructureItemReaderBuilder> {

		private LiveReaderOptions options = LiveReaderOptions.builder().build();

		public LiveDataStructureItemReader build() {
			return new LiveDataStructureItemReader(client, poolConfig, options);
		}

	}

}
