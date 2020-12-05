package org.springframework.batch.item.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.support.AbstractKeyValueItemReader;
import org.springframework.batch.item.redis.support.ClientBuilder;
import org.springframework.batch.item.redis.support.DumpReader;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.LiveKeyItemReader;
import org.springframework.batch.item.redis.support.LiveReaderOptions;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import lombok.Setter;
import lombok.experimental.Accessors;

public class LiveKeyDumpItemReader extends AbstractKeyValueItemReader<KeyValue<byte[]>> {

	public LiveKeyDumpItemReader(AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig, LiveReaderOptions options) {
		super(new LiveKeyItemReader(client, options.getLiveKeyReaderOptions()), new DumpReader(client, poolConfig),
				options.getTransferOptions(), options.getQueueOptions());
	}

	public static LiveKeyDumpItemReaderBuilder builder() {
		return new LiveKeyDumpItemReaderBuilder();
	}

	@Setter
	@Accessors(fluent = true)
	public static class LiveKeyDumpItemReaderBuilder extends ClientBuilder<LiveKeyDumpItemReaderBuilder> {

		private LiveReaderOptions options = LiveReaderOptions.builder().build();

		public LiveKeyDumpItemReader build() {
			return new LiveKeyDumpItemReader(client, poolConfig, options);
		}

	}

}
