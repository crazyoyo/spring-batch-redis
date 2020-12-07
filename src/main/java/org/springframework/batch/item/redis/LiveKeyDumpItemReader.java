package org.springframework.batch.item.redis;

import org.springframework.batch.item.redis.support.AbstractKeyValueItemReader;
import org.springframework.batch.item.redis.support.DumpReader;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.LiveKeyItemReader;
import org.springframework.batch.item.redis.support.LiveReaderOptions;
import org.springframework.batch.item.redis.support.RedisClientBuilder;

import io.lettuce.core.AbstractRedisClient;
import lombok.Setter;
import lombok.experimental.Accessors;

public class LiveKeyDumpItemReader extends AbstractKeyValueItemReader<KeyValue<byte[]>> {

	public LiveKeyDumpItemReader(AbstractRedisClient client, LiveReaderOptions options) {
		super(new LiveKeyItemReader(client, options.getLiveKeyReaderOptions()),
				new DumpReader(client, options.getPoolConfig()), options.getTransferOptions(),
				options.getQueueOptions());
	}

	public static LiveKeyDumpItemReaderBuilder builder() {
		return new LiveKeyDumpItemReaderBuilder();
	}

	@Setter
	@Accessors(fluent = true)
	public static class LiveKeyDumpItemReaderBuilder extends RedisClientBuilder<LiveKeyDumpItemReaderBuilder> {

		private LiveReaderOptions options = LiveReaderOptions.builder().build();

		public LiveKeyDumpItemReader build() {
			return new LiveKeyDumpItemReader(client, options);
		}

	}

}
