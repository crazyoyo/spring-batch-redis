package com.redis.spring.batch.support;

import java.time.Duration;

import org.springframework.util.ClassUtils;

import com.redis.spring.batch.support.RedisStreamItemReader.AckPolicy;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.codec.StringCodec;
import lombok.Setter;
import lombok.experimental.Accessors;

@Setter
@Accessors(fluent = true)
public class StreamItemReaderBuilder extends CommandBuilder<String, String, StreamItemReaderBuilder> {

	public static final Duration DEFAULT_BLOCK = Duration.ofMillis(100);
	public static final long DEFAULT_COUNT = 50;
	public static final String DEFAULT_CONSUMER_GROUP = ClassUtils.getShortName(RedisStreamItemReader.class);
	public static final String DEFAULT_CONSUMER = "consumer1";
	public static final AckPolicy DEFAULT_ACK_POLICY = AckPolicy.AUTO;

	private final StreamOffset<String> offset;
	private Duration block = DEFAULT_BLOCK;
	private Long count = DEFAULT_COUNT;
	private String consumerGroup = DEFAULT_CONSUMER_GROUP;
	private String consumer = DEFAULT_CONSUMER;
	private AckPolicy ackPolicy = DEFAULT_ACK_POLICY;

	public StreamItemReaderBuilder(AbstractRedisClient client, StreamOffset<String> offset) {
		super(client, StringCodec.UTF8);
		this.offset = offset;
	}

	public RedisStreamItemReader build() {
		return new RedisStreamItemReader(connectionSupplier(), poolConfig, sync(), count, block, consumerGroup,
				consumer, offset, ackPolicy);
	}

	public static class OffsetStreamItemReaderBuilder {

		private final StreamOffset<String> offset;

		public OffsetStreamItemReaderBuilder(StreamOffset<String> offset) {
			this.offset = offset;
		}

		public StreamItemReaderBuilder client(AbstractRedisClient client) {
			return new StreamItemReaderBuilder(client, offset);
		}
	}
}