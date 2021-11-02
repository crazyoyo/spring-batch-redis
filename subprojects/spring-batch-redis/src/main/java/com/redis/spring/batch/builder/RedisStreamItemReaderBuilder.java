package com.redis.spring.batch.builder;

import java.time.Duration;

import org.springframework.util.ClassUtils;

import com.redis.spring.batch.support.CommandBuilder;
import com.redis.spring.batch.support.RedisStreamItemReader;
import com.redis.spring.batch.support.RedisStreamItemReader.AckPolicy;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.codec.StringCodec;
import lombok.Setter;
import lombok.experimental.Accessors;

@Setter
@Accessors(fluent = true)
public class RedisStreamItemReaderBuilder extends CommandBuilder<String, String, RedisStreamItemReaderBuilder> {

	public static final String DEFAULT_CONSUMER_GROUP = ClassUtils.getShortName(RedisStreamItemReader.class);
	public static final String DEFAULT_CONSUMER = "consumer1";

	private final StreamOffset<String> offset;
	private Duration block = RedisStreamItemReader.DEFAULT_BLOCK;
	private long count = RedisStreamItemReader.DEFAULT_COUNT;
	private String consumerGroup = DEFAULT_CONSUMER_GROUP;
	private String consumer = DEFAULT_CONSUMER;
	private AckPolicy ackPolicy = RedisStreamItemReader.DEFAULT_ACK_POLICY;

	public RedisStreamItemReaderBuilder(AbstractRedisClient client, StreamOffset<String> offset) {
		super(client, StringCodec.UTF8);
		this.offset = offset;
	}

	public RedisStreamItemReader<String, String> build() {
		RedisStreamItemReader<String, String> reader = new RedisStreamItemReader<>(connectionSupplier(), poolConfig,
				sync(), offset);
		reader.setAckPolicy(ackPolicy);
		reader.setBlock(block);
		reader.setConsumer(consumer);
		reader.setConsumerGroup(consumerGroup);
		reader.setCount(count);
		return reader;
	}

	public static class OffsetStreamItemReaderBuilder {

		private final StreamOffset<String> offset;

		public OffsetStreamItemReaderBuilder(StreamOffset<String> offset) {
			this.offset = offset;
		}

		public RedisStreamItemReaderBuilder client(AbstractRedisClient client) {
			return new RedisStreamItemReaderBuilder(client, offset);
		}
	}
}