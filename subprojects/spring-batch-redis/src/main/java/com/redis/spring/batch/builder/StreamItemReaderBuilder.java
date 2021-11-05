package com.redis.spring.batch.builder;

import java.time.Duration;

import org.springframework.util.ClassUtils;

import com.redis.spring.batch.support.StreamItemReader;
import com.redis.spring.batch.support.StreamItemReader.AckPolicy;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.codec.StringCodec;
import lombok.Setter;
import lombok.experimental.Accessors;

@Setter
@Accessors(fluent = true)
public class StreamItemReaderBuilder extends RedisBuilder<String, String, StreamItemReaderBuilder> {

	public static final String DEFAULT_CONSUMER_GROUP = ClassUtils.getShortName(StreamItemReader.class);
	public static final String DEFAULT_CONSUMER = "consumer1";

	private final String stream;
	private String offset = "0-0";
	private Duration block = StreamItemReader.DEFAULT_BLOCK;
	private long count = StreamItemReader.DEFAULT_COUNT;
	private String consumerGroup = DEFAULT_CONSUMER_GROUP;
	private String consumer = DEFAULT_CONSUMER;
	private AckPolicy ackPolicy = StreamItemReader.DEFAULT_ACK_POLICY;

	public StreamItemReaderBuilder(AbstractRedisClient client, String stream) {
		super(client, StringCodec.UTF8);
		this.stream = stream;
	}

	public StreamItemReader<String, String> build() {
		StreamItemReader<String, String> reader = new StreamItemReader<>(connectionSupplier(), poolConfig, sync(),
				StreamOffset.from(stream, offset));
		reader.setAckPolicy(ackPolicy);
		reader.setBlock(block);
		reader.setConsumer(consumer);
		reader.setConsumerGroup(consumerGroup);
		reader.setCount(count);
		return reader;
	}

}