package com.redis.spring.batch.reader;

import java.time.Duration;

import org.springframework.util.ClassUtils;

import com.redis.spring.batch.reader.StreamItemReader.AckPolicy;
import com.redis.spring.batch.support.RedisConnectionBuilder;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.codec.StringCodec;

public class StreamItemReaderBuilder extends RedisConnectionBuilder<String, String, StreamItemReaderBuilder> {

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

	public StreamItemReaderBuilder offset(String offset) {
		this.offset = offset;
		return this;
	}

	public StreamItemReaderBuilder block(Duration block) {
		this.block = block;
		return this;
	}

	public StreamItemReaderBuilder count(long count) {
		this.count = count;
		return this;
	}

	public StreamItemReaderBuilder consumerGroup(String consumerGroup) {
		this.consumerGroup = consumerGroup;
		return this;
	}

	public StreamItemReaderBuilder consumer(String consumer) {
		this.consumer = consumer;
		return this;
	}

	public StreamItemReaderBuilder ackPolicy(AckPolicy ackPolicy) {
		this.ackPolicy = ackPolicy;
		return this;
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