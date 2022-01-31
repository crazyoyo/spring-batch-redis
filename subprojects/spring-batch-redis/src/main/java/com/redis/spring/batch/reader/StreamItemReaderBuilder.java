package com.redis.spring.batch.reader;

import java.time.Duration;

import org.springframework.util.ClassUtils;

import com.redis.spring.batch.reader.StreamItemReader.AckPolicy;
import com.redis.spring.batch.support.RedisConnectionBuilder;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.codec.RedisCodec;

public class StreamItemReaderBuilder<K, V> extends RedisConnectionBuilder<K, V, StreamItemReaderBuilder<K, V>> {

	public static final String DEFAULT_CONSUMER_GROUP = ClassUtils.getShortName(StreamItemReader.class);
	public static final String DEFAULT_CONSUMER = "consumer1";

	private final K stream;
	private String offset = "0-0";
	private Duration block = StreamItemReader.DEFAULT_BLOCK;
	private long count = StreamItemReader.DEFAULT_COUNT;
	private K consumerGroup;
	private K consumer;
	private AckPolicy ackPolicy = StreamItemReader.DEFAULT_ACK_POLICY;

	public StreamItemReaderBuilder(AbstractRedisClient client, RedisCodec<K, V> codec, K stream) {
		super(client, codec);
		this.stream = stream;
	}

	public StreamItemReaderBuilder<K, V> offset(String offset) {
		this.offset = offset;
		return this;
	}

	public StreamItemReaderBuilder<K, V> block(Duration block) {
		this.block = block;
		return this;
	}

	public StreamItemReaderBuilder<K, V> count(long count) {
		this.count = count;
		return this;
	}

	public StreamItemReaderBuilder<K, V> consumerGroup(K consumerGroup) {
		this.consumerGroup = consumerGroup;
		return this;
	}

	public StreamItemReaderBuilder<K, V> consumer(K consumer) {
		this.consumer = consumer;
		return this;
	}

	public StreamItemReaderBuilder<K, V> ackPolicy(AckPolicy ackPolicy) {
		this.ackPolicy = ackPolicy;
		return this;
	}

	public StreamItemReader<K, V> build() {
		StreamItemReader<K, V> reader = new StreamItemReader<>(connectionSupplier(), poolConfig, sync(),
				StreamOffset.from(stream, offset));
		reader.setAckPolicy(ackPolicy);
		reader.setBlock(block);
		reader.setConsumer(consumer == null ? encodeKey(DEFAULT_CONSUMER) : consumer);
		reader.setConsumerGroup(consumerGroup == null ? encodeKey(DEFAULT_CONSUMER_GROUP) : consumerGroup);
		reader.setCount(count);
		return reader;
	}

}