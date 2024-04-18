package com.redis.spring.batch.reader;

import java.time.Duration;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.operation.KeyValueRead;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.codec.StringCodec;

public class KeyComparisonItemReader extends RedisItemReader<String, String, KeyComparison> {

	public enum StreamMessageIdPolicy {
		COMPARE, IGNORE
	}

	public static final Duration DEFAULT_TTL_TOLERANCE = Duration.ofMillis(100);
	public static final StreamMessageIdPolicy DEFAULT_STREAM_MESSAGE_ID_POLICY = StreamMessageIdPolicy.COMPARE;
	public static final int DEFAULT_TARGET_POOL_SIZE = RedisItemReader.DEFAULT_POOL_SIZE;

	private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;
	private AbstractRedisClient targetClient;
	private int targetPoolSize = DEFAULT_TARGET_POOL_SIZE;
	private ReadFrom targetReadFrom;
	private StreamMessageIdPolicy streamMessageIdPolicy = DEFAULT_STREAM_MESSAGE_ID_POLICY;

	public KeyComparisonItemReader(KeyValueRead<String, String, Object> source,
			KeyValueRead<String, String, Object> target) {
		super(StringCodec.UTF8, new KeyComparisonRead(source, target));
	}

	public void setTargetClient(AbstractRedisClient client) {
		this.targetClient = client;
	}

	public void setTargetPoolSize(int size) {
		this.targetPoolSize = size;
	}

	public void setTargetReadFrom(ReadFrom readFrom) {
		this.targetReadFrom = readFrom;
	}

	public void setTtlTolerance(Duration ttlTolerance) {
		this.ttlTolerance = ttlTolerance;
	}

	public void setStreamMessageIdPolicy(StreamMessageIdPolicy policy) {
		this.streamMessageIdPolicy = policy;
	}

	@Override
	protected synchronized void doOpen() throws Exception {
		KeyComparisonRead keyComparisonRead = (KeyComparisonRead) operation;
		keyComparisonRead.setStreamMessageIdPolicy(streamMessageIdPolicy);
		keyComparisonRead.setTargetClient(targetClient);
		keyComparisonRead.setTargetPoolSize(targetPoolSize);
		keyComparisonRead.setTargetReadFrom(targetReadFrom);
		keyComparisonRead.setTtlTolerance(ttlTolerance);
		super.doOpen();
	}

}
