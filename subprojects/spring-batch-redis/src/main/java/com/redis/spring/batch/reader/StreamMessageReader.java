package com.redis.spring.batch.reader;

import java.util.List;

import io.lettuce.core.StreamMessage;

public interface StreamMessageReader<K, V> {

	/**
	 * Reads messages from a stream
	 * 
	 * @param blockMillis Duration in millis for xread block
	 * @return list of messages retrieved from the stream or empty list if no
	 *         messages available
	 */
	List<StreamMessage<K, V>> read(long blockMillis);

}