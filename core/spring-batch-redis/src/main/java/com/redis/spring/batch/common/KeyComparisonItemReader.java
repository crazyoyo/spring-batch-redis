package com.redis.spring.batch.common;

import java.time.Duration;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.support.PassThroughItemProcessor;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.reader.KeyComparisonValueReader;
import com.redis.spring.batch.reader.KeyValueItemReader;
import com.redis.spring.batch.util.CodecUtils;

public class KeyComparisonItemReader extends RedisItemReader<String, String, KeyComparison> {

	private Duration ttlTolerance = KeyComparisonValueReader.DEFAULT_TTL_TOLERANCE;

	private final OperationValueReader<String, String, String, KeyValue<String>> source;

	private final ItemProcessor<String, String> sourceKeyProcessor;

	private final OperationValueReader<String, String, String, KeyValue<String>> target;

	private ItemProcessor<KeyValue<String>, KeyValue<String>> processor = new PassThroughItemProcessor<>();

	private boolean compareStreamMessageIds;

	public KeyComparisonItemReader(KeyValueItemReader<String, String> source,
			KeyValueItemReader<String, String> target) {
		super(source.getClient(), CodecUtils.STRING_CODEC);
		this.source = source.operationValueReader();
		this.sourceKeyProcessor = source.getKeyProcessor();
		this.target = target.operationValueReader();
	}

	public void setCompareStreamMessageIds(boolean enable) {
		this.compareStreamMessageIds = enable;
	}

	public void setProcessor(ItemProcessor<KeyValue<String>, KeyValue<String>> processor) {
		this.processor = processor;
	}

	public void setTtlTolerance(Duration ttlTolerance) {
		this.ttlTolerance = ttlTolerance;
	}

	@Override
	public KeyComparisonValueReader valueReader() {
		KeyComparisonValueReader valueReader = new KeyComparisonValueReader(source, target);
		valueReader.setCompareStreamMessageIds(compareStreamMessageIds);
		valueReader.setKeyProcessor(sourceKeyProcessor);
		valueReader.setProcessor(processor);
		valueReader.setTtlTolerance(ttlTolerance);
		return valueReader;
	}

}
