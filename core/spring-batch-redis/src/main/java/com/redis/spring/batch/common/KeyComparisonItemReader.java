package com.redis.spring.batch.common;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.KeyComparison.Status;
import com.redis.spring.batch.reader.KeyValueItemReader;
import com.redis.spring.batch.util.CodecUtils;

import io.lettuce.core.StreamMessage;

public class KeyComparisonItemReader extends RedisItemReader<String, String, KeyComparison> {

	public static final Duration DEFAULT_TTL_TOLERANCE = Duration.ofMillis(100);

	private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;

	private final OperationValueReader<String, String, String, KeyValue<String>> source;

	private final OperationValueReader<String, String, String, KeyValue<String>> target;

	private ItemProcessor<KeyValue<String>, KeyValue<String>> processor = new PassThroughItemProcessor<>();

	private boolean compareStreamMessageIds;

	public KeyComparisonItemReader(KeyValueItemReader<String, String> source,
			KeyValueItemReader<String, String> target) {
		super(source.getClient(), CodecUtils.STRING_CODEC);
		this.source = source.operationValueReader();
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
	public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
		source.open(executionContext);
		target.open(executionContext);
		super.open(executionContext);
	}

	@Override
	public void update(ExecutionContext executionContext) throws ItemStreamException {
		source.update(executionContext);
		target.update(executionContext);
		super.update(executionContext);
	}

	@Override
	public synchronized void close() throws ItemStreamException {
		super.close();
		source.close();
		target.close();
	}

	@Override
	public List<KeyComparison> process(Iterable<String> keys) throws Exception {
		Iterable<String> processedKeys = processKeys(keys);
		List<KeyValue<String>> sourceItems = source.process(processedKeys);
		if (CollectionUtils.isEmpty(sourceItems)) {
			return Collections.emptyList();
		}
		List<KeyValue<String>> items = processValues(sourceItems);
		List<String> targetKeys = items.stream().map(KeyValue::getKey).collect(Collectors.toList());
		List<KeyValue<String>> targetItems = target.process(targetKeys);
		List<KeyComparison> comparisons = new ArrayList<>();
		if (CollectionUtils.isEmpty(items)) {
			throw new IllegalStateException("No source items found");
		}
		if (CollectionUtils.isEmpty(targetItems)) {
			throw new IllegalStateException("No target items found");
		}
		for (int index = 0; index < items.size(); index++) {
			KeyComparison comparison = new KeyComparison();
			comparison.setSource(items.get(index));
			if (index < targetItems.size()) {
				comparison.setTarget(targetItems.get(index));
			}
			comparison.setStatus(status(comparison));
			comparisons.add(comparison);
		}
		return comparisons;
	}

	private List<KeyValue<String>> processValues(List<KeyValue<String>> values) throws Exception {
		if (processor == null) {
			return values;
		}
		List<KeyValue<String>> processedValues = new ArrayList<>(values.size());
		for (KeyValue<String> value : values) {
			KeyValue<String> processedValue = processor.process(value);
			if (processedValue != null) {
				processedValues.add(processedValue);
			}
		}
		return processedValues;
	}

	private Iterable<String> processKeys(Iterable<String> keys) throws Exception {
		if (keyProcessor == null) {
			return keys;
		}
		List<String> processedKeys = new ArrayList<>();
		for (String key : keys) {
			String processedKey = keyProcessor.process(key);
			if (processedKey != null) {
				processedKeys.add(processedKey);
			}
		}
		return processedKeys;
	}

	private Status status(KeyComparison comparison) {
		KeyValue<String> sourceEntry = comparison.getSource();
		KeyValue<String> targetEntry = comparison.getTarget();
		if (targetEntry == null) {
			if (sourceEntry == null) {
				return Status.OK;
			}
			return Status.MISSING;
		}
		if (!targetEntry.exists() && sourceEntry.exists()) {
			return Status.MISSING;
		}
		if (targetEntry.getType() != sourceEntry.getType()) {
			return Status.TYPE;
		}
		if (!valueEquals(sourceEntry, targetEntry)) {
			return Status.VALUE;
		}
		if (sourceEntry.getTtl() != targetEntry.getTtl()) {
			long delta = Math.abs(sourceEntry.getTtl() - targetEntry.getTtl());
			if (delta > ttlTolerance.toMillis()) {
				return Status.TTL;
			}
		}
		return Status.OK;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private boolean valueEquals(KeyValue<String> source, KeyValue<String> target) {
		if (source.getType() == DataType.STREAM) {
			return streamEquals((Collection<StreamMessage>) source.getValue(),
					(Collection<StreamMessage>) target.getValue());
		}
		return Objects.deepEquals(source.getValue(), target.getValue());
	}

	@SuppressWarnings("rawtypes")
	private boolean streamEquals(Collection<StreamMessage> source, Collection<StreamMessage> target) {
		if (CollectionUtils.isEmpty(source)) {
			return CollectionUtils.isEmpty(target);
		}
		if (source.size() != target.size()) {
			return false;
		}
		Iterator<StreamMessage> sourceIterator = source.iterator();
		Iterator<StreamMessage> targetIterator = target.iterator();
		while (sourceIterator.hasNext()) {
			if (!targetIterator.hasNext()) {
				return false;
			}
			StreamMessage sourceMessage = sourceIterator.next();
			StreamMessage targetMessage = targetIterator.next();
			if (!streamMessageEquals(sourceMessage, targetMessage)) {
				return false;
			}
		}
		return true;
	}

	@SuppressWarnings("rawtypes")
	private boolean streamMessageEquals(StreamMessage sourceMessage, StreamMessage targetMessage) {
		if (!Objects.equals(sourceMessage.getStream(), targetMessage.getStream())) {
			return false;
		}
		if (compareStreamMessageIds && !Objects.equals(sourceMessage.getId(), targetMessage.getId())) {
			return false;
		}
		Map sourceBody = sourceMessage.getBody();
		Map targetBody = targetMessage.getBody();
		if (CollectionUtils.isEmpty(sourceBody)) {
			return CollectionUtils.isEmpty(targetBody);
		}
		return sourceBody.equals(targetBody);
	}

}
