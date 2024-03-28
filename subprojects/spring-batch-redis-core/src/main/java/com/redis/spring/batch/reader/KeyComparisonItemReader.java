package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.batch.item.Chunk;
import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.KeyValue.Type;
import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.spring.batch.util.CodecUtils;

import io.lettuce.core.StreamMessage;

public class KeyComparisonItemReader extends RedisItemReader<String, String, KeyComparison> {

	public static final Duration DEFAULT_TTL_TOLERANCE = Duration.ofMillis(100);

	private final AbstractKeyValueItemReader<String, String> source;
	private final AbstractKeyValueItemReader<String, String> target;

	private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;

	private ValueReader<String, String, String, KeyValue<String>> sourceValueReader;
	private ValueReader<String, String, String, KeyValue<String>> targetValueReader;
	private Function<KeyValue<String>, KeyValue<String>> processor = Function.identity();
	private boolean compareStreamMessageIds;

	public KeyComparisonItemReader(AbstractKeyValueItemReader<String, String> source,
			AbstractKeyValueItemReader<String, String> target) {
		super(source.getClient(), CodecUtils.STRING_CODEC);
		this.source = source;
		this.target = target;
	}

	public void setCompareStreamMessageIds(boolean enable) {
		this.compareStreamMessageIds = enable;
	}

	public void setProcessor(Function<KeyValue<String>, KeyValue<String>> processor) {
		this.processor = processor;
	}

	public void setTtlTolerance(Duration ttlTolerance) {
		this.ttlTolerance = ttlTolerance;
	}

	@Override
	protected synchronized void doOpen() throws Exception {
		if (sourceValueReader == null) {
			sourceValueReader = source.valueReader();
			sourceValueReader.open();
		}
		if (targetValueReader == null) {
			targetValueReader = target.valueReader();
			targetValueReader.open();
		}
		super.doOpen();
	}

	@Override
	protected synchronized void doClose() throws Exception {
		super.doClose();
		if (sourceValueReader != null) {
			sourceValueReader.close();
			sourceValueReader = null;
		}
		if (targetValueReader != null) {
			targetValueReader.close();
			targetValueReader = null;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Chunk<KeyComparison> values(Chunk<? extends String> keys) {
		Chunk<KeyComparison> comparisons = new Chunk<>();
		Chunk<String> processedKeys = processKeys((Chunk<String>) keys);
		Chunk<KeyValue<String>> sourceItems = sourceValueReader.execute(processedKeys);
		List<KeyValue<String>> items = processValues(sourceItems).getItems();
		List<String> targetKeys = items.stream().map(KeyValue::getKey).collect(Collectors.toList());
		List<KeyValue<String>> targetItems = targetValueReader.execute(new Chunk<>(targetKeys)).getItems();
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

	private Chunk<KeyValue<String>> processValues(Chunk<KeyValue<String>> values) {
		if (processor == null) {
			return values;
		}
		Chunk<KeyValue<String>> processedValues = new Chunk<>();
		for (KeyValue<String> value : values) {
			KeyValue<String> processedValue = processor.apply(value);
			if (processedValue != null) {
				processedValues.add(processedValue);
			}
		}
		return processedValues;
	}

	private Chunk<String> processKeys(Chunk<String> keys) {
		if (keyProcessor == null) {
			return keys;
		}
		Chunk<String> processedKeys = new Chunk<>();
		for (String key : keys) {
			try {
				String processedKey = keyProcessor.process(key);
				if (processedKey != null) {
					processedKeys.add(processedKey);
				}
			} catch (Exception e) {
				// ignore
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
		if (source.getType() == Type.STREAM) {
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
