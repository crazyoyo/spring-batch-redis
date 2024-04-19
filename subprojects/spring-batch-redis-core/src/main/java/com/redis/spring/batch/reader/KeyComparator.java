package com.redis.spring.batch.reader;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.KeyValue.Type;
import com.redis.spring.batch.reader.KeyComparatorOptions.StreamMessageIdPolicy;
import com.redis.spring.batch.reader.KeyComparison.Status;

import io.lettuce.core.StreamMessage;

public class KeyComparator {

	private KeyComparatorOptions options = new KeyComparatorOptions();

	public KeyComparison compare(KeyValue<String, Object> source, KeyValue<String, Object> target) {
		KeyComparison comparison = new KeyComparison();
		comparison.setSource(source);
		comparison.setTarget(target);
		comparison.setStatus(status(source, target));
		return comparison;
	}

	private Status status(KeyValue<String, Object> source, KeyValue<String, Object> target) {
		if (target == null || !target.exists()) {
			if (source == null || !source.exists()) {
				return Status.OK;
			}
			return Status.MISSING;
		}
		if (!target.exists() && source.exists()) {
			return Status.MISSING;
		}
		if (target.getType() != source.getType()) {
			return Status.TYPE;
		}
		if (!valueEquals(source, target)) {
			return Status.VALUE;
		}
		if (source.getTtl() != target.getTtl()) {
			long delta = Math.abs(source.getTtl() - target.getTtl());
			if (delta > options.getTtlTolerance().toMillis()) {
				return Status.TTL;
			}
		}
		return Status.OK;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private boolean valueEquals(KeyValue<String, Object> source, KeyValue<String, Object> target) {
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
	private boolean streamMessageEquals(StreamMessage source, StreamMessage target) {
		if (!Objects.equals(source.getStream(), target.getStream())) {
			return false;
		}
		if (options.getStreamMessageIdPolicy() == StreamMessageIdPolicy.COMPARE
				&& !Objects.equals(source.getId(), target.getId())) {
			return false;
		}
		Map sourceBody = source.getBody();
		Map targetBody = target.getBody();
		if (CollectionUtils.isEmpty(sourceBody)) {
			return CollectionUtils.isEmpty(targetBody);
		}
		return sourceBody.equals(targetBody);
	}

	public KeyComparatorOptions getOptions() {
		return options;
	}

	public void setOptions(KeyComparatorOptions options) {
		this.options = options;
	}

}
