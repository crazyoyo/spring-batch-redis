package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.KeyValue.Type;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.operation.OperationExecutor;
import com.redis.spring.batch.reader.KeyComparison.Status;

import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.StringCodec;

public class KeyComparisonItemReader extends AbstractRedisItemReader<String, String, KeyComparison> {

	public static final Duration DEFAULT_TTL_TOLERANCE = Duration.ofMillis(100);

	private final RedisItemReader<String, String> source;
	private final RedisItemReader<String, String> target;

	private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;
	private boolean compareStreamMessageIds;

	private OperationExecutor<String, String, String, KeyValue<String>> sourceOperationExecutor;
	private OperationExecutor<String, String, String, KeyValue<String>> targetOperationExecutor;

	public KeyComparisonItemReader(RedisItemReader<String, String> source, RedisItemReader<String, String> target) {
		super(StringCodec.UTF8);
		this.source = source;
		this.target = target;
	}

	@Override
	protected synchronized void doOpen() {
		sourceOperationExecutor = source.operationExecutor();
		targetOperationExecutor = target.operationExecutor();
		super.doOpen();
	}

	@Override
	protected synchronized void doClose() throws InterruptedException {
		super.doClose();
		targetOperationExecutor.close();
		sourceOperationExecutor.close();
	}

	@Override
	protected List<KeyComparison> execute(Iterable<? extends String> keys) {
		List<KeyValue<String>> sources = sourceOperationExecutor.apply(keys);
		Map<String, KeyValue<String>> targets = targetOperationExecutor.apply(keys).stream()
				.collect(Collectors.toMap(KeyValue::getKey, Function.identity()));
		return sources.stream().map(s -> compare(s, targets.get(s.getKey()))).collect(Collectors.toList());
	}

	private KeyComparison compare(KeyValue<String> source, KeyValue<String> target) {
		KeyComparison comparison = new KeyComparison();
		comparison.setSource(source);
		comparison.setTarget(target);
		comparison.setStatus(status(source, target));
		return comparison;
	}

	private Status status(KeyValue<String> source, KeyValue<String> target) {
		if (target == null) {
			if (source == null) {
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

	public void setCompareStreamMessageIds(boolean enable) {
		this.compareStreamMessageIds = enable;
	}

	public void setTtlTolerance(Duration ttlTolerance) {
		this.ttlTolerance = ttlTolerance;
	}

}
