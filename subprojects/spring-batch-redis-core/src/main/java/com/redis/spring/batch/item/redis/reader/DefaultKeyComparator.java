package com.redis.spring.batch.item.redis.reader;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.DataType;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.reader.KeyComparison.Status;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.RedisCodec;

public class DefaultKeyComparator<K, V> implements KeyComparator<K> {

	public static final Duration DEFAULT_TTL_TOLERANCE = Duration.ofMillis(100);

	private final ToIntFunction<K> keyHashCode;
	private final ToIntFunction<V> valueHashCode;

	private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;
	private boolean ignoreStreamMessageId;

	public DefaultKeyComparator(RedisCodec<K, V> codec) {
		this.keyHashCode = BatchUtils.hashCodeFunction(codec);
		this.valueHashCode = BatchUtils.hashCodeFunction(codec);
	}

	private int keyHashCode(K key) {
		return keyHashCode.applyAsInt(key);
	}

	private int valueHashCode(V value) {
		return valueHashCode.applyAsInt(value);
	}

	@Override
	public KeyComparison<K> compare(KeyValue<K, Object> source, KeyValue<K, Object> target) {
		KeyComparison<K> comparison = new KeyComparison<>();
		comparison.setSource(source);
		comparison.setTarget(target);
		comparison.setStatus(status(source, target));
		return comparison;
	}

	private Status status(KeyValue<K, Object> source, KeyValue<K, Object> target) {
		if (!KeyValue.exists(target)) {
			if (!KeyValue.exists(source)) {
				return Status.OK;
			}
			return Status.MISSING;
		}
		if (KeyValue.hasType(source) && !source.getType().equalsIgnoreCase(target.getType())) {
			return Status.TYPE;
		}
		if (!ttlEquals(source, target)) {
			return Status.TTL;
		}
		if (!valueEquals(source, target)) {
			return Status.VALUE;
		}
		return Status.OK;
	}

	private boolean ttlEquals(KeyValue<K, Object> source, KeyValue<K, Object> target) {
		long diff = Math.abs(KeyValue.absoluteTTL(source) - KeyValue.absoluteTTL(target));
		return diff <= ttlTolerance.toMillis();
	}

	@SuppressWarnings("unchecked")
	private boolean valueEquals(KeyValue<K, Object> source, KeyValue<K, Object> target) {
		Object a = source.getValue();
		Object b = target.getValue();
		if (a == b) {
			return true;
		} else {
			if (a == null || b == null) {
				return false;
			}
		}
		if (!KeyValue.hasType(source)) {
			return !KeyValue.hasType(target);
		}
		DataType type = KeyValue.type(source);
		if (type == null) {
			return Objects.deepEquals(a, b);
		}
		switch (type) {
		case JSON:
		case STRING:
			return valueEquals((V) a, (V) b);
		case HASH:
			return mapEquals((Map<K, V>) a, (Map<K, V>) b);
		case LIST:
			return collectionEquals((Collection<V>) a, (Collection<V>) b);
		case SET:
			return setEquals((Collection<V>) a, (Collection<V>) b);
		case STREAM:
			return streamEquals((Collection<StreamMessage<K, V>>) a, (Collection<StreamMessage<K, V>>) b);
		case ZSET:
			return zsetEquals((Collection<ScoredValue<V>>) a, (Collection<ScoredValue<V>>) b);
		default:
			return Objects.deepEquals(a, b);
		}
	}

	private boolean zsetEquals(Collection<ScoredValue<V>> source, Collection<ScoredValue<V>> target) {
		return Objects.deepEquals(hashCodeZset(source), hashCodeZset(target));
	}

	private boolean setEquals(Collection<V> source, Collection<V> target) {
		return Objects.deepEquals(hashCodeSet(source), hashCodeSet(target));
	}

	private boolean collectionEquals(Collection<V> source, Collection<V> target) {
		return Objects.deepEquals(hashCodeList(source), hashCodeList(target));
	}

	private boolean mapEquals(Map<K, V> source, Map<K, V> target) {
		return Objects.deepEquals(hashCodeMap(source), hashCodeMap(target));
	}

	private Set<ScoredValue<Integer>> hashCodeZset(Collection<ScoredValue<V>> zset) {
		return zset.stream().map(v -> ScoredValue.just(v.getScore(), valueHashCode(v.getValue())))
				.collect(Collectors.toSet());
	}

	private Set<Integer> hashCodeSet(Collection<V> set) {
		return set.stream().map(this::valueHashCode).collect(Collectors.toSet());
	}

	private List<Integer> hashCodeList(Collection<V> collection) {
		return collection.stream().map(this::valueHashCode).collect(Collectors.toList());
	}

	private Map<Integer, Integer> hashCodeMap(Map<K, V> map) {
		Map<Integer, Integer> intMap = new HashMap<>();
		map.forEach((k, v) -> intMap.put(keyHashCode(k), valueHashCode(v)));
		return intMap;
	}

	private boolean valueEquals(V source, V target) {
		return valueHashCode(source) == valueHashCode(target);
	}

	private boolean streamEquals(Collection<StreamMessage<K, V>> source, Collection<StreamMessage<K, V>> target) {
		if (CollectionUtils.isEmpty(source)) {
			return CollectionUtils.isEmpty(target);
		}
		if (source.size() != target.size()) {
			return false;
		}
		Iterator<StreamMessage<K, V>> sourceIterator = source.iterator();
		Iterator<StreamMessage<K, V>> targetIterator = target.iterator();
		while (sourceIterator.hasNext()) {
			if (!targetIterator.hasNext()) {
				return false;
			}
			StreamMessage<K, V> sourceMessage = sourceIterator.next();
			StreamMessage<K, V> targetMessage = targetIterator.next();
			if (!ignoreStreamMessageId && !Objects.equals(sourceMessage.getId(), targetMessage.getId())) {
				return false;
			}
			if (!mapEquals(sourceMessage.getBody(), targetMessage.getBody())) {
				return false;
			}
		}
		return true;
	}

	public Duration getTtlTolerance() {
		return ttlTolerance;
	}

	public void setTtlTolerance(Duration ttlTolerance) {
		this.ttlTolerance = ttlTolerance;
	}

	public boolean isIgnoreStreamMessageId() {
		return ignoreStreamMessageId;
	}

	public void setIgnoreStreamMessageId(boolean ignore) {
		this.ignoreStreamMessageId = ignore;
	}

}
