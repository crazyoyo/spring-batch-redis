package com.redis.spring.batch.item.redis.reader;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.item.redis.common.DataType;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.reader.KeyComparatorOptions.StreamMessageIdPolicy;
import com.redis.spring.batch.item.redis.reader.KeyComparison.Status;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

public class KeyComparator<K, V> {

	private KeyComparatorOptions options = new KeyComparatorOptions();

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
		long diff = Math.abs(source.getTtl() - target.getTtl());
		return diff <= options.getTtlTolerance().toMillis();
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
		case STREAM:
			return streamEquals((Collection<StreamMessage<K, V>>) a, (Collection<StreamMessage<K, V>>) b);
		case HASH:
			return mapEquals((Map<K, V>) a, (Map<K, V>) b);
		case JSON:
			return equals((V) a, (V) b);
		case LIST:
			return collectionEquals((Collection<V>) a, (Collection<V>) b);
		case SET:
			return setEquals((Set<V>) a, (Set<V>) b);
		case STRING:
			return equals((V) a, (V) b);
		case ZSET:
			return zsetEquals((Set<ScoredValue<V>>) a, (Set<ScoredValue<V>>) b);
		default:
			return Objects.deepEquals(a, b);
		}
	}

	private boolean zsetEquals(Set<ScoredValue<V>> a, Set<ScoredValue<V>> b) {
		return Objects.deepEquals(wrapZset(a), wrapZset(b));
	}

	private Object wrapZset(Set<ScoredValue<V>> collection) {
		return collection.stream().map(v -> ScoredValue.just(v.getScore(), new Wrapper<>(v.getValue())))
				.collect(Collectors.toSet());
	}

	private boolean setEquals(Set<V> a, Set<V> b) {
		return Objects.deepEquals(wrapSet(a), wrapSet(b));
	}

	private boolean collectionEquals(Collection<V> a, Collection<V> b) {
		return Objects.deepEquals(wrapCollection(a), wrapCollection(b));
	}

	private Collection<Wrapper<V>> wrapCollection(Collection<V> collection) {
		return collection.stream().map(Wrapper::new).collect(Collectors.toList());
	}

	private Set<Wrapper<V>> wrapSet(Set<V> set) {
		return set.stream().map(Wrapper::new).collect(Collectors.toSet());
	}

	private <T> boolean equals(T a, T b) {
		return new Wrapper<>(a).equals(new Wrapper<>(b));
	}

	private boolean mapEquals(Map<K, V> source, Map<K, V> target) {
		return wrap(source).equals(wrap(target));
	}

	private Map<Wrapper<K>, Wrapper<V>> wrap(Map<K, V> source) {
		Map<Wrapper<K>, Wrapper<V>> wrapper = new HashMap<>();
		source.forEach((k, v) -> wrapper.put(new Wrapper<>(k), new Wrapper<>(v)));
		return wrapper;
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
			if (!equals(sourceMessage.getStream(), targetMessage.getStream())) {
				return false;
			}
			if (options.getStreamMessageIdPolicy() == StreamMessageIdPolicy.COMPARE
					&& !Objects.equals(sourceMessage.getId(), targetMessage.getId())) {
				return false;
			}
			if (!mapEquals(sourceMessage.getBody(), targetMessage.getBody())) {
				return false;
			}
		}
		return true;
	}

	public KeyComparatorOptions getOptions() {
		return options;
	}

	public void setOptions(KeyComparatorOptions options) {
		this.options = options;
	}

}
