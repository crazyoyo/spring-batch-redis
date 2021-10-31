package com.redis.spring.batch.support;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
public class DataStructure<K> extends KeyValue<K, Object> {

	public enum Type {

		HASH, LIST, SET, STREAM, STRING, ZSET

	}

	public final static String STRING = "string";
	public final static String LIST = "list";
	public final static String SET = "set";
	public final static String ZSET = "zset";
	public final static String HASH = "hash";
	public final static String STREAM = "stream";
	public static final String NONE = "none";

	private String type;

	public DataStructure() {
	}

	public DataStructure(String type, K key) {
		super(key);
		this.type = type;
	}

	public DataStructure(String type, K key, Object value) {
		super(key, value);
		this.type = type;
	}

	public DataStructure(String type, K key, Object value, Long absoluteTTL) {
		super(key, value, absoluteTTL);
		this.type = type;
	}

	public static <K, V> DataStructure<K> string(K key, V value) {
		return new DataStructure<>(STRING, key, value);
	}

	public static <K, V> DataStructure<K> hash(K key, Map<K, V> value) {
		return new DataStructure<>(HASH, key, value);
	}

	public static <K, V> DataStructure<K> set(K key, Set<V> value) {
		return new DataStructure<>(SET, key, value);
	}

	public static <K, V> DataStructure<K> zset(K key, Collection<ScoredValue<V>> value) {
		return new DataStructure<>(ZSET, key, value);
	}

	public static <K, V> DataStructure<K> list(K key, List<V> value) {
		return new DataStructure<>(LIST, key, value);
	}

	public static <K, V> DataStructure<K> stream(K key, Collection<StreamMessage<K, V>> value) {
		return new DataStructure<>(STREAM, key, value);
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return "DataStructure [type=" + type + ", key=" + getKey() + ", value=" + getValue() + ", absoluteTTL="
				+ getAbsoluteTTL() + "]";
	}

}
