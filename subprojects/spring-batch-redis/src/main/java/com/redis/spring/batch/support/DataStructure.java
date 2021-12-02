package com.redis.spring.batch.support;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

public class DataStructure<K> extends KeyValue<K, Object> {

	public static final String STRING = "string";
	public static final String LIST = "list";
	public static final String SET = "set";
	public static final String ZSET = "zset";
	public static final String HASH = "hash";
	public static final String STREAM = "stream";
	public static final String NONE = "none";

	private String type;

	public static Set<String> types() {
		return new LinkedHashSet<>(Arrays.asList(STRING, HASH, SET, ZSET, LIST, STREAM));
	}

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

	public static <K, V> DataStructure<K> createString(K key, V value) {
		return new DataStructure<>(STRING, key, value);
	}

	public static <K, V> DataStructure<K> createHash(K key, Map<K, V> value) {
		return new DataStructure<>(HASH, key, value);
	}

	public static <K, V> DataStructure<K> createSet(K key, Set<V> value) {
		return new DataStructure<>(SET, key, value);
	}

	public static <K, V> DataStructure<K> createZset(K key, Collection<ScoredValue<V>> value) {
		return new DataStructure<>(ZSET, key, value);
	}

	public static <K, V> DataStructure<K> createList(K key, List<V> value) {
		return new DataStructure<>(LIST, key, value);
	}

	public static <K, V> DataStructure<K> createStream(K key, Collection<StreamMessage<K, V>> value) {
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
