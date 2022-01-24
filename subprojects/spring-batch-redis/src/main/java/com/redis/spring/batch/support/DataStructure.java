package com.redis.spring.batch.support;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

public class DataStructure<K> extends KeyValue<K, Object> {

	public enum Type {
		STRING, LIST, SET, ZSET, HASH, STREAM, NONE
	}

	public static final Set<Type> TYPES = Collections.unmodifiableSet(EnumSet.complementOf(EnumSet.of(Type.NONE)));

	private Type type;

	public DataStructure() {
	}

	public DataStructure(K key, String type) {
		super(key);
		this.type = type(type);
	}

	public static Type type(String type) {
		if (type == null) {
			return Type.NONE;
		}
		return Type.valueOf(type.toUpperCase());
	}

	public DataStructure(K key, Type type) {
		super(key);
		this.type = type;
	}

	public DataStructure(K key, Object value, Type type) {
		super(key, value);
		this.type = type;
	}

	public DataStructure(K key, Object value, Long absoluteTTL, Type type) {
		super(key, value, absoluteTTL);
		this.type = type;
	}

	public static <K, V> DataStructure<K> createString(K key, V value) {
		return new DataStructure<>(key, value, Type.STRING);
	}

	public static <K, V> DataStructure<K> createHash(K key, Map<K, V> value) {
		return new DataStructure<>(key, value, Type.HASH);
	}

	public static <K, V> DataStructure<K> createSet(K key, Set<V> value) {
		return new DataStructure<>(key, value, Type.SET);
	}

	public static <K, V> DataStructure<K> createZset(K key, Collection<ScoredValue<V>> value) {
		return new DataStructure<>(key, value, Type.ZSET);
	}

	public static <K, V> DataStructure<K> createList(K key, List<V> value) {
		return new DataStructure<>(key, value, Type.LIST);
	}

	public static <K, V> DataStructure<K> createStream(K key, Collection<StreamMessage<K, V>> value) {
		return new DataStructure<>(key, value, Type.STREAM);
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return "DataStructure [type=" + type + ", key=" + getKey() + ", value=" + getValue() + ", absoluteTTL="
				+ getAbsoluteTTL() + "]";
	}

}
