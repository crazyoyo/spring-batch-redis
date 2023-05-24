package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import com.redis.spring.batch.common.KeyDump;

import io.lettuce.core.RestoreArgs;

public class RestoreReplace<K, V, T> extends Restore<K, V, T> {

	public RestoreReplace(Function<T, K> key, Function<T, byte[]> value, Function<T, Long> ttl) {
		super(key, value, ttl);
	}

	@Override
	protected RestoreArgs args(T item) {
		return super.args(item).replace();
	}

	public static <K, V> RestoreReplace<K, V, KeyDump<K>> keyDump() {
		return new RestoreReplace<>(KeyDump::getKey, KeyDump::getDump, KeyDump::getTtl);
	}
}
