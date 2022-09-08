package com.redis.spring.batch.writer.operation;

import org.springframework.core.convert.converter.Converter;

import com.redis.spring.batch.common.KeyDump;

import io.lettuce.core.RestoreArgs;

public class RestoreReplace<K, V, T> extends Restore<K, V, T> {

	public RestoreReplace(Converter<T, K> key, Converter<T, byte[]> value, Converter<T, Long> absoluteTTL) {
		super(key, value, absoluteTTL);
	}

	@Override
	protected RestoreArgs args(T item) {
		return super.args(item).replace();
	}

	public static <K, V, T> RestoreReplace<K, V, T> of(Converter<T, K> key, Converter<T, byte[]> value,
			Converter<T, Long> absoluteTTL) {
		return new RestoreReplace<>(key, value, absoluteTTL);
	}

	public static <K, V> RestoreReplace<K, V, KeyDump<K>> keyDump() {
		return new RestoreReplace<>(KeyDump::getKey, KeyDump::getDump, KeyDump::getTtl);
	}
}
