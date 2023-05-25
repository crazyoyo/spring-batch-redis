package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import io.lettuce.core.RestoreArgs;

public class RestoreReplace<K, V, T> extends Restore<K, V, T> {

	public RestoreReplace(Function<T, K> key, Function<T, byte[]> value, Function<T, Long> ttl) {
		super(key, value, ttl);
	}

	@Override
	protected RestoreArgs args(Long ttl) {
		return super.args(ttl).replace();
	}

}
