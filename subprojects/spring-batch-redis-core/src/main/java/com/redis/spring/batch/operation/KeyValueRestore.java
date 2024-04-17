package com.redis.spring.batch.operation;

import com.redis.spring.batch.KeyValue;

public class KeyValueRestore<K, V> extends Restore<K, V, KeyValue<K>> {

	public KeyValueRestore() {
		super(KeyValue::getKey, k -> (byte[]) k.getValue());
		setTtlFunction(KeyValue::getTtl);
	}

}
