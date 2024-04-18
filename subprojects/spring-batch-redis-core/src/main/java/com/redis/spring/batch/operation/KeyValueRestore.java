package com.redis.spring.batch.operation;

import com.redis.spring.batch.KeyValue;

public class KeyValueRestore<K, V> extends Restore<K, V, KeyValue<K, byte[]>> {

	public KeyValueRestore() {
		super(KeyValue::getKey, KeyValue::getValue);
		setTtlFunction(KeyValue::getTtl);
	}

}
