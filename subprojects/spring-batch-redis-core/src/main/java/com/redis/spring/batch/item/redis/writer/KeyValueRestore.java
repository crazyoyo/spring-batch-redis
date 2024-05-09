package com.redis.spring.batch.item.redis.writer;

import com.redis.spring.batch.item.redis.common.KeyValue;

public class KeyValueRestore<K, V> extends Restore<K, V, KeyValue<K, byte[]>> {

	public KeyValueRestore() {
		super(KeyValue::getKey, KeyValue::getValue);
		setTtlFunction(KeyValue::getTtl);
	}

}
