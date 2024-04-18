package com.redis.spring.batch.gen;

import java.util.function.Function;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.KeyValue.Type;

public class ItemToKeyValueFunction implements Function<Item, KeyValue<String, Object>> {

	@Override
	public KeyValue<String, Object> apply(Item item) {
		KeyValue<String, Object> kv = new KeyValue<>();
		kv.setKey(item.getKey());
		kv.setTtl(item.getTtl());
		kv.setType(dataType(item));
		kv.setValue(item.getValue());
		return kv;
	}

	private Type dataType(Item item) {
		return Type.valueOf(item.getType().name());
	}

}
