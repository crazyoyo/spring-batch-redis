package com.redis.spring.batch.gen;

import java.util.function.Function;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.KeyValue.DataType;
import com.redis.spring.batch.gen.Item.Type;

public class ItemToKeyValueFunction implements Function<Item, KeyValue<String, Object>> {

	@Override
	public KeyValue<String, Object> apply(Item item) {
		KeyValue<String, Object> kv = new KeyValue<>();
		kv.setKey(item.getKey());
		kv.setTtl(item.getTtl());
		kv.setType(toRedisTypeString(item.getType()));
		kv.setValue(item.getValue());
		return kv;
	}

	private String toRedisTypeString(Type type) {
		switch (type) {
		case HASH:
			return DataType.HASH.getString();
		case JSON:
			return DataType.JSON.getString();
		case LIST:
			return DataType.LIST.getString();
		case SET:
			return DataType.SET.getString();
		case STREAM:
			return DataType.STREAM.getString();
		case STRING:
			return DataType.STRING.getString();
		case TIMESERIES:
			return DataType.TIMESERIES.getString();
		case ZSET:
			return DataType.ZSET.getString();
		default:
			return type.name().toLowerCase();
		}
	}

}
