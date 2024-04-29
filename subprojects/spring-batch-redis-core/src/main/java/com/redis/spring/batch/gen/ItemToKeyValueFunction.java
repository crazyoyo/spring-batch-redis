package com.redis.spring.batch.gen;

import java.util.function.Function;
import java.util.function.Supplier;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.KeyValue.DataType;
import com.redis.spring.batch.gen.Item.Type;

public class ItemToKeyValueFunction<T extends KeyValue<String, Object>> implements Function<Item, T> {

	private final Supplier<T> factory;

	public ItemToKeyValueFunction(Supplier<T> factory) {
		this.factory = factory;
	}

	@Override
	public T apply(Item item) {
		T kv = factory.get();
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
