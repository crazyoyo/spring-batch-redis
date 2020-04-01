package org.springframework.batch.item.redis.support;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.connection.stream.MapRecord;

public class IntrospectedTypeMapper<T> implements Converter<T, DataType> {

	@Override
	public DataType convert(T value) {
		if (value instanceof Map) {
			return DataType.HASH;
		}
		if (value instanceof List) {
			List<?> list = (List<?>) value;
			if (list.isEmpty()) {
				return DataType.NONE;
			}
			if (list.get(0) instanceof MapRecord) {
				return DataType.STREAM;
			}
			return DataType.LIST;
		}
		if (value instanceof Set) {
			Set<?> set = (Set<?>) value;
			if (set.isEmpty()) {
				return DataType.NONE;
			}
			if (set.iterator().next() instanceof Tuple) {
				return DataType.ZSET;
			}
			return DataType.SET;
		}
		return DataType.STRING;
	}

}
