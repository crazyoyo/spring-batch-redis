package com.redis.spring.batch.reader;

import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.redis.spring.batch.DataStructure;
import com.redis.spring.batch.DataStructure.Type;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class DataStructureIntrospectingValueReader extends DataStructureValueReader<String, String> {

	public DataStructureIntrospectingValueReader(Supplier<StatefulConnection<String, String>> connectionSupplier,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig,
			Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> async) {
		super(connectionSupplier, poolConfig, async);
	}

	@Override
	protected void setValue(DataStructure<String> dataStructure, Object value) {
		if (dataStructure.getType() == Type.STRING && ((String) value).startsWith("HYLL")) {
			dataStructure.setType(Type.HYPERLOGLOG);
		}
		super.setValue(dataStructure, value);
	}

}
