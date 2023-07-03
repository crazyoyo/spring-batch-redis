package com.redis.spring.batch.reader;

import io.lettuce.core.AbstractRedisClient;

public class StringDataStructureReadOperation extends AbstractDataStructureReadOperation<String, String> {

	public StringDataStructureReadOperation(AbstractRedisClient client) {
		super(client);
	}

	@Override
	protected String string(Object object) {
		return (String) object;
	}

	@Override
	protected String encodeValue(String value) {
		return value;
	}

}
