package com.redis.spring.batch.reader;

import io.lettuce.core.AbstractRedisClient;

public class DataStructureStringOperation extends AbstractDataStructureOperation<String, String> {

	public DataStructureStringOperation(AbstractRedisClient client) {
		super(client);
	}

	@Override
	protected String string(Object object) {
		return (String) object;
	}

}
