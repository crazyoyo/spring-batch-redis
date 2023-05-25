package com.redis.spring.batch.reader;

import io.lettuce.core.AbstractRedisClient;

public class DataStructureStringReadOperation extends AbstractDataStructureReadOperation<String, String> {

	public DataStructureStringReadOperation(AbstractRedisClient client) {
		super(client);
	}

	@Override
	protected String string(Object object) {
		return (String) object;
	}

}
