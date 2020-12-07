package org.springframework.batch.item.redis.support;

import io.lettuce.core.AbstractRedisClient;

public class RedisClientBuilder {

	protected AbstractRedisClient client;

	public RedisClientBuilder(AbstractRedisClient client) {
		this.client = client;
	}

}