package org.springframework.batch.item.redis.support;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;

@SuppressWarnings("unchecked")
public class RedisClientBuilder<B extends RedisClientBuilder<B>> {

	protected AbstractRedisClient client = RedisClient.create();

	public B client(AbstractRedisClient client) {
		this.client = client;
		return (B) this;
	}

}