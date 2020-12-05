package org.springframework.batch.item.redis.support;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;

public class ClientBuilder<B extends ClientBuilder<B>> {

	protected AbstractRedisClient client;
	protected GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig = new GenericObjectPoolConfig<>();

	@SuppressWarnings("unchecked")
	public B client(AbstractRedisClient client) {
		this.client = client;
		return (B) this;
	}

	@SuppressWarnings("unchecked")
	public B poolConfig(GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig) {
		this.poolConfig = poolConfig;
		return (B) this;
	}

}
