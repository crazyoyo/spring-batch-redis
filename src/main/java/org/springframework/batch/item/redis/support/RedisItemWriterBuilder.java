package org.springframework.batch.item.redis.support;

import io.lettuce.core.codec.RedisCodec;

public class RedisItemWriterBuilder<K, V, B extends RedisItemWriterBuilder<K, V, B>>
		extends RedisConnectionBuilder<K, V, B> {

	public RedisItemWriterBuilder(RedisCodec<K, V> codec) {
		super(codec);
	}

	protected <T extends AbstractRedisItemWriter<K, V, ?>> T configure(T writer) {
		writer.setPool(pool());
		writer.setCommands(async());
		writer.setCommandTimeout(uri().getTimeout());
		return writer;
	}

}
