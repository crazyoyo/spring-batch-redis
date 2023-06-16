package com.redis.spring.batch.reader;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class DataStructureReadOperation<K, V> extends AbstractDataStructureReadOperation<K, V> {

	private final RedisCodec<K, V> codec;

	public DataStructureReadOperation(AbstractRedisClient client, RedisCodec<K, V> codec) {
		super(client);
		this.codec = codec;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected String string(Object object) {
		return StringCodec.UTF8.decodeValue(codec.encodeValue((V) object));
	}

}
