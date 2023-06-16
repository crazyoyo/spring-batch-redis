package com.redis.spring.batch.reader;

import java.util.List;

import com.redis.spring.batch.common.KeyDump;

import io.lettuce.core.AbstractRedisClient;

public class KeyDumpReadOperation<K, V> extends AbstractLuaReadOperation<K, V, KeyDump<K>> {

	public KeyDumpReadOperation(AbstractRedisClient client) {
		super(client, "keydump.lua");
	}

	@Override
	@SuppressWarnings("unchecked")
	protected KeyDump<K> convert(List<Object> list) {
		KeyDump<K> dump = new KeyDump<>();
		K key = (K) list.get(0);
		dump.setKey(key);
		Long ttl = (Long) list.get(1);
		dump.setTtl(ttl);
		dump.setDump((byte[]) list.get(2));
		return dump;
	}

}
