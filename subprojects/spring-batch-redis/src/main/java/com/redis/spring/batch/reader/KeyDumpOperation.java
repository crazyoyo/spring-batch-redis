package com.redis.spring.batch.reader;

import java.util.List;

import com.redis.spring.batch.common.KeyDump;

import io.lettuce.core.AbstractRedisClient;

public class KeyDumpOperation extends AbstractLuaOperation<byte[], byte[], KeyDump<byte[]>> {

	public KeyDumpOperation(AbstractRedisClient client) {
		super(client, "keydump.lua");
	}

	@Override
	protected KeyDump<byte[]> convert(List<Object> list) {
		KeyDump<byte[]> dump = new KeyDump<>();
		byte[] key = (byte[]) list.get(0);
		dump.setKey(key);
		Long ttl = (Long) list.get(1);
		dump.setTtl(ttl);
		dump.setDump((byte[]) list.get(2));
		return dump;
	}

}
