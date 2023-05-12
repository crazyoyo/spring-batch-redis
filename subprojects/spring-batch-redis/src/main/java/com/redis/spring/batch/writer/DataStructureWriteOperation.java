package com.redis.spring.batch.writer;

import java.util.List;
import java.util.Map;

import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.writer.operation.Noop;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class DataStructureWriteOperation<K, V> implements WriteOperation<K, V, DataStructure<K>> {

	private WriteOperation<K, V, DataStructure<K>> noop = new Noop<>();
	private Map<String, WriteOperation<K, V, DataStructure<K>>> operations;

	public DataStructureWriteOperation(Map<String, WriteOperation<K, V, DataStructure<K>>> operations) {
		this.operations = operations;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, DataStructure<K> item) {
		if (item == null || item.getKey() == null) {
			return;
		}
		if (item.getValue() == null || DataStructure.isNone(item)) {
			futures.add(((RedisKeyAsyncCommands<K, K>) commands).del(item.getKey()));
		} else {
			operation(item).execute(commands, futures, item);
			if (item.hasTtl()) {
				futures.add(((RedisKeyAsyncCommands<K, V>) commands).pexpireat(item.getKey(), item.getTtl()));
			}
		}
	}

	private WriteOperation<K, V, DataStructure<K>> operation(DataStructure<K> item) {
		return operations.getOrDefault(item.getType(), noop);
	}

}
