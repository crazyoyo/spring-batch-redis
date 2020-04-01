package org.springframework.batch.item.redis;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.item.redis.support.AbstractRedisItemReader;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;

import lombok.Builder;

public class KeyDumpRedisItemReader<K, V> extends AbstractRedisItemReader<K, V, KeyDump> {

	@Builder
	private KeyDumpRedisItemReader(int currentItemCount, Integer maxItemCount, Boolean saveState,
			RedisTemplate<K, V> redisTemplate, ScanOptions scanOptions, int batchSize) {
		super(currentItemCount, maxItemCount, saveState, redisTemplate, scanOptions, batchSize);
	}

	@Override
	protected List<KeyDump> getValues(List<byte[]> keys) {
		List<Object> results = redisTemplate.executePipelined(new RedisCallback<Object>() {
			@Override
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				for (byte[] key : keys) {
					connection.pTtl(key);
					connection.dump(key);
				}
				return null;
			}
		});
		List<KeyDump> values = new ArrayList<>(keys.size());
		for (int index = 0; index < keys.size(); index++) {
			byte[] key = keys.get(index);
			Long ttl = (Long) results.get(index * 2);
			byte[] bytes = (byte[]) results.get(index * 2 + 1);
			values.add(new KeyDump(key, ttl, bytes));
		}
		return values;
	}
}