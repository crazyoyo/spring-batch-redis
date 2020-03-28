package org.springframework.batch.item.redis;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.batch.item.redis.support.AbstractRedisItemWriter;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;

import lombok.Builder;
import lombok.Setter;

public class KeyDumpRedisItemWriter extends AbstractRedisItemWriter<byte[], byte[], KeyDump> {

	private @Setter boolean replace;

	@Builder
	private KeyDumpRedisItemWriter(RedisTemplate<byte[], byte[]> template, boolean delete, boolean replace) {
		super(template, delete);
		this.replace = replace;
	}

	@Override
	protected void doWrite(List<? extends KeyDump> items) {
		redisTemplate.executePipelined(new RedisCallback<Object>() {
			@Override
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				for (KeyDump item : items) {
					connection.restore(item.getKey(), item.getTtl(), item.getValue(), replace);
				}
				return null;
			}
		});
	}

	@Override
	protected Collection<byte[]> getKeys(List<? extends KeyDump> items) {
		return items.stream().map(KeyDump::getKey).collect(Collectors.toList());
	}

}
