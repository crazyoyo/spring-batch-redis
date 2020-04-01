package org.springframework.batch.item.redis.support;

import java.util.Collection;
import java.util.List;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.Assert;

import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
public abstract class AbstractRedisItemWriter<K, V, T> extends AbstractItemStreamItemWriter<T>
		implements InitializingBean {

	protected @Setter RedisTemplate<K, V> redisTemplate;
	protected @Setter boolean delete;

	protected AbstractRedisItemWriter(RedisTemplate<K, V> redisTemplate, boolean delete) {
		Assert.notNull(redisTemplate, "redisTemplate is required.");
		this.redisTemplate = redisTemplate;
		this.delete = delete;
	}

	@Override
	public void write(List<? extends T> items) throws Exception {
		if (delete) {
			redisTemplate.delete(getKeys(items));
		} else {
			doWrite(items);
		}
	}

	protected abstract Collection<K> getKeys(List<? extends T> items);

	protected abstract void doWrite(List<? extends T> items);

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(redisTemplate, "A RedisTemplate is required.");
	}

}
