package com.redis.spring.batch.support;

import java.util.List;

public interface ValueReader<K, T> {

	List<T> read(List<? extends K> keys) throws Exception;

}
