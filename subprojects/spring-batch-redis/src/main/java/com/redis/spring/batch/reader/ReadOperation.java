package com.redis.spring.batch.reader;

import java.util.List;

public interface ReadOperation<K, T> {

	List<T> read(List<? extends K> keys) throws Exception;

}
