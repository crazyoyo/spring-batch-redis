package com.redis.spring.batch.reader;

import java.util.List;

public interface ValueReader<K, T> {

	List<T> read(List<? extends K> items) throws Exception;

}
