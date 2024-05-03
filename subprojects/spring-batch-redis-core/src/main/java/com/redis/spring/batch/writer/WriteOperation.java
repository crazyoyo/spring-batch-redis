package com.redis.spring.batch.writer;

import com.redis.spring.batch.common.Operation;

public interface WriteOperation<K, V, T> extends Operation<K, V, T, Object> {

}
