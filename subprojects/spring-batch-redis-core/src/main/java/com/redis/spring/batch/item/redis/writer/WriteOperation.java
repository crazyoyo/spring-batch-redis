package com.redis.spring.batch.item.redis.writer;

import com.redis.spring.batch.item.redis.common.Operation;

public interface WriteOperation<K, V, T> extends Operation<K, V, T, Object> {

}
