package com.redis.spring.batch.writer;

import com.redis.spring.batch.common.BatchOperation;

public interface BatchWriteOperation<K, V, T> extends BatchOperation<K, V, T, Object> {

}
