package com.redis.spring.batch.common;

import com.redis.spring.batch.writer.BatchWriteOperation;
import com.redis.spring.batch.writer.WriteOperation;

public class SimpleBatchWriteOperation<K, V, I> extends SimpleBatchOperation<K, V, I, Object>
        implements BatchWriteOperation<K, V, I> {

    public SimpleBatchWriteOperation(WriteOperation<K, V, I> operation) {
        super(operation);
    }

}
