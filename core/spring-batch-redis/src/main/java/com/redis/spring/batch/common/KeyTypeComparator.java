package com.redis.spring.batch.common;

import com.redis.spring.batch.common.KeyComparison.Status;

public class KeyTypeComparator implements KeyComparator<KeyValue<String>> {

    @Override
    public Status compare(KeyValue<String> source, KeyValue<String> target) {
        if (target.getType() != source.getType()) {
            if (target.getType() == DataType.NONE) {
                return Status.MISSING;
            }
            return Status.TYPE;
        }
        return Status.OK;
    }

}
