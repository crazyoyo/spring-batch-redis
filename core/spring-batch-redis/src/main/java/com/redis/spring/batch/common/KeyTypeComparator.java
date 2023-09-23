package com.redis.spring.batch.common;

import java.util.Objects;

import com.redis.spring.batch.common.KeyComparison.Status;

public class KeyTypeComparator implements KeyComparator<KeyType<String>> {

    @Override
    public Status compare(KeyType<String> source, KeyType<String> target) {
        if (target == null) {
            return Status.MISSING;
        }
        if (!Objects.equals(source.getType(), target.getType())) {
            if (target.getType() == DataStructureType.NONE) {
                return Status.MISSING;
            }
            return Status.TYPE;
        }
        return Status.OK;
    }

}
