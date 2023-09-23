package com.redis.spring.batch.common;

import com.redis.spring.batch.common.KeyComparison.Status;

public interface KeyComparator<T> {

    Status compare(T source, T target);

}
