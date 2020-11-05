package org.springframework.batch.item.redis.support;

public interface Filter<T> {

    boolean accept(T object);
}
