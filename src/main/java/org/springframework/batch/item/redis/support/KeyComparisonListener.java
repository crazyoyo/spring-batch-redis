package org.springframework.batch.item.redis.support;

public interface KeyComparisonListener<K> {

    void compare(DataStructure<K> source, DataStructure<K> target);

}
