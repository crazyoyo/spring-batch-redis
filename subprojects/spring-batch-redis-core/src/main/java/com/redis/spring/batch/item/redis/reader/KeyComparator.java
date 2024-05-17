package com.redis.spring.batch.item.redis.reader;

import com.redis.spring.batch.item.redis.common.KeyValue;

public interface KeyComparator<K, V> {

	KeyComparison<K> compare(KeyValue<K, Object> source, KeyValue<K, Object> target);

}