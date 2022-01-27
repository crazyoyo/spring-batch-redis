package com.redis.spring.batch.compare;

public interface KeyComparisonListener<K> {

	void keyComparison(KeyComparison<K> comparison);

}
