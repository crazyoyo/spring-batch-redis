package com.redis.spring.batch.support.compare;

public interface KeyComparisonListener<K> {

	void keyComparison(KeyComparison<K> comparison);

}
