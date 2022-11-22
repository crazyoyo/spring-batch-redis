package com.redis.spring.batch.common.queue;

public interface ConcurrentSetListener<E> {

	void onDuplicate(E e);

}