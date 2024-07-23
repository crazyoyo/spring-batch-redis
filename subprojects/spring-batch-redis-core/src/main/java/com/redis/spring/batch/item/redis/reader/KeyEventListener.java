package com.redis.spring.batch.item.redis.reader;

import com.redis.spring.batch.item.redis.common.KeyEvent;

public interface KeyEventListener<K> {

	void event(KeyEvent<K> event, KeyEventStatus status);

}
