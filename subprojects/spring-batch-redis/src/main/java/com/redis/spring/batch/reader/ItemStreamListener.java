package com.redis.spring.batch.reader;

public interface ItemStreamListener {

	void beforeOpen();

	void afterOpen();

	void beforeClose();

	void afterClose();

	void beforeUpdate();

	void afterUpdate();

}
