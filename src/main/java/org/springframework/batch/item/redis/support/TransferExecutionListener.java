package org.springframework.batch.item.redis.support;

public interface TransferExecutionListener {

	void onUpdate(long count);

	void onComplete();

	void onError(Throwable throwable);

}