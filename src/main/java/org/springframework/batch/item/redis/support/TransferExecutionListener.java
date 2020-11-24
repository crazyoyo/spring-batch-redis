package org.springframework.batch.item.redis.support;

public interface TransferExecutionListener {

	void onProgress(long count);

	void onComplete();

	void onError(Throwable throwable);

}