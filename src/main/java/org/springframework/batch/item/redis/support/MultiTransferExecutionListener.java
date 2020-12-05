package org.springframework.batch.item.redis.support;

public interface MultiTransferExecutionListener {

	void onStart();

	void onStart(TransferExecution<?, ?> execution);

	void onUpdate(TransferExecution<?, ?> execution, long count);

	void onComplete(TransferExecution<?, ?> execution);

	void onError(TransferExecution<?, ?> execution, Throwable throwable);

	void onComplete();

}