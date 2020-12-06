package org.springframework.batch.item.redis.support;

public class MultiTransferExecutionListenerAdapter implements MultiTransferExecutionListener {

	@Override
	public void onStart() {
	}

	@Override
	public void onComplete() {
	}

	@Override
	public void onStart(TransferExecution<?, ?> execution) {
	}

	@Override
	public void onUpdate(TransferExecution<?, ?> execution, long count) {
	}

	@Override
	public void onMessage(String name) {
	}

	@Override
	public void onComplete(TransferExecution<?, ?> execution) {
	}

	@Override
	public void onError(TransferExecution<?, ?> execution, Throwable throwable) {
	}

}
