package org.springframework.batch.item.redis.support;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import lombok.Getter;

public class MultiTransferExecution {

	@Getter
	private final List<TransferExecution<?, ?>> executions;
	private final Collection<MultiTransferExecutionListener> listeners = new ArrayList<>();

	public MultiTransferExecution(List<? extends Transfer<?, ?>> transfers) {
		executions = new ArrayList<>(transfers.size());
		for (Transfer<?, ?> transfer : transfers) {
			executions.add(new TransferExecution<>(transfer));
		}
	}

	public void addListener(MultiTransferExecutionListener listener) {
		listeners.add(listener);
	}

	public CompletableFuture<Void> start() {
		listeners.forEach(MultiTransferExecutionListener::onStart);
		List<CompletableFuture<Void>> futures = new ArrayList<>(executions.size());
		for (TransferExecution<?, ?> execution : executions) {
			execution.addListener(new TransferExecutionListener() {

				@Override
				public void onUpdate(long count) {
					listeners.forEach(l -> l.onUpdate(execution, count));
				}
				
				@Override
				public void onMessage(String message) {
					listeners.forEach(l -> l.onMessage(message));
				}

				@Override
				public void onError(Throwable throwable) {
					listeners.forEach(l -> l.onError(execution, throwable));
				}

				@Override
				public void onComplete() {
					listeners.forEach(l -> l.onComplete(execution));
				}
			});
			listeners.forEach(l -> l.onStart(execution));
			futures.add(execution.start());
		}
		CompletableFuture<Void> future = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
		future.whenComplete((v, t) -> listeners.forEach(MultiTransferExecutionListener::onComplete));
		return future;
	}

}
