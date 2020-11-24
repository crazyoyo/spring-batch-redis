package org.springframework.batch.item.redis.support;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import lombok.Getter;

public class MultiTransferExecution {

	@Getter
	private final CompletableFuture<Void> future;

	public MultiTransferExecution(List<? extends TransferExecution<?, ?>> executions) {
		this.future = CompletableFuture.allOf(executions.stream().map(TransferExecution::getFuture)
				.collect(Collectors.toList()).toArray(new CompletableFuture[0]));
	}

}
