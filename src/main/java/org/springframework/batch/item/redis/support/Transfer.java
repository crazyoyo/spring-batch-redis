package org.springframework.batch.item.redis.support;

import java.time.Duration;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;

@Builder
public class Transfer<I, O> {

	public static final int DEFAULT_THREAD_COUNT = 1;

	private static final int DEFAULT_BATCH_SIZE = 50;

	@Getter
	private final String name;
	@NonNull
	@Getter
	private final ItemReader<I> reader;
	@Getter
	private final ItemProcessor<I, O> processor;
	@NonNull
	@Getter
	private final ItemWriter<O> writer;
	@Getter
	@Default
	private final int threads = DEFAULT_THREAD_COUNT;
	@Getter
	@Default
	private final int batch = DEFAULT_BATCH_SIZE;
	@Getter
	private final Duration flushInterval;

	public TransferExecution<I, O> execute() {
		return new TransferExecution<>(this);
	}

}
