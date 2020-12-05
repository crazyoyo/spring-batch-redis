package org.springframework.batch.item.redis.support;

import java.time.Duration;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;

@Data
@Builder
public class TransferOptions {

	public static final int DEFAULT_THREAD_COUNT = 1;
	public static final int DEFAULT_BATCH_SIZE = 50;

	@Default
	private int threads = DEFAULT_THREAD_COUNT;
	@Default
	private int batch = DEFAULT_BATCH_SIZE;
	private Duration flushInterval;

}
